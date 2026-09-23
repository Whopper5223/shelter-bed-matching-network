use std::sync::Arc;

use common::model::BedStatus;
use common::proto;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use uuid::Uuid;

use crate::matcher::{self, ApplyOutcome};
use crate::notify;
use crate::state::AppState;

/// Consumes `bed-events`, keyed by shelter_id so a shelter's events stay
/// ordered within a partition. Offsets are committed manually, only after
/// the corresponding Postgres transaction has committed -- if the process
/// crashes between applying an event and committing its offset, the event
/// is redelivered and re-applied safely because apply is idempotent.
pub async fn run(
    state: Arc<AppState>,
    brokers: &str,
    topic: &str,
    group_id: &str,
) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[topic])?;
    tracing::info!(topic, group_id, "kafka consumer subscribed");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    match proto::BedStatusEvent::decode(payload) {
                        Ok(event) => {
                            if let Err(err) = handle_event(&state, &event).await {
                                tracing::error!(%err, event_id = %event.event_id, "failed to apply bed status event, will retry on redelivery");
                                continue; // skip commit -- this message will be redelivered
                            }
                        }
                        Err(err) => {
                            tracing::error!(%err, "failed to decode BedStatusEvent payload, skipping");
                        }
                    }
                }
                if let Err(err) = consumer.commit_message(&msg, CommitMode::Async) {
                    tracing::error!(%err, "failed to commit kafka offset");
                }
            }
            Err(err) => {
                tracing::error!(%err, "kafka receive error");
            }
        }
    }
}

async fn handle_event(state: &Arc<AppState>, event: &proto::BedStatusEvent) -> anyhow::Result<()> {
    let event_id = Uuid::parse_str(&event.event_id)?;
    let bed_id = Uuid::parse_str(&event.bed_id)?;
    let status: BedStatus = event.status().into();

    let outcome =
        matcher::apply_bed_status_event(&state.pool, event_id, bed_id, status, event.sequence)
            .await?;

    match outcome {
        ApplyOutcome::Applied {
            shelter_id,
            became_available,
        } => {
            let mut final_status = status;
            if became_available
                && matcher::allocate_for_bed(&state.pool, bed_id)
                    .await?
                    .is_some()
            {
                // Immediately claimed by a waiting referral -- publish the
                // final state rather than a misleading momentary "available".
                final_status = BedStatus::Reserved;
            }
            notify::publish_bed_update(
                state,
                shelter_id,
                bed_id,
                final_status,
                event.occurred_at_ms,
            )
            .await?;
        }
        ApplyOutcome::Duplicate => {
            tracing::debug!(event_id = %event.event_id, "duplicate event, skipping");
        }
        ApplyOutcome::Stale => {
            tracing::debug!(event_id = %event.event_id, sequence = event.sequence, "stale/out-of-order event, skipping");
        }
        ApplyOutcome::UnknownBed => {
            tracing::warn!(bed_id = %event.bed_id, "event for unknown bed, skipping");
        }
    }

    Ok(())
}
