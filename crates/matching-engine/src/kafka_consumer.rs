use std::sync::Arc;
use std::time::Duration;

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
/// the corresponding Postgres transaction has committed.
///
/// A failing event is retried in place with backoff, not skipped: Kafka's
/// committed offset is a single per-partition high-water mark, not a sparse
/// set of acked messages, so skipping ahead and later committing a *later*
/// message's offset would silently move the committed position past the one
/// that failed -- it would never be redelivered. Retrying in place does mean
/// a stuck event blocks this partition (and, since one consumer task here
/// serves every assigned partition, other shelters' events too); a
/// production version would likely give each partition its own consumer
/// task, or a dead-letter topic, to avoid that head-of-line blocking.
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
                            let mut attempt = 0u32;
                            while let Err(err) = handle_event(&state, &event).await {
                                attempt += 1;
                                tracing::error!(%err, event_id = %event.event_id, attempt, "failed to apply bed status event, retrying in place");
                                tokio::time::sleep(Duration::from_millis(
                                    200 * u64::from(attempt.min(25)),
                                ))
                                .await;
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

    let applied_shelter_id = match outcome {
        ApplyOutcome::UnknownBed => {
            tracing::warn!(bed_id = %event.bed_id, "event for unknown bed, skipping");
            return Ok(());
        }
        ApplyOutcome::Duplicate => {
            tracing::debug!(event_id = %event.event_id, "duplicate event, still checking for a strandable allocation");
            None
        }
        ApplyOutcome::Stale => {
            tracing::debug!(event_id = %event.event_id, sequence = event.sequence, "stale/out-of-order event, still checking for a strandable allocation");
            None
        }
        ApplyOutcome::Applied { shelter_id, .. } => Some(shelter_id),
    };

    // Always attempt an allocation pass on this bed, even for a
    // Duplicate/Stale outcome: it's a cheap no-op (an immediate rollback)
    // if the bed isn't currently `available`, and it closes a real gap
    // where a crash between apply_bed_status_event's commit and this call
    // -- or between this call and the offset commit below -- could
    // otherwise strand a bed as `available` forever, with no pending
    // referral ever considered for it again.
    if let Some(result) = matcher::allocate_for_bed(&state.pool, bed_id).await? {
        notify::publish_bed_update(
            state,
            result.shelter_id,
            result.bed_id,
            BedStatus::Reserved,
            event.occurred_at_ms,
        )
        .await?;
    } else if let Some(shelter_id) = applied_shelter_id {
        notify::publish_bed_update(state, shelter_id, bed_id, status, event.occurred_at_ms).await?;
    }

    Ok(())
}
