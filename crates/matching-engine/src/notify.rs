use common::model::BedStatus;
use common::proto;
use uuid::Uuid;

use crate::matcher;
use crate::state::AppState;

/// Publishes a live availability update to every subscribed
/// `StreamAvailability` caller. Called after any bed-state change, whether
/// it came from a Kafka intake event or from an in-request allocation.
///
/// Deliberately infallible: by the time this is called, the bed/reservation
/// state it's describing has *already been committed* to Postgres by the
/// caller. This is a best-effort live notification on top of that already-
/// durable fact, not part of the operation's own correctness -- so a
/// transient failure fetching the region/count for display purposes must
/// never fail the caller's request or, in the Kafka consumer, trigger a
/// retry (retrying would re-run apply_bed_status_event, which would then
/// see the event as a Duplicate and skip re-publishing, permanently losing
/// the notification instead of just delaying it). Log and move on instead.
pub async fn publish_bed_update(
    state: &AppState,
    shelter_id: Uuid,
    bed_id: Uuid,
    status: BedStatus,
    event_timestamp_ms: i64,
) {
    let region = match matcher::region_of_shelter(&state.pool, shelter_id).await {
        Ok(region) => region,
        Err(err) => {
            tracing::error!(%err, %shelter_id, "failed to look up region for availability notification, dropping it");
            return;
        }
    };
    let available_count = match matcher::available_count_in_region(&state.pool, &region).await {
        Ok(count) => count,
        Err(err) => {
            tracing::error!(%err, %region, "failed to look up available count for availability notification, dropping it");
            return;
        }
    };

    let update = proto::BedAvailabilityUpdate {
        shelter_id: shelter_id.to_string(),
        bed_id: bed_id.to_string(),
        status: proto::BedStatus::from(status) as i32,
        available_count_in_region: available_count,
        event_timestamp_ms,
        delivered_timestamp_ms: chrono::Utc::now().timestamp_millis(),
        region,
    };

    // Err just means there are no subscribers right now -- not a failure.
    let _ = state.availability_tx.send(update);
}
