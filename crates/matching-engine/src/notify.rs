use common::error::AppError;
use common::model::BedStatus;
use common::proto;
use uuid::Uuid;

use crate::matcher;
use crate::state::AppState;

/// Publishes a live availability update to every subscribed
/// `StreamAvailability` caller. Called after any bed-state change, whether
/// it came from a Kafka intake event or from an in-request allocation.
pub async fn publish_bed_update(
    state: &AppState,
    shelter_id: Uuid,
    bed_id: Uuid,
    status: BedStatus,
    event_timestamp_ms: i64,
) -> Result<(), AppError> {
    let region = matcher::region_of_shelter(&state.pool, shelter_id).await?;
    let available_count = matcher::available_count_in_region(&state.pool, &region).await?;

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
    Ok(())
}
