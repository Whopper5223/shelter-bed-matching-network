use std::time::Duration;

use common::proto::{self, intake_service_client::IntakeServiceClient};
use rand::random_range;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::seed::SeededBed;

/// One task per bed, each acting as that bed's intake terminal: it
/// alternates the bed between occupied and available on a random interval,
/// publishing a `BedStatusEvent` over the unary `SubmitBedStatus` RPC each
/// time, with a strictly increasing per-bed sequence number.
pub async fn run(client: IntakeServiceClient<Channel>, beds: Vec<SeededBed>, duration_secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(duration_secs);
    let mut handles = Vec::with_capacity(beds.len());

    for bed in beds {
        let mut client = client.clone();
        handles.push(tokio::spawn(async move {
            let mut sequence: i64 = 0;
            let mut currently_available = false;

            loop {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(random_range(300..3000))).await;

                sequence += 1;
                currently_available = !currently_available;
                let status = if currently_available {
                    proto::BedStatus::Available
                } else {
                    proto::BedStatus::Occupied
                };

                let event = proto::BedStatusEvent {
                    event_id: Uuid::new_v4().to_string(),
                    shelter_id: bed.shelter_id.to_string(),
                    bed_id: bed.id.to_string(),
                    status: status as i32,
                    sequence,
                    occurred_at_ms: chrono::Utc::now().timestamp_millis(),
                };

                let request = proto::SubmitBedStatusRequest { event: Some(event) };
                if let Err(err) = client.submit_bed_status(request).await {
                    tracing::warn!(%err, bed_id = %bed.id, "failed to submit bed status event");
                }
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }
}
