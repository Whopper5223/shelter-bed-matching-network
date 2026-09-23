use std::sync::Arc;
use std::time::Duration;

use common::proto::{self, caseworker_service_client::CaseworkerServiceClient};
use tonic::transport::Channel;

use crate::stats::LatencyStats;

/// Subscribes to the live availability stream (the same one real
/// caseworkers would use) and records end-to-end latency for every update,
/// as `(this process's clock on receipt) - event_timestamp_ms`. This
/// deliberately does *not* use the update's own `delivered_timestamp_ms`:
/// that field is stamped by matching-engine at broadcast-send time, so
/// subtracting it would only measure "intake -> Kafka -> engine", missing
/// the gateway proxy hop and actual network delivery to this subscriber --
/// the rest of the path the "sub-second visibility" claim is about.
/// docker-compose and kind share a clock across containers/pods, so this
/// comparison is valid without NTP-syncing anything extra.
///
/// Retries connecting indefinitely with a short backoff: in Kubernetes all
/// pods can start at the same instant, so caseworker-gateway may not be
/// accepting connections yet when this task's first attempt runs. The
/// caller cancels this task (via `JoinHandle::abort`) once the simulation
/// is done.
pub async fn run(mut client: CaseworkerServiceClient<Channel>, stats: Arc<LatencyStats>) {
    loop {
        let request = proto::AvailabilitySubscribeRequest {
            region: String::new(),
        };

        let mut stream = match client.stream_availability(request).await {
            Ok(response) => response.into_inner(),
            Err(err) => {
                tracing::warn!(%err, "failed to open availability stream, retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        loop {
            match stream.message().await {
                Ok(Some(update)) => {
                    let received_at_ms = chrono::Utc::now().timestamp_millis();
                    stats.record(received_at_ms - update.event_timestamp_ms);
                }
                Ok(None) => return,
                Err(err) => {
                    tracing::warn!(%err, "availability stream error, reconnecting");
                    break;
                }
            }
        }
    }
}
