use std::sync::Arc;
use std::time::Duration;

use common::proto::{self, caseworker_service_client::CaseworkerServiceClient};
use tonic::transport::Channel;

use crate::stats::LatencyStats;

/// Subscribes to the live availability stream (the same one real
/// caseworkers would use) and records end-to-end latency for every update:
/// `delivered_timestamp_ms - event_timestamp_ms`, i.e. time from the
/// original intake-terminal event to this subscriber receiving it.
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
                    let latency_ms = update.delivered_timestamp_ms - update.event_timestamp_ms;
                    stats.record(latency_ms);
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
