use std::sync::Mutex;

/// Tracks end-to-end latency (intake terminal -> Kafka -> matching engine ->
/// gateway stream -> this process) for every availability update observed,
/// so we can report a real measured number instead of an assumed one.
#[derive(Default)]
pub struct LatencyStats {
    samples_ms: Mutex<Vec<i64>>,
}

impl LatencyStats {
    pub fn record(&self, latency_ms: i64) {
        self.samples_ms.lock().unwrap().push(latency_ms);
    }

    pub fn print_summary(&self) {
        let mut samples = self.samples_ms.lock().unwrap().clone();
        if samples.is_empty() {
            tracing::info!("no availability updates observed during this run");
            return;
        }
        samples.sort_unstable();
        let count = samples.len();
        let avg = samples.iter().sum::<i64>() as f64 / count as f64;
        let p50 = samples[count / 2];
        let p99 = samples[(count * 99 / 100).min(count - 1)];
        let max = *samples.last().unwrap();
        tracing::info!(
            count,
            avg_ms = format!("{avg:.1}"),
            p50_ms = p50,
            p99_ms = p99,
            max_ms = max,
            "end-to-end availability update latency"
        );
    }
}
