mod config;
mod events;
mod listener;
mod referrals;
mod seed;
mod stats;

use std::sync::Arc;
use std::time::Duration;

use common::proto::caseworker_service_client::CaseworkerServiceClient;
use common::proto::intake_service_client::IntakeServiceClient;
use tonic::transport::Endpoint;
use tracing_subscriber::EnvFilter;

use config::Config;
use stats::LatencyStats;

/// Stands in for N shelters' intake terminals plus a stream of caseworkers.
/// Seeds fixture shelters/beds directly in Postgres (a demo-only shortcut --
/// real intake terminals never touch the database), then drives the whole
/// pipeline purely over gRPC: bed check-in/check-out events into
/// intake-ingestion, referrals into caseworker-gateway, and a live
/// subscription to caseworker-gateway's availability stream to measure real
/// end-to-end latency.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env();

    let pool = common::db::connect_with_retry(&config.database_url).await?;
    let beds = seed::seed(&pool, config.num_shelters, config.beds_per_shelter).await?;
    tracing::info!(
        num_shelters = config.num_shelters,
        num_beds = beds.len(),
        "seeded simulated shelters and beds"
    );

    let intake_channel = Endpoint::from_shared(config.intake_addr.clone())?.connect_lazy();
    let intake_client = IntakeServiceClient::new(intake_channel);

    let gateway_channel = Endpoint::from_shared(config.gateway_addr.clone())?.connect_lazy();
    let referral_client = CaseworkerServiceClient::new(gateway_channel.clone());
    let stream_client = CaseworkerServiceClient::new(gateway_channel);

    let stats = Arc::new(LatencyStats::default());

    let listener_stats = stats.clone();
    let listener_handle = tokio::spawn(listener::run(stream_client, listener_stats));

    // Give the listener a moment to establish its stream before the event
    // storm starts, so early updates aren't missed.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let events_handle = tokio::spawn(events::run(
        intake_client,
        beds.clone(),
        config.duration_secs,
    ));
    let referrals_handle =
        tokio::spawn(referrals::run(referral_client, beds, config.duration_secs));

    let _ = events_handle.await;
    let _ = referrals_handle.await;

    // Let any in-flight availability updates land before reporting.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    listener_handle.abort();

    stats.print_summary();

    Ok(())
}
