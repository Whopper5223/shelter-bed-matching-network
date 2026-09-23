pub mod config;
pub mod grpc;
pub mod kafka_consumer;
pub mod matcher;
pub mod notify;
pub mod state;

use std::sync::Arc;

use common::proto::matching_service_server::MatchingServiceServer;
use tokio::sync::broadcast;
use tonic::transport::Server;

use config::Config;
use grpc::MatchingServiceImpl;
use state::AppState;

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env();

    let pool = common::db::connect_with_retry(&config.database_url).await?;
    common::db::run_migrations(&pool).await?;
    tracing::info!("migrations applied");

    let (availability_tx, _rx) = broadcast::channel(1024);
    let state = Arc::new(AppState {
        pool,
        availability_tx,
    });

    let kafka_state = state.clone();
    let brokers = config.kafka_brokers.clone();
    let topic = config.kafka_topic.clone();
    let group_id = config.kafka_group_id.clone();
    tokio::spawn(async move {
        if let Err(err) = kafka_consumer::run(kafka_state, &brokers, &topic, &group_id).await {
            tracing::error!(%err, "kafka consumer task exited");
        }
    });

    let addr = config.grpc_addr.parse()?;
    tracing::info!(%addr, "matching-engine gRPC server listening");

    Server::builder()
        .add_service(MatchingServiceServer::new(MatchingServiceImpl { state }))
        .serve(addr)
        .await?;

    Ok(())
}
