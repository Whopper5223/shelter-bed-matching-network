mod config;
mod grpc;

use common::proto::intake_service_server::IntakeServiceServer;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

use config::Config;
use grpc::IntakeServiceImpl;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let service = IntakeServiceImpl {
        producer,
        topic: config.kafka_topic.clone(),
    };

    let addr = config.grpc_addr.parse()?;
    tracing::info!(
        %addr,
        brokers = %config.kafka_brokers,
        topic = %config.kafka_topic,
        "intake-ingestion gRPC server listening"
    );

    Server::builder()
        .add_service(IntakeServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
