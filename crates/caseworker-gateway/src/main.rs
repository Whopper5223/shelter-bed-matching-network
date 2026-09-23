mod config;
mod grpc;

use common::proto::caseworker_service_server::CaseworkerServiceServer;
use common::proto::matching_service_client::MatchingServiceClient;
use tonic::transport::{Endpoint, Server};
use tracing_subscriber::EnvFilter;

use config::Config;
use grpc::CaseworkerServiceImpl;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env();

    let channel = Endpoint::from_shared(config.matching_engine_addr.clone())?.connect_lazy();
    let matching_client = MatchingServiceClient::new(channel);

    let service = CaseworkerServiceImpl { matching_client };

    let addr = config.grpc_addr.parse()?;
    tracing::info!(
        %addr,
        upstream = %config.matching_engine_addr,
        "caseworker-gateway gRPC server listening"
    );

    Server::builder()
        .add_service(CaseworkerServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
