use common::proto::caseworker_service_server::CaseworkerService;
use common::proto::matching_service_client::MatchingServiceClient;
use common::proto::{self};
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

/// Thin proxy in front of matching-engine's MatchingService. Kept as its
/// own service/deployment so the public-facing gateway can be scaled,
/// rate-limited, and secured independently of the internal matching engine.
pub struct CaseworkerServiceImpl {
    pub matching_client: MatchingServiceClient<Channel>,
}

#[tonic::async_trait]
impl CaseworkerService for CaseworkerServiceImpl {
    type StreamAvailabilityStream = Streaming<proto::BedAvailabilityUpdate>;

    async fn submit_referral(
        &self,
        request: Request<proto::SubmitReferralRequest>,
    ) -> Result<Response<proto::SubmitReferralResponse>, Status> {
        let mut client = self.matching_client.clone();
        client.match_referral(request.into_inner()).await
    }

    async fn stream_availability(
        &self,
        request: Request<proto::AvailabilitySubscribeRequest>,
    ) -> Result<Response<Self::StreamAvailabilityStream>, Status> {
        let mut client = self.matching_client.clone();
        client.stream_availability(request.into_inner()).await
    }
}
