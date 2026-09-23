use std::pin::Pin;
use std::sync::Arc;

use common::model::{BedStatus, ReferralNeeds};
use common::proto::{self, matching_service_server::MatchingService};
use futures::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::matcher::{self, SubmitOutcome};
use crate::notify;
use crate::state::AppState;

pub struct MatchingServiceImpl {
    pub state: Arc<AppState>,
}

#[tonic::async_trait]
impl MatchingService for MatchingServiceImpl {
    type StreamAvailabilityStream =
        Pin<Box<dyn Stream<Item = Result<proto::BedAvailabilityUpdate, Status>> + Send + 'static>>;

    async fn match_referral(
        &self,
        request: Request<proto::SubmitReferralRequest>,
    ) -> Result<Response<proto::SubmitReferralResponse>, Status> {
        let req = request.into_inner();
        let criteria = req
            .criteria
            .ok_or_else(|| Status::invalid_argument("criteria is required"))?;

        let needs = ReferralNeeds {
            unit_type_required: criteria.unit_type_required().into(),
            needs_pet_friendly: criteria.needs_pet_friendly,
            needs_sobriety_free: criteria.needs_sobriety_free_environment,
            needs_ada_accessible: criteria.needs_ada_accessible,
        };

        let outcome = matcher::submit_referral(
            &self.state.pool,
            &req.caseworker_id,
            &req.region,
            needs,
            criteria.vulnerability_score,
            criteria.family_size,
        )
        .await?;

        match outcome {
            SubmitOutcome::Matched(result) => {
                notify::publish_bed_update(
                    &self.state,
                    result.shelter_id,
                    result.bed_id,
                    BedStatus::Reserved,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;

                Ok(Response::new(proto::SubmitReferralResponse {
                    referral_id: result.referral_id.to_string(),
                    status: proto::ReferralStatus::Matched as i32,
                    matched_bed: Some(proto::BedRef {
                        bed_id: result.bed_id.to_string(),
                        shelter_id: result.shelter_id.to_string(),
                    }),
                }))
            }
            SubmitOutcome::Pending { referral_id } => {
                Ok(Response::new(proto::SubmitReferralResponse {
                    referral_id: referral_id.to_string(),
                    status: proto::ReferralStatus::Pending as i32,
                    matched_bed: None,
                }))
            }
        }
    }

    async fn stream_availability(
        &self,
        request: Request<proto::AvailabilitySubscribeRequest>,
    ) -> Result<Response<Self::StreamAvailabilityStream>, Status> {
        let region_filter = request.into_inner().region;
        let rx = self.state.availability_tx.subscribe();

        let stream = BroadcastStream::new(rx).filter_map(move |item| match item {
            Ok(update) if region_filter.is_empty() || update.region == region_filter => {
                Some(Ok(update))
            }
            Ok(_) => None,
            // A slow subscriber missed some updates. Drop and keep going
            // rather than fail the stream -- availability is eventually
            // reflected by the next update either way.
            Err(_lagged) => None,
        });

        Ok(Response::new(Box::pin(stream)))
    }
}
