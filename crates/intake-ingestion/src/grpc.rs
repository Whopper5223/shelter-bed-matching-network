use std::time::Duration;

use common::proto::{self, intake_service_server::IntakeService, BedStatus};
use prost::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tonic::{Request, Response, Status};

pub struct IntakeServiceImpl {
    pub producer: FutureProducer,
    pub topic: String,
}

#[tonic::async_trait]
impl IntakeService for IntakeServiceImpl {
    /// Validates and publishes a bed-status event to Kafka, keyed by
    /// shelter_id so a shelter's events stay ordered within one partition.
    /// This is the only write path for bed state -- matching-engine never
    /// accepts bed-status writes directly, only referrals.
    async fn submit_bed_status(
        &self,
        request: Request<proto::SubmitBedStatusRequest>,
    ) -> Result<Response<proto::SubmitBedStatusResponse>, Status> {
        let event = request
            .into_inner()
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        if event.event_id.is_empty() || event.shelter_id.is_empty() || event.bed_id.is_empty() {
            return Err(Status::invalid_argument(
                "event_id, shelter_id, and bed_id are all required",
            ));
        }

        // A real intake terminal only ever reports one of these three
        // transitions (see the proto's own doc comment on BedStatusEvent).
        // `RESERVED` is a state matching-engine's allocator assigns
        // internally, never one a terminal should be able to declare
        // directly -- accepting it here would let a caller mark a bed
        // reserved with no backing reservation row, silently pulling real
        // capacity out of the matching pool. An unset/out-of-range status
        // (proto3's default `BED_STATUS_UNSPECIFIED`) must also be rejected
        // rather than silently treated as `AVAILABLE` downstream.
        match event.status() {
            BedStatus::Available | BedStatus::Occupied | BedStatus::Maintenance => {}
            BedStatus::Unspecified | BedStatus::Reserved => {
                return Err(Status::invalid_argument(
                    "status must be one of AVAILABLE, OCCUPIED, or MAINTENANCE",
                ));
            }
        }

        let key = event.shelter_id.clone();
        let payload = event.encode_to_vec();

        self.producer
            .send(
                FutureRecord::to(&self.topic).key(&key).payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| {
                Status::internal(format!("failed to publish to kafka: {err}"))
            })?;

        Ok(Response::new(proto::SubmitBedStatusResponse {
            accepted: true,
            message: "accepted".into(),
        }))
    }
}
