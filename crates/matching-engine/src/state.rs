use common::proto;
use sqlx::PgPool;
use tokio::sync::broadcast;

pub struct AppState {
    pub pool: PgPool,
    pub availability_tx: broadcast::Sender<proto::BedAvailabilityUpdate>,
}
