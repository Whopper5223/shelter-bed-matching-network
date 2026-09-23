#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("invalid enum value in database row: {0}")]
    InvalidEnumValue(String),
    #[error("not found: {0}")]
    NotFound(String),
}

impl From<AppError> for tonic::Status {
    fn from(err: AppError) -> Self {
        match err {
            AppError::NotFound(msg) => tonic::Status::not_found(msg),
            AppError::InvalidEnumValue(msg) => tonic::Status::internal(msg),
            AppError::Database(e) => tonic::Status::internal(format!("database error: {e}")),
        }
    }
}
