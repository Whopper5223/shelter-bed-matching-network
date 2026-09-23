pub mod db;
pub mod error;
pub mod model;

pub mod proto {
    tonic::include_proto!("shelterbed.v1");
}

pub use error::AppError;
