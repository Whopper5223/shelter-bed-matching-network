pub struct Config {
    pub grpc_addr: String,
    pub matching_engine_addr: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            grpc_addr: std::env::var("GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50053".into()),
            matching_engine_addr: std::env::var("MATCHING_ENGINE_ADDR")
                .unwrap_or_else(|_| "http://localhost:50051".into()),
        }
    }
}
