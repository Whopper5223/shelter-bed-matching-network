pub struct Config {
    pub grpc_addr: String,
    pub kafka_brokers: String,
    pub kafka_topic: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            grpc_addr: std::env::var("GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50052".into()),
            kafka_brokers: std::env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".into()),
            kafka_topic: std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "bed-events".into()),
        }
    }
}
