pub struct Config {
    pub database_url: String,
    pub grpc_addr: String,
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_group_id: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://shelterbed:shelterbed@localhost:5433/shelterbed".into()
            }),
            grpc_addr: std::env::var("GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".into()),
            kafka_brokers: std::env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".into()),
            kafka_topic: std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "bed-events".into()),
            kafka_group_id: std::env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "matching-engine".into()),
        }
    }
}
