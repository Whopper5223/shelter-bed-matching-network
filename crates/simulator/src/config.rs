pub struct Config {
    pub database_url: String,
    pub intake_addr: String,
    pub gateway_addr: String,
    pub num_shelters: usize,
    pub beds_per_shelter: usize,
    pub duration_secs: u64,
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://shelterbed:shelterbed@localhost:5433/shelterbed".into()
            }),
            intake_addr: std::env::var("INTAKE_ADDR")
                .unwrap_or_else(|_| "http://localhost:50052".into()),
            gateway_addr: std::env::var("GATEWAY_ADDR")
                .unwrap_or_else(|_| "http://localhost:50053".into()),
            num_shelters: env_or("NUM_SHELTERS", 12),
            beds_per_shelter: env_or("BEDS_PER_SHELTER", 8),
            duration_secs: env_or("SIMULATION_SECONDS", 30),
        }
    }
}
