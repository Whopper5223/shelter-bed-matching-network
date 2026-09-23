use sqlx::postgres::{PgPool, PgPoolOptions};

/// Connects to Postgres with a small retry loop, since in docker-compose /
/// Kubernetes the database container can still be coming up when this
/// service starts.
pub async fn connect_with_retry(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(err) if attempt < 20 => {
                tracing::warn!(attempt, %err, "database not ready yet, retrying");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Runs the embedded migrations in `migrations/` from the workspace root.
/// Only matching-engine calls this; it's the sole owner of the schema.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}
