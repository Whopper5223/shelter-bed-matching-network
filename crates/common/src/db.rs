use std::time::{Duration, Instant};

use sqlx::postgres::{PgPool, PgPoolOptions};

/// How long to keep retrying a Postgres connection before giving up.
///
/// In docker-compose, matching-engine only starts after postgres's own
/// healthcheck (`pg_isready`) passes, so this budget is rarely exercised
/// there. In Kubernetes, nothing gates matching-engine's pod on postgres's
/// pod being ready (only the simulator Job's initContainer waits on
/// anything) -- a Deployment's crashed container does get restarted
/// automatically, but a short budget here just means more restarts before
/// it succeeds. 60 seconds comfortably covers a cold image pull plus a
/// slow Postgres first-start, while a compose service with no restart
/// policy configured (the default) simply stays down if this is ever too
/// short -- better to err generous.
const CONNECT_RETRY_BUDGET: Duration = Duration::from_secs(60);
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// Connects to Postgres with a retry loop, since in docker-compose /
/// Kubernetes the database container can still be coming up when this
/// service starts.
pub async fn connect_with_retry(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let deadline = Instant::now() + CONNECT_RETRY_BUDGET;
    let mut attempt = 0;
    loop {
        attempt += 1;
        match PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(err) if Instant::now() < deadline => {
                tracing::warn!(attempt, %err, "database not ready yet, retrying");
                tokio::time::sleep(CONNECT_RETRY_INTERVAL).await;
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
