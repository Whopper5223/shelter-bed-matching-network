use sqlx::PgPool;
use uuid::Uuid;

/// Connects to the Postgres instance used for tests. Run `docker compose up
/// -d postgres` first, or set TEST_DATABASE_URL to point elsewhere.
pub async fn setup_pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://shelterbed:shelterbed@localhost:5433/shelterbed".into());
    let pool = common::db::connect_with_retry(&url)
        .await
        .expect("connect to postgres for tests -- is `docker compose up -d postgres` running?");
    common::db::run_migrations(&pool)
        .await
        .expect("run migrations for tests");
    pool
}

pub async fn reset(pool: &PgPool) {
    sqlx::query("TRUNCATE reservations, applied_events, referrals, beds, shelters CASCADE")
        .execute(pool)
        .await
        .expect("truncate tables between tests");
}

pub async fn insert_shelter(pool: &PgPool, region: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO shelters (id, name, region) VALUES ($1, 'Test Shelter', $2)")
        .bind(id)
        .bind(region)
        .execute(pool)
        .await
        .expect("insert shelter");
    id
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_bed(
    pool: &PgPool,
    shelter_id: Uuid,
    unit_type: &str,
    allows_pets: bool,
    sobriety_required: bool,
    ada_accessible: bool,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO beds (id, shelter_id, unit_type, allows_pets, sobriety_required, ada_accessible, status)
         VALUES ($1, $2, $3, $4, $5, $6, 'available')",
    )
    .bind(id)
    .bind(shelter_id)
    .bind(unit_type)
    .bind(allows_pets)
    .bind(sobriety_required)
    .bind(ada_accessible)
    .execute(pool)
    .await
    .expect("insert bed");
    id
}
