use rand::random_range;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone)]
pub struct SeededBed {
    pub id: Uuid,
    pub shelter_id: Uuid,
    pub region: String,
}

const REGIONS: [&str; 3] = ["north-metro", "south-metro", "east-county"];
const UNIT_TYPES: [&str; 5] = ["general", "family", "womens_only", "mens_only", "youth"];

/// Seeds `num_shelters` shelters across a few regions, each with
/// `beds_per_shelter` beds of randomized type/attributes. Most beds start
/// `occupied` so the simulator's check-out events create real newly
/// available capacity for simulated referrals to compete over, instead of
/// everything being trivially available from the start.
pub async fn seed(
    pool: &PgPool,
    num_shelters: usize,
    beds_per_shelter: usize,
) -> anyhow::Result<Vec<SeededBed>> {
    let mut beds = Vec::with_capacity(num_shelters * beds_per_shelter);

    for i in 0..num_shelters {
        let shelter_id = Uuid::new_v4();
        let region = REGIONS[i % REGIONS.len()];
        sqlx::query("INSERT INTO shelters (id, name, region) VALUES ($1, $2, $3)")
            .bind(shelter_id)
            .bind(format!("Simulated Shelter {i}"))
            .bind(region)
            .execute(pool)
            .await?;

        for _ in 0..beds_per_shelter {
            let bed_id = Uuid::new_v4();
            let unit_type = UNIT_TYPES[random_range(0..UNIT_TYPES.len())];
            let allows_pets = random_range(0..100) < 25;
            let sobriety_required = random_range(0..100) < 40;
            let ada_accessible = random_range(0..100) < 20;
            let status = if random_range(0..100) < 70 {
                "occupied"
            } else {
                "available"
            };

            sqlx::query(
                "INSERT INTO beds (id, shelter_id, unit_type, allows_pets, sobriety_required, ada_accessible, status)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(bed_id)
            .bind(shelter_id)
            .bind(unit_type)
            .bind(allows_pets)
            .bind(sobriety_required)
            .bind(ada_accessible)
            .bind(status)
            .execute(pool)
            .await?;

            beds.push(SeededBed {
                id: bed_id,
                shelter_id,
                region: region.to_string(),
            });
        }
    }

    Ok(beds)
}
