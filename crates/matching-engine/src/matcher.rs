use common::error::AppError;
use common::model::{is_eligible, BedAttributes, BedStatus, ReferralNeeds, UnitType};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct AllocationResult {
    pub bed_id: Uuid,
    pub shelter_id: Uuid,
    pub referral_id: Uuid,
}

#[derive(Debug)]
pub enum SubmitOutcome {
    Matched(AllocationResult),
    Pending { referral_id: Uuid },
}

/// Inserts a referral as `pending`, then repeatedly tries candidate beds it
/// is structurally eligible for. A bed is only actually awarded to this
/// referral if [`allocate_for_bed`] independently decides it is the
/// highest-vulnerability *eligible pending* referral for that specific bed
/// -- so a referral submitted a second ago cannot jump ahead of one with a
/// higher vulnerability score that is already waiting. This is what makes
/// matching priority-ordered rather than first-come-first-served.
///
/// Returns the outcome for *this* referral, plus every allocation this call
/// caused as a side effect (candidate beds that ended up awarded to some
/// *other*, higher-priority pending referral instead). The caller should
/// publish an availability update for each of those too -- otherwise a bed
/// this call reserved for someone else would sit invisible to live
/// subscribers.
#[allow(clippy::too_many_arguments)]
pub async fn submit_referral(
    pool: &PgPool,
    caseworker_id: &str,
    region: &str,
    needs: ReferralNeeds,
    vulnerability_score: i32,
    family_size: i32,
) -> Result<(SubmitOutcome, Vec<AllocationResult>), AppError> {
    let referral_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO referrals
            (id, caseworker_id, region, family_size, unit_type_required,
             needs_pet_friendly, needs_sobriety_free, needs_ada_accessible,
             vulnerability_score, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')",
    )
    .bind(referral_id)
    .bind(caseworker_id)
    .bind(region)
    .bind(family_size)
    .bind(needs.unit_type_required.as_db_str())
    .bind(needs.needs_pet_friendly)
    .bind(needs.needs_sobriety_free)
    .bind(needs.needs_ada_accessible)
    .bind(vulnerability_score)
    .execute(pool)
    .await?;

    let mut side_allocations = Vec::new();

    // Bounded: at most one iteration per matching bed that exists, so this
    // always terminates even under heavy concurrent contention.
    for _ in 0..500 {
        let candidate: Option<(Uuid,)> = sqlx::query_as(
            "SELECT beds.id FROM beds
             JOIN shelters ON shelters.id = beds.shelter_id
             WHERE beds.status = 'available'
               AND shelters.region = $1
               AND beds.unit_type = $2
               AND (beds.allows_pets = TRUE OR $3 = FALSE)
               AND (beds.sobriety_required = FALSE OR $4 = FALSE)
               AND (beds.ada_accessible = TRUE OR $5 = FALSE)
             ORDER BY beds.id
             LIMIT 1",
        )
        .bind(region)
        .bind(needs.unit_type_required.as_db_str())
        .bind(needs.needs_pet_friendly)
        .bind(needs.needs_sobriety_free)
        .bind(needs.needs_ada_accessible)
        .fetch_optional(pool)
        .await?;

        let Some((bed_id,)) = candidate else {
            let outcome = resolve_final_outcome(pool, referral_id).await?;
            return Ok((outcome, side_allocations));
        };

        match allocate_for_bed(pool, bed_id).await? {
            Some(result) if result.referral_id == referral_id => {
                return Ok((SubmitOutcome::Matched(result), side_allocations));
            }
            // Bed went to a higher-priority referral, or was taken /
            // temporarily lock-contended between the two queries above --
            // either way, record it (if it's a real allocation) and try the
            // next candidate.
            Some(result) => {
                side_allocations.push(result);
            }
            None => {}
        }
    }

    let outcome = resolve_final_outcome(pool, referral_id).await?;
    Ok((outcome, side_allocations))
}

/// Re-reads this referral's true current state before reporting `Pending`.
/// Necessary because, under concurrency, this referral can be matched by a
/// *different* call's `allocate_for_bed` (one triggered by another
/// referral's search, or by a bed becoming available) at any point --
/// including after this call's own search already gave up. Without this
/// check, a caseworker could be told "no bed" for a referral that, in the
/// database, already holds one.
async fn resolve_final_outcome(
    pool: &PgPool,
    referral_id: Uuid,
) -> Result<SubmitOutcome, AppError> {
    // 'checked_in' counts too, not just 'active': a referral whose bed was
    // occupied (a normal check-in) in the moments between being matched and
    // this re-check still holds that bed just as much as one still 'active'.
    let row: Option<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT reservations.bed_id, beds.shelter_id
         FROM reservations
         JOIN beds ON beds.id = reservations.bed_id
         WHERE reservations.referral_id = $1 AND reservations.status IN ('active', 'checked_in')",
    )
    .bind(referral_id)
    .fetch_optional(pool)
    .await?;

    Ok(match row {
        Some((bed_id, shelter_id)) => SubmitOutcome::Matched(AllocationResult {
            bed_id,
            shelter_id,
            referral_id,
        }),
        None => SubmitOutcome::Pending { referral_id },
    })
}

/// The single allocation routine, called both when a referral is submitted
/// and whenever a bed becomes available. Locks exactly the bed row and the
/// one referral row it commits to -- nothing else -- so concurrent calls
/// for different beds never block each other, and two concurrent calls
/// racing for the *same* bed serialize on that bed's row lock, guaranteeing
/// only one can win.
pub async fn allocate_for_bed(
    pool: &PgPool,
    bed_id: Uuid,
) -> Result<Option<AllocationResult>, AppError> {
    let mut tx = pool.begin().await?;

    let bed_row: Option<(Uuid, String, bool, bool, bool, String)> = sqlx::query_as(
        "SELECT shelter_id, unit_type, allows_pets, sobriety_required, ada_accessible, status
         FROM beds WHERE id = $1 FOR UPDATE",
    )
    .bind(bed_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some((shelter_id, unit_type_str, allows_pets, sobriety_required, ada_accessible, status)) =
        bed_row
    else {
        return Ok(None);
    };

    if status != "available" {
        tx.rollback().await?;
        return Ok(None);
    }

    let (region,): (String,) = sqlx::query_as("SELECT region FROM shelters WHERE id = $1")
        .bind(shelter_id)
        .fetch_one(&mut *tx)
        .await?;

    let bed_attrs = BedAttributes {
        unit_type: UnitType::from_db_str(&unit_type_str)?,
        allows_pets,
        sobriety_required,
        ada_accessible,
    };

    // Mirrors is_eligible() in SQL so we lock (FOR UPDATE) only the single
    // referral row we are about to commit to, instead of every pending
    // referral in the region.
    let chosen: Option<(Uuid, String, bool, bool, bool)> = sqlx::query_as(
        "SELECT id, unit_type_required, needs_pet_friendly, needs_sobriety_free, needs_ada_accessible
         FROM referrals
         WHERE region = $1
           AND status = 'pending'
           AND unit_type_required = $2
           AND (needs_pet_friendly = FALSE OR $3 = TRUE)
           AND (needs_sobriety_free = FALSE OR $4 = FALSE)
           AND (needs_ada_accessible = FALSE OR $5 = TRUE)
         ORDER BY vulnerability_score DESC, created_at ASC
         FOR UPDATE SKIP LOCKED
         LIMIT 1",
    )
    .bind(&region)
    .bind(bed_attrs.unit_type.as_db_str())
    .bind(bed_attrs.allows_pets)
    .bind(bed_attrs.sobriety_required)
    .bind(bed_attrs.ada_accessible)
    .fetch_optional(&mut *tx)
    .await?;

    let Some((
        referral_id,
        unit_type_required,
        needs_pet_friendly,
        needs_sobriety_free,
        needs_ada_accessible,
    )) = chosen
    else {
        tx.rollback().await?;
        return Ok(None);
    };

    debug_assert!(
        is_eligible(
            &bed_attrs,
            &ReferralNeeds {
                unit_type_required: UnitType::from_db_str(&unit_type_required)?,
                needs_pet_friendly,
                needs_sobriety_free,
                needs_ada_accessible,
            },
        ),
        "SQL eligibility filter and is_eligible() have drifted apart"
    );

    let reservation_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO reservations (id, bed_id, referral_id, status) VALUES ($1, $2, $3, 'active')",
    )
    .bind(reservation_id)
    .bind(bed_id)
    .bind(referral_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query("UPDATE beds SET status = 'reserved' WHERE id = $1")
        .bind(bed_id)
        .execute(&mut *tx)
        .await?;

    sqlx::query("UPDATE referrals SET status = 'matched' WHERE id = $1")
        .bind(referral_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    Ok(Some(AllocationResult {
        bed_id,
        shelter_id,
        referral_id,
    }))
}

#[derive(Debug)]
pub enum ApplyOutcome {
    Applied {
        shelter_id: Uuid,
        became_available: bool,
    },
    Duplicate,
    Stale,
    UnknownBed,
}

/// Applies a bed-status event from Kafka idempotently: a redelivered
/// `event_id` is a no-op (checked against `applied_events`), and a
/// `sequence` at or below the bed's `last_event_sequence` is a no-op too
/// (guards against out-of-order delivery). Both checks happen inside the
/// same transaction as the state change, and the row is locked with
/// `FOR UPDATE` for the duration.
pub async fn apply_bed_status_event(
    pool: &PgPool,
    event_id: Uuid,
    bed_id: Uuid,
    new_status: BedStatus,
    sequence: i64,
) -> Result<ApplyOutcome, AppError> {
    let mut tx = pool.begin().await?;

    let inserted = sqlx::query(
        "INSERT INTO applied_events (event_id, bed_id) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(bed_id)
    .execute(&mut *tx)
    .await?;

    if inserted.rows_affected() == 0 {
        tx.rollback().await?;
        return Ok(ApplyOutcome::Duplicate);
    }

    let row: Option<(Uuid, i64, String)> = sqlx::query_as(
        "SELECT shelter_id, last_event_sequence, status FROM beds WHERE id = $1 FOR UPDATE",
    )
    .bind(bed_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some((shelter_id, last_seq, current_status)) = row else {
        tx.rollback().await?;
        return Ok(ApplyOutcome::UnknownBed);
    };

    if sequence <= last_seq {
        tx.rollback().await?;
        return Ok(ApplyOutcome::Stale);
    }

    sqlx::query("UPDATE beds SET status = $1, last_event_sequence = $2 WHERE id = $3")
        .bind(new_status.as_db_str())
        .bind(sequence)
        .bind(bed_id)
        .execute(&mut *tx)
        .await?;

    // Reservation lifecycle bookkeeping. The no-double-booking guarantee
    // itself rests entirely on the partial unique indexes plus the locking
    // above, not on this -- but it IS load-bearing for availability: if a
    // bed goes back to `available` (or into `maintenance`) while it still
    // holds an active/checked_in reservation -- a no-show, a cancellation
    // before check-in, or a maintenance hold -- that reservation has to be
    // released here, or the next allocate_for_bed on this bed hits the
    // unique index as a real error instead of proceeding. The displaced
    // referral goes back to `pending` (keeping its original created_at, so
    // it doesn't lose its place in line) so it can be matched to a
    // different bed.
    if new_status == BedStatus::Occupied {
        sqlx::query(
            "UPDATE reservations SET status = 'checked_in' WHERE bed_id = $1 AND status = 'active'",
        )
        .bind(bed_id)
        .execute(&mut *tx)
        .await?;
    } else if matches!(new_status, BedStatus::Available | BedStatus::Maintenance) {
        let released: Option<(Uuid,)> = sqlx::query_as(
            "UPDATE reservations SET status = 'released'
             WHERE bed_id = $1 AND status IN ('active', 'checked_in')
             RETURNING referral_id",
        )
        .bind(bed_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some((referral_id,)) = released {
            sqlx::query(
                "UPDATE referrals SET status = 'pending' WHERE id = $1 AND status = 'matched'",
            )
            .bind(referral_id)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;

    let became_available = new_status == BedStatus::Available && current_status != "available";
    Ok(ApplyOutcome::Applied {
        shelter_id,
        became_available,
    })
}

pub async fn available_count_in_region(pool: &PgPool, region: &str) -> Result<i64, AppError> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM beds
         JOIN shelters ON shelters.id = beds.shelter_id
         WHERE shelters.region = $1 AND beds.status = 'available'",
    )
    .bind(region)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

pub async fn region_of_shelter(pool: &PgPool, shelter_id: Uuid) -> Result<String, AppError> {
    let (region,): (String,) = sqlx::query_as("SELECT region FROM shelters WHERE id = $1")
        .bind(shelter_id)
        .fetch_one(pool)
        .await?;
    Ok(region)
}
