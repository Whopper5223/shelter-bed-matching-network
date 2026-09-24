mod support;

use common::model::{ReferralNeeds, UnitType};
use matching_engine::matcher::{self, SubmitOutcome};
use std::sync::Arc;

const REGION: &str = "test-region-concurrency";

/// The headline correctness guarantee: with more simultaneous referrals
/// than available beds, every bed goes to exactly one referral. This is
/// what `FOR UPDATE SKIP LOCKED` on the bed row and the partial unique
/// index on `reservations(bed_id) WHERE status='active'` exist to
/// guarantee, together -- this test proves the pair actually holds under
/// real concurrent load against real Postgres.
// See tests/priority.rs for why file_serial (cross-process) rather than
// plain serial (in-process only) is needed here.
#[tokio::test]
#[serial_test::file_serial(shelterbed_db)]
async fn concurrent_referrals_never_double_book_a_bed() {
    let pool = support::setup_pool().await;
    support::reset(&pool).await;

    let shelter_id = support::insert_shelter(&pool, REGION).await;
    const NUM_BEDS: usize = 10;
    const NUM_REFERRALS: usize = 50;

    for _ in 0..NUM_BEDS {
        support::insert_bed(&pool, shelter_id, "general", false, false, false).await;
    }

    let needs = ReferralNeeds {
        unit_type_required: UnitType::General,
        needs_pet_friendly: false,
        needs_sobriety_free: false,
        needs_ada_accessible: false,
    };

    let pool = Arc::new(pool);
    let mut handles = Vec::with_capacity(NUM_REFERRALS);
    for i in 0..NUM_REFERRALS {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            matcher::submit_referral(
                &pool,
                &format!("caseworker-{i}"),
                REGION,
                needs,
                50, // equal vulnerability: this test isolates concurrency, not priority
                1,
            )
            .await
            .expect("submit_referral should not error")
        }));
    }

    // Each call's own return value must accurately reflect the final
    // database state, even when a *different* concurrent call is the one
    // whose allocate_for_bed actually assigned this referral's bed
    // (submit_referral re-checks before reporting Pending specifically to
    // guarantee this -- see resolve_final_outcome).
    let mut locally_matched = 0usize;
    let mut locally_pending = 0usize;
    for handle in handles {
        let (outcome, _side_allocations) = handle.await.expect("task panicked");
        match outcome {
            SubmitOutcome::Matched(_) => locally_matched += 1,
            SubmitOutcome::Pending { .. } => locally_pending += 1,
        }
    }
    assert_eq!(
        locally_matched, NUM_BEDS,
        "every call's own return value must reflect the true final outcome"
    );
    assert_eq!(locally_pending, NUM_REFERRALS - NUM_BEDS);

    let (active_reservations,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM reservations WHERE status = 'active'")
            .fetch_one(pool.as_ref())
            .await
            .unwrap();
    assert_eq!(active_reservations as usize, NUM_BEDS);

    let (distinct_beds,): (i64,) =
        sqlx::query_as("SELECT count(DISTINCT bed_id) FROM reservations WHERE status = 'active'")
            .fetch_one(pool.as_ref())
            .await
            .unwrap();
    assert_eq!(
        distinct_beds as usize, NUM_BEDS,
        "no bed should hold more than one active reservation"
    );

    let (reserved_beds,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM beds WHERE status = 'reserved'")
            .fetch_one(pool.as_ref())
            .await
            .unwrap();
    assert_eq!(reserved_beds as usize, NUM_BEDS);

    let (matched_referrals,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM referrals WHERE status = 'matched'")
            .fetch_one(pool.as_ref())
            .await
            .unwrap();
    assert_eq!(
        matched_referrals as usize, NUM_BEDS,
        "exactly one referral should win each bed, system-wide"
    );

    let (pending_referrals,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM referrals WHERE status = 'pending'")
            .fetch_one(pool.as_ref())
            .await
            .unwrap();
    assert_eq!(pending_referrals as usize, NUM_REFERRALS - NUM_BEDS);
}
