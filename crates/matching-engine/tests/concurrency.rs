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
#[tokio::test]
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

    // NOTE on what we assert here and why: a task's own `submit_referral`
    // call can legitimately come back `Pending` even though its referral
    // gets matched moments later by a *different* task's concurrent
    // allocation attempt (allocate_for_bed always awards a freed-up bed to
    // whichever pending referral is globally top-priority, not necessarily
    // the one that triggered the check). That is correct, intended
    // behavior, not a bug -- so we don't assert on each call's own return
    // value. What must hold is the actual database state once every task
    // has finished, which is what the checks below verify.
    let mut locally_matched = 0usize;
    let mut locally_pending = 0usize;
    for handle in handles {
        match handle.await.expect("task panicked") {
            SubmitOutcome::Matched(_) => locally_matched += 1,
            SubmitOutcome::Pending { .. } => locally_pending += 1,
        }
    }
    assert_eq!(locally_matched + locally_pending, NUM_REFERRALS);
    assert!(locally_matched <= NUM_BEDS);

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
