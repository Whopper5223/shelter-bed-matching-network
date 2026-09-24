mod support;

use common::model::{ReferralNeeds, UnitType};
use matching_engine::matcher::{self, SubmitOutcome};

const REGION: &str = "test-region-priority";

fn general_needs() -> ReferralNeeds {
    ReferralNeeds {
        unit_type_required: UnitType::General,
        needs_pet_friendly: false,
        needs_sobriety_free: false,
        needs_ada_accessible: false,
    }
}

/// HUD Coordinated Entry requires prioritizing by vulnerability, not
/// arrival order. This proves it: a low-vulnerability referral arrives
/// first and waits, a high-vulnerability referral arrives second, and when
/// a bed frees up the high-vulnerability referral wins it -- even though it
/// asked second.
// All DB-touching integration tests share one real Postgres instance with
// no per-test isolation beyond a region name -- file_serial (not plain
// serial, since each tests/*.rs file is a separate binary/process) makes
// sure they never actually run concurrently against it.
#[tokio::test]
#[serial_test::file_serial(shelterbed_db)]
async fn higher_vulnerability_referral_wins_over_one_that_arrived_first() {
    let pool = support::setup_pool().await;
    support::reset(&pool).await;

    let shelter_id = support::insert_shelter(&pool, REGION).await;
    let bed_id = support::insert_bed(&pool, shelter_id, "general", false, false, false).await;

    // Take the only bed so both referrals below start out pending.
    let (blocker, _) = matcher::submit_referral(&pool, "blocker", REGION, general_needs(), 1, 1)
        .await
        .unwrap();
    assert!(matches!(blocker, SubmitOutcome::Matched(_)));

    let (low, _) = matcher::submit_referral(&pool, "low-vuln", REGION, general_needs(), 10, 1)
        .await
        .unwrap();
    let low_id = match low {
        SubmitOutcome::Pending { referral_id } => referral_id,
        other => panic!("expected Pending, got {other:?}"),
    };

    let (high, _) = matcher::submit_referral(&pool, "high-vuln", REGION, general_needs(), 90, 1)
        .await
        .unwrap();
    let high_id = match high {
        SubmitOutcome::Pending { referral_id } => referral_id,
        other => panic!("expected Pending, got {other:?}"),
    };

    // Free the bed directly (equivalent to a Kafka check-out event having
    // already been applied, including the reservation-lifecycle bookkeeping
    // that apply_bed_status_event does) and run the same allocation routine
    // the consumer runs.
    sqlx::query("UPDATE beds SET status = 'available' WHERE id = $1")
        .bind(bed_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE reservations SET status = 'released' WHERE bed_id = $1 AND status = 'active'",
    )
    .bind(bed_id)
    .execute(&pool)
    .await
    .unwrap();

    let result = matcher::allocate_for_bed(&pool, bed_id)
        .await
        .unwrap()
        .expect("an eligible pending referral is waiting");

    assert_eq!(
        result.referral_id, high_id,
        "the higher-vulnerability referral must win, regardless of arrival order"
    );

    let (low_status,): (String,) = sqlx::query_as("SELECT status FROM referrals WHERE id = $1")
        .bind(low_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        low_status, "pending",
        "the lower-vulnerability referral must still be waiting"
    );

    let (bed_status,): (String,) = sqlx::query_as("SELECT status FROM beds WHERE id = $1")
        .bind(bed_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(bed_status, "reserved");
}
