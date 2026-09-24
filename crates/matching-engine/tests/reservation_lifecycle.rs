mod support;

use common::model::{BedStatus, ReferralNeeds, UnitType};
use matching_engine::matcher::{self, ApplyOutcome, SubmitOutcome};
use uuid::Uuid;

const REGION: &str = "test-region-lifecycle";

fn general_needs() -> ReferralNeeds {
    ReferralNeeds {
        unit_type_required: UnitType::General,
        needs_pet_friendly: false,
        needs_sobriety_free: false,
        needs_ada_accessible: false,
    }
}

/// A bed can go back to `available` while it still holds an active (not yet
/// `checked_in`) reservation: a no-show, or a cancellation before check-in.
/// `apply_bed_status_event` has to release that reservation and re-pend its
/// referral, or the very next allocation attempt on this bed hits the
/// `reservations_bed_active_uq` partial unique index as a real error instead
/// of proceeding.
// See tests/priority.rs for why file_serial (cross-process) rather than
// plain serial (in-process only) is needed here.
#[tokio::test]
#[serial_test::file_serial(shelterbed_db)]
async fn a_no_show_releases_the_reservation_and_re_pends_the_referral() {
    let pool = support::setup_pool().await;
    support::reset(&pool).await;

    let shelter_id = support::insert_shelter(&pool, REGION).await;
    let bed_id = support::insert_bed(&pool, shelter_id, "general", false, false, false).await;

    let (outcome, _) =
        matcher::submit_referral(&pool, "caseworker-1", REGION, general_needs(), 50, 1)
            .await
            .unwrap();
    let referral_id = match outcome {
        SubmitOutcome::Matched(result) => result.referral_id,
        other => panic!("expected Matched, got {other:?}"),
    };

    // The client never checks in -- the terminal reports the bed available
    // again directly from `reserved`, skipping `occupied` entirely.
    let apply_outcome =
        matcher::apply_bed_status_event(&pool, Uuid::new_v4(), bed_id, BedStatus::Available, 1)
            .await
            .expect("apply_bed_status_event must not error on a no-show");

    assert!(matches!(
        apply_outcome,
        ApplyOutcome::Applied {
            became_available: true,
            ..
        }
    ));

    let (referral_status,): (String,) =
        sqlx::query_as("SELECT status FROM referrals WHERE id = $1")
            .bind(referral_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        referral_status, "pending",
        "the displaced referral must go back to pending, not stay matched to a bed it never occupied"
    );

    let (released_reservations,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM reservations WHERE bed_id = $1 AND status = 'released'",
    )
    .bind(bed_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(released_reservations, 1);

    // The real regression: this must succeed, not fail with a unique
    // constraint violation on reservations_bed_active_uq.
    let result = matcher::allocate_for_bed(&pool, bed_id)
        .await
        .expect("re-allocating a freed bed must not hit the active-reservation unique index")
        .expect("the re-pended referral is eligible and should win the now-available bed");

    assert_eq!(result.referral_id, referral_id);
    assert_eq!(result.bed_id, bed_id);
}

/// The same release-and-re-pend behavior applies when a bed goes into
/// `maintenance` while reserved -- the referral shouldn't stay artificially
/// `matched` to a bed that just went offline for an unknown duration.
#[tokio::test]
#[serial_test::file_serial(shelterbed_db)]
async fn a_maintenance_hold_releases_the_reservation_and_re_pends_the_referral() {
    let pool = support::setup_pool().await;
    support::reset(&pool).await;

    let shelter_id = support::insert_shelter(&pool, REGION).await;
    let bed_id = support::insert_bed(&pool, shelter_id, "general", false, false, false).await;

    let (outcome, _) =
        matcher::submit_referral(&pool, "caseworker-1", REGION, general_needs(), 50, 1)
            .await
            .unwrap();
    let referral_id = match outcome {
        SubmitOutcome::Matched(result) => result.referral_id,
        other => panic!("expected Matched, got {other:?}"),
    };

    matcher::apply_bed_status_event(&pool, Uuid::new_v4(), bed_id, BedStatus::Maintenance, 1)
        .await
        .expect("apply_bed_status_event must not error on a maintenance hold");

    let (referral_status,): (String,) =
        sqlx::query_as("SELECT status FROM referrals WHERE id = $1")
            .bind(referral_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(referral_status, "pending");

    // Bring it back available and confirm allocation works cleanly, same as
    // the no-show case.
    matcher::apply_bed_status_event(&pool, Uuid::new_v4(), bed_id, BedStatus::Available, 2)
        .await
        .unwrap();

    let result = matcher::allocate_for_bed(&pool, bed_id)
        .await
        .expect("no unique-index error after a maintenance hold clears")
        .expect("the re-pended referral should win the bed");
    assert_eq!(result.referral_id, referral_id);
}
