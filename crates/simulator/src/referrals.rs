use std::collections::HashSet;
use std::time::Duration;

use common::proto::{self, caseworker_service_client::CaseworkerServiceClient};
use rand::random_range;
use tonic::transport::Channel;

use crate::seed::SeededBed;

const UNIT_TYPES: [proto::UnitType; 5] = [
    proto::UnitType::General,
    proto::UnitType::Family,
    proto::UnitType::WomensOnly,
    proto::UnitType::MensOnly,
    proto::UnitType::Youth,
];

/// Plays the role of caseworkers submitting referrals through the public
/// gateway, at random intervals, with randomized eligibility criteria and
/// vulnerability scores across the seeded regions.
pub async fn run(
    mut client: CaseworkerServiceClient<Channel>,
    beds: Vec<SeededBed>,
    duration_secs: u64,
) {
    let regions: Vec<String> = beds
        .iter()
        .map(|b| b.region.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if regions.is_empty() {
        return;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(duration_secs);
    let mut submitted = 0u64;
    let mut matched = 0u64;

    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(random_range(200..1500))).await;

        submitted += 1;
        let region = regions[random_range(0..regions.len())].clone();
        let unit_type = UNIT_TYPES[random_range(0..UNIT_TYPES.len())];

        let criteria = proto::ReferralCriteria {
            family_size: random_range(1..6),
            unit_type_required: unit_type as i32,
            needs_pet_friendly: random_range(0..100) < 15,
            needs_sobriety_free_environment: random_range(0..100) < 30,
            needs_ada_accessible: random_range(0..100) < 10,
            vulnerability_score: random_range(0..100),
        };

        let request = proto::SubmitReferralRequest {
            caseworker_id: format!("simulated-caseworker-{submitted}"),
            region,
            criteria: Some(criteria),
        };

        match client.submit_referral(request).await {
            Ok(response) => {
                let response = response.into_inner();
                if response.status == proto::ReferralStatus::Matched as i32 {
                    matched += 1;
                }
            }
            Err(err) => tracing::warn!(%err, "referral submission failed"),
        }
    }

    tracing::info!(submitted, matched, "referral simulation complete");
}
