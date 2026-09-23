use crate::error::AppError;
use crate::proto;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitType {
    General,
    Family,
    WomensOnly,
    MensOnly,
    Youth,
}

impl UnitType {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            UnitType::General => "general",
            UnitType::Family => "family",
            UnitType::WomensOnly => "womens_only",
            UnitType::MensOnly => "mens_only",
            UnitType::Youth => "youth",
        }
    }

    pub fn from_db_str(s: &str) -> Result<Self, AppError> {
        Ok(match s {
            "general" => UnitType::General,
            "family" => UnitType::Family,
            "womens_only" => UnitType::WomensOnly,
            "mens_only" => UnitType::MensOnly,
            "youth" => UnitType::Youth,
            other => return Err(AppError::InvalidEnumValue(format!("unit_type={other}"))),
        })
    }
}

impl From<proto::UnitType> for UnitType {
    fn from(v: proto::UnitType) -> Self {
        match v {
            proto::UnitType::Family => UnitType::Family,
            proto::UnitType::WomensOnly => UnitType::WomensOnly,
            proto::UnitType::MensOnly => UnitType::MensOnly,
            proto::UnitType::Youth => UnitType::Youth,
            proto::UnitType::General | proto::UnitType::Unspecified => UnitType::General,
        }
    }
}

impl From<UnitType> for proto::UnitType {
    fn from(v: UnitType) -> Self {
        match v {
            UnitType::General => proto::UnitType::General,
            UnitType::Family => proto::UnitType::Family,
            UnitType::WomensOnly => proto::UnitType::WomensOnly,
            UnitType::MensOnly => proto::UnitType::MensOnly,
            UnitType::Youth => proto::UnitType::Youth,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BedStatus {
    Available,
    Reserved,
    Occupied,
    Maintenance,
}

impl BedStatus {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            BedStatus::Available => "available",
            BedStatus::Reserved => "reserved",
            BedStatus::Occupied => "occupied",
            BedStatus::Maintenance => "maintenance",
        }
    }

    pub fn from_db_str(s: &str) -> Result<Self, AppError> {
        Ok(match s {
            "available" => BedStatus::Available,
            "reserved" => BedStatus::Reserved,
            "occupied" => BedStatus::Occupied,
            "maintenance" => BedStatus::Maintenance,
            other => return Err(AppError::InvalidEnumValue(format!("bed_status={other}"))),
        })
    }
}

impl From<proto::BedStatus> for BedStatus {
    fn from(v: proto::BedStatus) -> Self {
        match v {
            proto::BedStatus::Reserved => BedStatus::Reserved,
            proto::BedStatus::Occupied => BedStatus::Occupied,
            proto::BedStatus::Maintenance => BedStatus::Maintenance,
            proto::BedStatus::Available | proto::BedStatus::Unspecified => BedStatus::Available,
        }
    }
}

impl From<BedStatus> for proto::BedStatus {
    fn from(v: BedStatus) -> Self {
        match v {
            BedStatus::Available => proto::BedStatus::Available,
            BedStatus::Reserved => proto::BedStatus::Reserved,
            BedStatus::Occupied => proto::BedStatus::Occupied,
            BedStatus::Maintenance => proto::BedStatus::Maintenance,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferralStatus {
    Pending,
    Matched,
    Expired,
    Cancelled,
}

impl ReferralStatus {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            ReferralStatus::Pending => "pending",
            ReferralStatus::Matched => "matched",
            ReferralStatus::Expired => "expired",
            ReferralStatus::Cancelled => "cancelled",
        }
    }

    pub fn from_db_str(s: &str) -> Result<Self, AppError> {
        Ok(match s {
            "pending" => ReferralStatus::Pending,
            "matched" => ReferralStatus::Matched,
            "expired" => ReferralStatus::Expired,
            "cancelled" => ReferralStatus::Cancelled,
            other => {
                return Err(AppError::InvalidEnumValue(format!(
                    "referral_status={other}"
                )))
            }
        })
    }
}

impl From<ReferralStatus> for proto::ReferralStatus {
    fn from(v: ReferralStatus) -> Self {
        match v {
            ReferralStatus::Pending => proto::ReferralStatus::Pending,
            ReferralStatus::Matched => proto::ReferralStatus::Matched,
            ReferralStatus::Expired => proto::ReferralStatus::Expired,
            ReferralStatus::Cancelled => proto::ReferralStatus::Cancelled,
        }
    }
}

/// The fixed physical/policy attributes of a bed, as stored on the `beds` row.
#[derive(Debug, Clone, Copy)]
pub struct BedAttributes {
    pub unit_type: UnitType,
    pub allows_pets: bool,
    pub sobriety_required: bool,
    pub ada_accessible: bool,
}

/// What a referral needs, taken from `ReferralCriteria`.
#[derive(Debug, Clone, Copy)]
pub struct ReferralNeeds {
    pub unit_type_required: UnitType,
    pub needs_pet_friendly: bool,
    pub needs_sobriety_free: bool,
    pub needs_ada_accessible: bool,
}

/// Pure eligibility predicate: does this bed satisfy this referral's hard
/// constraints? Vulnerability score is deliberately not an input here — it
/// decides *priority among eligible referrals*, not eligibility itself.
pub fn is_eligible(bed: &BedAttributes, referral: &ReferralNeeds) -> bool {
    if bed.unit_type != referral.unit_type_required {
        return false;
    }
    if referral.needs_pet_friendly && !bed.allows_pets {
        return false;
    }
    if referral.needs_sobriety_free && bed.sobriety_required {
        return false;
    }
    if referral.needs_ada_accessible && !bed.ada_accessible {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family_bed() -> BedAttributes {
        BedAttributes {
            unit_type: UnitType::Family,
            allows_pets: false,
            sobriety_required: false,
            ada_accessible: false,
        }
    }

    fn family_need() -> ReferralNeeds {
        ReferralNeeds {
            unit_type_required: UnitType::Family,
            needs_pet_friendly: false,
            needs_sobriety_free: false,
            needs_ada_accessible: false,
        }
    }

    #[test]
    fn matching_unit_type_with_no_extra_needs_is_eligible() {
        assert!(is_eligible(&family_bed(), &family_need()));
    }

    #[test]
    fn mismatched_unit_type_is_ineligible() {
        let mut need = family_need();
        need.unit_type_required = UnitType::WomensOnly;
        assert!(!is_eligible(&family_bed(), &need));
    }

    #[test]
    fn pet_friendly_requirement_excludes_non_pet_bed() {
        let mut need = family_need();
        need.needs_pet_friendly = true;
        assert!(!is_eligible(&family_bed(), &need));

        let mut bed = family_bed();
        bed.allows_pets = true;
        assert!(is_eligible(&bed, &need));
    }

    #[test]
    fn sobriety_free_requirement_excludes_sobriety_required_bed() {
        let mut bed = family_bed();
        bed.sobriety_required = true;
        let mut need = family_need();
        need.needs_sobriety_free = true;
        assert!(!is_eligible(&bed, &need));

        bed.sobriety_required = false;
        assert!(is_eligible(&bed, &need));
    }

    #[test]
    fn ada_requirement_excludes_inaccessible_bed() {
        let mut need = family_need();
        need.needs_ada_accessible = true;
        assert!(!is_eligible(&family_bed(), &need));

        let mut bed = family_bed();
        bed.ada_accessible = true;
        assert!(is_eligible(&bed, &need));
    }

    #[test]
    fn a_referral_with_no_extra_needs_accepts_a_stricter_bed() {
        // A bed that happens to allow pets / be ADA-accessible / not require
        // sobriety is still fine for a referral that doesn't need those
        // things -- needs are floors, not exact-match flags.
        let bed = BedAttributes {
            unit_type: UnitType::Family,
            allows_pets: true,
            sobriety_required: false,
            ada_accessible: true,
        };
        assert!(is_eligible(&bed, &family_need()));
    }
}
