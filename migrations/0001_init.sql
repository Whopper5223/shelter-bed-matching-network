-- Source of truth for shelters, beds, referrals, and reservations.
-- Only matching-engine connects to Postgres; it owns and runs these
-- migrations on startup (see crates/matching-engine/src/main.rs).

CREATE TABLE shelters (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    region      TEXT NOT NULL
);

CREATE TABLE beds (
    id                   UUID PRIMARY KEY,
    shelter_id           UUID NOT NULL REFERENCES shelters(id),
    unit_type            TEXT NOT NULL CHECK (unit_type IN ('general','family','womens_only','mens_only','youth')),
    allows_pets          BOOLEAN NOT NULL DEFAULT FALSE,
    sobriety_required    BOOLEAN NOT NULL DEFAULT FALSE,
    ada_accessible       BOOLEAN NOT NULL DEFAULT FALSE,
    status               TEXT NOT NULL CHECK (status IN ('available','reserved','occupied','maintenance')),
    -- Per-bed monotonic sequence from the intake terminal. A Kafka event is
    -- only applied if event.sequence > beds.last_event_sequence, which makes
    -- redelivery / out-of-order delivery a no-op instead of a regression.
    last_event_sequence  BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX beds_shelter_status_idx ON beds(shelter_id, status);
CREATE INDEX beds_region_lookup_idx ON beds(status) WHERE status = 'available';

CREATE TABLE referrals (
    id                    UUID PRIMARY KEY,
    caseworker_id         TEXT NOT NULL,
    region                TEXT NOT NULL,
    family_size           INT NOT NULL,
    unit_type_required    TEXT NOT NULL CHECK (unit_type_required IN ('general','family','womens_only','mens_only','youth')),
    needs_pet_friendly    BOOLEAN NOT NULL DEFAULT FALSE,
    needs_sobriety_free   BOOLEAN NOT NULL DEFAULT FALSE,
    needs_ada_accessible  BOOLEAN NOT NULL DEFAULT FALSE,
    vulnerability_score   INT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('pending','matched','expired','cancelled')) DEFAULT 'pending',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Allocation picks the highest-vulnerability eligible pending referral for a
-- region; this index makes that scan cheap.
CREATE INDEX referrals_pending_priority_idx
    ON referrals(region, vulnerability_score DESC, created_at)
    WHERE status = 'pending';

CREATE TABLE reservations (
    id           UUID PRIMARY KEY,
    bed_id       UUID NOT NULL REFERENCES beds(id),
    referral_id  UUID NOT NULL REFERENCES referrals(id),
    reserved_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       TEXT NOT NULL CHECK (status IN ('active','checked_in','released','cancelled')) DEFAULT 'active'
);

-- Database-level backstop for the double-booking guarantee: even if the
-- application-level SELECT ... FOR UPDATE SKIP LOCKED logic had a bug, these
-- indexes make it impossible for a bed or a referral to hold two active
-- reservations at once.
CREATE UNIQUE INDEX reservations_bed_active_uq ON reservations(bed_id) WHERE status = 'active';
CREATE UNIQUE INDEX reservations_referral_active_uq ON reservations(referral_id) WHERE status = 'active';

-- Idempotent-apply ledger for Kafka bed-status events, keyed by the
-- producer-assigned event_id. Belt-and-suspenders alongside
-- beds.last_event_sequence: this catches an exact redelivery even if a bed
-- was later deleted/recreated, while the sequence check catches
-- out-of-order delivery within a bed's lifetime.
CREATE TABLE applied_events (
    event_id    UUID PRIMARY KEY,
    bed_id      UUID NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
