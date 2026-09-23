# Coordinated Entry / Shelter-Bed Matching Network

A real-time system that matches homelessness caseworkers to available
shelter beds across a region, in place of what most Continuums of Care
actually run today: phone trees and spreadsheets.

The hard part isn't the CRUD — it's that two caseworkers can try to book the
same bed at the same instant, and getting that wrong means someone gets
turned away from a shelter that's actually full. HUD's Coordinated Entry
policy also requires prioritizing by vulnerability, not first-come-first-served,
which means "who gets the bed" has to be decided correctly under concurrent
access, not just safely.

## Architecture

```
                 ┌─────────────────────┐
 shelter intake  │  intake-ingestion    │   unary gRPC: SubmitBedStatus
 terminals  ───► │  (scales indep.,     │──────────────┐
 (N simulated)   │   bursty traffic)    │              │
                 └─────────────────────┘              ▼
                                                 ┌────────────┐   keyed by
                                                 │   Kafka    │   shelter_id
                                                 │ bed-events │
                                                 └─────┬──────┘
                                                       │ consume
                                                       ▼
 caseworkers ──► ┌─────────────────────┐   internal   ┌──────────────────┐
  (referrals,    │ caseworker-gateway  │◄────gRPC────►│ matching-engine  │
   live stream)  │ (public-facing,     │  unary +     │ (allocation +    │
                 │  thin proxy)        │  streaming   │  reservation)    │
                 └─────────────────────┘              └────────┬─────────┘
                                                                │ SELECT ... FOR UPDATE
                                                                ▼
                                                          ┌───────────┐
                                                          │ Postgres  │
                                                          │  (source  │
                                                          │  of truth)│
                                                          └───────────┘
```

- **matching-engine** (Rust) — the only service that talks to Postgres.
  Consumes bed-status events from Kafka, and runs the allocation routine
  that decides which referral gets which bed.
- **intake-ingestion** — one gRPC unary endpoint (`SubmitBedStatus`) per
  shelter intake terminal. Publishes to Kafka; never touches Postgres.
  Deployed as its own service specifically so it can scale independently —
  it's the bursty part of the system.
- **caseworker-gateway** — the public-facing service. A thin proxy in front
  of matching-engine's internal `MatchingService`, so the internal engine
  can be scaled/secured separately from what caseworkers talk to.
- **Postgres** — beds, shelters, referrals, reservations. Source of truth.
- **Kafka** — `bed-events` topic, keyed by `shelter_id` so a shelter's
  events stay ordered within a partition.
- **gRPC** (tonic) — `CaseworkerService.SubmitReferral` (unary) and
  `StreamAvailability` (server streaming) on the gateway;
  `IntakeService.SubmitBedStatus` (unary) on ingestion.

Proto contract: [`proto/shelterbed.proto`](proto/shelterbed.proto). Schema:
[`migrations/0001_init.sql`](migrations/0001_init.sql).

## The concurrency + priority guarantee

This is the part worth walking through in an interview.

**The wrong design** would be: a referral comes in, immediately grab any
eligible available bed. That's first-come-first-served — exactly what HUD's
vulnerability-based prioritization forbids.

**What this does instead** ([`matcher.rs`](crates/matching-engine/src/matcher.rs)):

1. A submitted referral is always inserted as `pending` first. It is never
   matched directly by the request that submitted it.
2. A single routine, `allocate_for_bed(bed_id)`, is the *only* code path
   that ever creates a reservation. It:
   - Locks the bed row: `SELECT ... FROM beds WHERE id = $1 FOR UPDATE`.
   - If the bed isn't `available` anymore, stops — someone else got there
     first.
   - Otherwise selects the single highest-`vulnerability_score` eligible
     pending referral for that bed's exact attributes, with
     `ORDER BY vulnerability_score DESC, created_at ASC ... FOR UPDATE SKIP LOCKED LIMIT 1`.
   - Inserts the reservation, flips the bed to `reserved`, flips the
     referral to `matched` — all in one transaction.
3. `allocate_for_bed` runs from **two** triggers: a new referral arriving
   (it searches for a bed it might win), and a bed becoming available (a
   Kafka check-out event, or a reservation being released). Either way, the
   same routine decides who wins — so a referral submitted a second ago can
   never jump ahead of one with a higher vulnerability score that's already
   waiting, regardless of which request happens to trigger the allocation.

**Why no double-booking:** `FOR UPDATE` on the bed row means two concurrent
attempts to fill the *same* bed serialize — only one can proceed past the
lock. `FOR UPDATE SKIP LOCKED LIMIT 1` on the referral side means the query
locks exactly one row (not the whole pending queue), so concurrent
allocations for *different* beds never block each other. As a database-level
backstop independent of any application logic, partial unique indexes make
it physically impossible to store two active reservations for the same bed
or the same referral:

```sql
CREATE UNIQUE INDEX reservations_bed_active_uq ON reservations(bed_id) WHERE status = 'active';
CREATE UNIQUE INDEX reservations_referral_active_uq ON reservations(referral_id) WHERE status = 'active';
```

**Proved, not asserted** — two integration tests run against real Postgres:

- [`tests/concurrency.rs`](crates/matching-engine/tests/concurrency.rs): 50
  referrals submitted concurrently for 10 beds. Asserts exactly 10 active
  reservations, 10 distinct beds, 10 matched referrals system-wide, *and*
  that every individual call's own return value agrees with that (see
  `resolve_final_outcome` in `matcher.rs`: a referral can be matched by a
  *different* concurrent call's allocation, so `submit_referral` re-checks
  its own referral's true state before ever reporting "pending").
- [`tests/priority.rs`](crates/matching-engine/tests/priority.rs): a
  low-vulnerability referral arrives first and a high-vulnerability one
  arrives second; when a bed frees up, the high-vulnerability referral wins
  it — arrival order doesn't matter, vulnerability does.
- [`tests/reservation_lifecycle.rs`](crates/matching-engine/tests/reservation_lifecycle.rs):
  a no-show or a maintenance hold can send a bed back to `available` while
  it still holds an active reservation (the client never checked in).
  Asserts that reservation is released and its referral re-pended, rather
  than the next allocation attempt on that bed failing against the
  `reservations_bed_active_uq` unique index.

Run them yourself:

```sh
docker compose up -d postgres
cargo test --workspace
```

## Exactly-once-ish Kafka consumption

- Events are keyed by `shelter_id`, so per-shelter ordering is preserved
  within a partition.
- Apply is idempotent two ways: an `applied_events` ledger dedupes exact
  redelivery by `event_id`, and a per-bed monotonic `sequence` rejects
  anything at or below what's already been applied (out-of-order delivery).
- The Kafka offset is committed only *after* the Postgres transaction that
  applied the event has committed. A failing event is **retried in place
  with backoff, not skipped**: a Kafka partition's committed offset is a
  single high-water mark, not a sparse set of acked messages, so skipping
  one message and later committing a *later* one would silently move the
  committed position past the one that failed — it would never actually be
  redelivered. Blocking the partition to retry is the correct trade for
  strict per-shelter ordering; it does mean one stuck event blocks that
  shelter's (and, since one task here serves every assigned partition,
  every other shelter's) further events until it resolves. A production
  version would likely give each partition its own consumer task, or add a
  dead-letter topic, to avoid that head-of-line blocking.
- Every event -- including a `Duplicate` or `Stale` one -- still triggers an
  allocation attempt on its bed. That's a cheap no-op if the bed isn't
  actually `available`, and it closes a real crash-recovery gap: without it,
  a crash between `apply_bed_status_event`'s commit and the allocation call
  (or between that call and the offset commit) could leave a bed sitting
  `available` forever with no referral ever considered for it again.

See [`kafka_consumer.rs`](crates/matching-engine/src/kafka_consumer.rs).

## Running it locally

```sh
docker compose up --build
```

This starts Postgres, Kafka (KRaft, single node), matching-engine,
intake-ingestion, and caseworker-gateway (each gated on the previous one's
health check, so the simulator can't start seeding before matching-engine
has finished its startup migration), then runs the `simulator` once: it
seeds 12 simulated shelters (96 beds), plays each bed's intake terminal
(random check-in/check-out events over gRPC), plays a stream of caseworkers
submitting referrals with randomized eligibility and vulnerability scores,
and subscribes to the live availability stream to measure real end-to-end
latency. Latency is computed in the simulator itself, as `(local clock at
receipt) - event_timestamp_ms`, so it captures the whole path -- intake
terminal → Kafka → matching engine → gateway proxy → this subscriber -- not
just the server-side leg up to matching-engine's broadcast send.

Measured with a genuinely cold `docker compose down -v && docker compose up
--build`, 12 shelters / 96 beds, 30s of bed events + 30s of referrals:

```
seeded simulated shelters and beds num_shelters=12 num_beds=96
referral simulation complete submitted=38 matched=23
end-to-end availability update latency count=1830 avg_ms=407.6 p50_ms=10 p99_ms=4723 max_ms=5089
```

p50 is sub-second by roughly two orders of magnitude. The p99/max are a
one-time cold-start artifact, not steady state: Kafka doesn't create the
`bed-events` topic until the first message is produced, so the first
handful of events pay a topic-creation delay while matching-engine's
consumer retries `UnknownTopicOrPartition` (visible in its logs). A second
run against the now-warm cluster (`docker compose run --rm simulator`)
keeps essentially every update in single-digit milliseconds: `p50_ms=10
p99_ms=23 max_ms=117`.

## Deploying to Kubernetes

Manifests are under [`k8s/`](k8s/): a `shelterbed` namespace, dev-only
single-pod Postgres and Kafka (a real deployment points at managed
equivalents instead), Deployments + Services for the three application
services, an HPA on `intake-ingestion` specifically — it's the one service
the architecture calls bursty — and the simulator as a one-shot `Job`.

```sh
kind create cluster --name shelterbed
docker compose build
kind load docker-image resproj-matching-engine:latest --name shelterbed
kind load docker-image resproj-intake-ingestion:latest --name shelterbed
kind load docker-image resproj-caseworker-gateway:latest --name shelterbed
kind load docker-image resproj-simulator:latest --name shelterbed

kubectl apply -f k8s/
kubectl -n shelterbed get pods -w
kubectl -n shelterbed logs job/simulator -f
```

The HPA needs `metrics-server` installed in the cluster to actually act on
CPU utilization; the manifest applies cleanly either way.

Unlike docker-compose's staggered `depends_on`, a single `kubectl apply -f
k8s/` starts every pod at once -- there's no ordering guarantee at all. Two
real races follow from that, and both are handled explicitly rather than
worked around by luck:

- The simulator seeds fixture data directly via its own DB connection
  without running migrations itself, so it can reach Postgres before
  matching-engine's startup migration has created the schema
  (`relation "shelters" does not exist`). The simulator Job's pod has an
  `initContainer` that blocks on `matching-engine`'s port specifically --
  which only starts listening after its migration succeeds (see
  `crates/matching-engine/src/lib.rs`) -- so this can't happen.
- The simulator's very first gRPC connection can race ahead of
  caseworker-gateway accepting connections. Its stream listener retries
  with backoff instead of giving up on the first failure (see
  [`listener.rs`](crates/simulator/src/listener.rs)).

Verified on a freshly created local `kind` cluster (`kind create cluster` →
load the 4 images → one `kubectl apply -f k8s/`, nothing pre-warmed):

```
NAME                                  READY   STATUS      RESTARTS   AGE
caseworker-gateway-648cdc4558-cnh7l   1/1     Running     0          82s
intake-ingestion-5b6f9ccc5c-nccd6     1/1     Running     0          82s
kafka-7dfb4fc48-cvvgq                 1/1     Running     0          82s
matching-engine-85b9b54bc-jqhfx       1/1     Running     0          82s
postgres-5954fd6dc5-f5mfh             1/1     Running     0          82s
simulator-gq76m                       0/1     Completed   0          82s

# simulator pod's initContainer log -- the migration-race guard actually firing:
waiting for matching-engine
waiting for matching-engine

# simulator job logs:
seeded simulated shelters and beds num_shelters=12 num_beds=96
referral simulation complete submitted=34 matched=21
end-to-end availability update latency count=1838 avg_ms=430.4 p50_ms=11 p99_ms=4752 max_ms=23276
```

And the actual database state after that run, confirming no double-booking
under a real cold-cluster race, not just in the unit-test harness:

```
active_reservations = 17,  distinct_beds = 17,  beds.status='reserved' = 17
```

## Project layout

```
proto/shelterbed.proto        the one shared gRPC/message contract
migrations/0001_init.sql      schema + the concurrency-guarantee indexes
crates/common                 generated proto code, domain types, DB pool
crates/matching-engine        allocation logic, Kafka consumer, gRPC server
crates/intake-ingestion       unary gRPC -> Kafka producer
crates/caseworker-gateway     public gRPC proxy -> matching-engine
crates/simulator              N simulated shelters + caseworkers + latency measurement
docker/Dockerfile             one multi-stage build, one runtime target per service
k8s/                          namespace, config, dev-only Postgres/Kafka, app Deployments, HPA, Job
```
