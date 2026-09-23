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
  reservations, 10 distinct beds, 10 matched referrals, system-wide — a
  task's own RPC response can legitimately say "pending" even though its
  referral gets matched moments later by a *different* concurrent
  allocation, so the test checks final database state, not each task's own
  return value.
- [`tests/priority.rs`](crates/matching-engine/tests/priority.rs): a
  low-vulnerability referral arrives first and a high-vulnerability one
  arrives second; when a bed frees up, the high-vulnerability referral wins
  it — arrival order doesn't matter, vulnerability does.

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
  applied the event has committed — a crash between the two just means the
  event is redelivered and safely re-applied as a no-op.

See [`kafka_consumer.rs`](crates/matching-engine/src/kafka_consumer.rs).

## Running it locally

```sh
docker compose up --build
```

This starts Postgres, Kafka (KRaft, single node), matching-engine,
intake-ingestion, and caseworker-gateway, then runs the `simulator` once:
it seeds 12 simulated shelters (96 beds), plays each bed's intake terminal
(random check-in/check-out events over gRPC), plays a stream of caseworkers
submitting referrals with randomized eligibility and vulnerability scores,
and subscribes to the live availability stream to measure real end-to-end
latency (intake terminal → Kafka → matching engine → gateway stream →
subscriber).

Measured on a single-node local run, 12 shelters / 96 beds, 30s of bed
events + 30s of referrals (`docker compose run --rm simulator`):

First run against a cold cluster:

```
count=1797 avg_ms=419.6 p50_ms=11 p99_ms=4701 max_ms=5081
referral simulation complete: submitted=32 matched=21
```

Second run, same cluster, now warm:

```
count=1829 avg_ms=10.8 p50_ms=10 p99_ms=23 max_ms=117
referral simulation complete: submitted=35 matched=30
```

The first run's p99/max are a one-time cold-start artifact, not steady
state: Kafka doesn't create the `bed-events` topic until the first message
is produced, so the very first handful of events pay a topic-creation delay
while matching-engine's consumer retries `UnknownTopicOrPartition`. Once the
topic exists, end-to-end latency (intake terminal → Kafka → matching engine
→ gateway stream → subscriber) sits at p50 10ms / p99 23ms / max 117ms —
sub-second by roughly two orders of magnitude.

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

Verified on a real local `kind` cluster (all 6 pods starting cold at the
same instant via a single `kubectl apply -f k8s/`):

```
NAME                                  READY   STATUS      RESTARTS   AGE
caseworker-gateway-648cdc4558-v6697   1/1     Running     0          5m32s
intake-ingestion-5b6f9ccc5c-mzhtf     1/1     Running     0          5m32s
kafka-7dfb4fc48-429gz                 1/1     Running     0          5m32s
matching-engine-85b9b54bc-r5rlf       1/1     Running     0          5m32s
postgres-5954fd6dc5-pzptn             1/1     Running     0          5m32s
simulator-wdvnf                       0/1     Completed   0          38s

# simulator job logs:
seeded simulated shelters and beds num_shelters=12 num_beds=96
referral simulation complete submitted=35 matched=33
end-to-end availability update latency count=1828 avg_ms=10.7 p50_ms=9 p99_ms=42 max_ms=150
```

(All pods starting simultaneously in Kubernetes -- unlike docker-compose's
staggered `depends_on` -- means the simulator's very first connection
attempt to caseworker-gateway can race ahead of that pod being ready. Its
gRPC stream listener retries with backoff rather than giving up on the
first failure, which is what makes this cold-start case work cleanly; see
[`listener.rs`](crates/simulator/src/listener.rs).)

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
