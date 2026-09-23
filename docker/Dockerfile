# Single builder stage compiles the whole workspace once; each service gets
# its own minimal runtime stage copying out just its binary.

FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    cmake \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY migrations ./migrations
COPY crates ./crates

RUN cargo build --release --workspace

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

FROM runtime-base AS matching-engine
COPY --from=builder /app/target/release/matching-engine /usr/local/bin/matching-engine
ENTRYPOINT ["/usr/local/bin/matching-engine"]

FROM runtime-base AS intake-ingestion
COPY --from=builder /app/target/release/intake-ingestion /usr/local/bin/intake-ingestion
ENTRYPOINT ["/usr/local/bin/intake-ingestion"]

FROM runtime-base AS caseworker-gateway
COPY --from=builder /app/target/release/caseworker-gateway /usr/local/bin/caseworker-gateway
ENTRYPOINT ["/usr/local/bin/caseworker-gateway"]

FROM runtime-base AS simulator
COPY --from=builder /app/target/release/simulator /usr/local/bin/simulator
ENTRYPOINT ["/usr/local/bin/simulator"]
