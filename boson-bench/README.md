# boson-bench

Performance CLI over shared [`boson-testkit`](../boson-testkit/README.md) scenarios.

## Documentation

| Doc | Role |
|-----|------|
| [`PERFORMANCE.md`](PERFORMANCE.md) | Decision-grade findings — Redis vs NATS Tier 3 capacity |
| [`PERFORMANCE.md`](PERFORMANCE.md) | Pre-registered IDs, phase status, run commands |
| [`EXPERIMENTS-ARCHIVE.md`](EXPERIMENTS-ARCHIVE.md) | Scylla, Tier 1–2, campaign debug history |

## Role

Records throughput and latency for three tracks: **BM-BE*** enqueue capacity (workers off), **BM-BD*** dequeue capacity (prefill then drain), and **BM-BC1** completed durable tasks/s (leases on, run rows persisted, mixed sleep/retry handlers). **BM-BL*** remains the paced soak tier. Matrix dimension **`backend`**: `mem` for CI; Redis and NATS for Tier 3.

## Quick start

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-bench

cargo run -p boson-bench -- experiments
cargo run -p boson-bench -- run --experiment bm-be4 --backend redis \
  --client-count 64 --pool-count 10 --pool-layout distinct --telemetry off

# Completed-task track (BM-BC1). AWS default is 60s / W=32 / Redis.
# Local mem smoke: shorten the window with BOSON_BENCH_BC1_DURATION_SECS.
cargo run -p boson-bench --release -- run --experiment bm-bc1 --backend redis

# Embedded SQLite completed-task smoke (same ID; tagged --report for the campaign cell)
BOSON_BENCH_BC1_DURATION_SECS=2 BOSON_BENCH_WORKER_COUNT=2 cargo run -p boson-bench -- \
  run --experiment bm-bc1 --backend sqlite --topology isolated-lab --telemetry off
```

Reports: [`profiling/boson-bench/reports/`](../profiling/boson-bench/reports/)

AWS campaigns are run out of tree by maintainers (see [`PERFORMANCE.md`](PERFORMANCE.md)).

CI smoke: [`.github/workflows/boson-matrix.yml`](../.github/workflows/boson-matrix.yml).

## Related crates

- [`boson-testkit`](../boson-testkit/README.md) — shared scenario definitions
- [`boson-e2e`](../boson-e2e/README.md) — correctness assertions on the same scenarios
