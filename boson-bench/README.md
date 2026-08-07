# boson-bench

Performance CLI over shared [`boson-testkit`](../boson-testkit/README.md) scenarios.

## Documentation

| Doc | Role |
|-----|------|
| [`PERFORMANCE.md`](PERFORMANCE.md) | Decision-grade findings — Redis vs NATS Tier 3 capacity |
| [`PERFORMANCE.md`](PERFORMANCE.md) | Pre-registered IDs, phase status, run commands |
| [`EXPERIMENTS-ARCHIVE.md`](EXPERIMENTS-ARCHIVE.md) | Scylla, Tier 1–2, campaign debug history |

## Role

Records throughput and latency for **BM-BE*** (enqueue capacity), **BM-BD*** (dequeue capacity), and **BM-BL*** (soak) experiments. Matrix dimension **`backend`**: `mem` for CI; Redis and NATS for Tier 3 hyperscale evaluation.

## Quick start

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-bench

cargo run -p boson-bench -- experiments
cargo run -p boson-bench -- run --experiment bm-be4 --backend redis \
  --client-count 64 --pool-count 10 --pool-layout distinct --telemetry off
```

Reports: [`profiling/boson-bench/reports/`](../profiling/boson-bench/reports/)

AWS campaigns are run out of tree by maintainers (see [`PERFORMANCE.md`](PERFORMANCE.md)).

CI smoke: [`.github/workflows/boson-matrix.yml`](../.github/workflows/boson-matrix.yml).

## Related crates

- [`boson-testkit`](../boson-testkit/README.md) — shared scenario definitions
- [`boson-e2e`](../boson-e2e/README.md) — correctness assertions on the same scenarios
