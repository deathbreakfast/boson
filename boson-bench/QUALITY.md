# boson-bench Quality Gates

Sentrux MCP structure-health signal for the BM-B* benchmark CLI.

## Baseline

- Smoke: `cargo run -p boson-bench -- experiments`
- CI: upstream `.github/workflows/boson-matrix.yml`
- Registry: [`EXPERIMENTS.md`](EXPERIMENTS.md), [`PERFORMANCE_STUDY.md`](PERFORMANCE_STUDY.md)

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-bench
cargo run -p boson-bench -- run --experiment bm-b0 --backend mem --topology isolated-lab --telemetry off --ops 1000
cargo run -p boson-bench -- matrix --subset mem-lab --backend mem --hardware aws-t3-medium
cargo clippy -p boson-bench --all-targets -- -D warnings
```

## Targets

- BM-B0–B17, BM-BL0–BL4, BM-BP/BM* registered and mem-runnable
- JSON reports include `hardware_detail`, `metrics`, pass/fail in `profiling/boson-bench/reports/`
- `project-fleet` and `project-scaling-curve` for 10M/s decomposition
