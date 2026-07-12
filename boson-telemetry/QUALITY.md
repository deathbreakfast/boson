# boson-telemetry Quality Gates

Sentrux structure-health signal for this crate.

## Baseline (Phase 1)

- `scan(path="boson-telemetry")` → `quality_signal`: **7805**
- Tests: `cargo test -p boson-telemetry`
- Clippy: `cargo clippy -p boson-telemetry --all-targets -- -D warnings`

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=target-boson-extract
cargo test -p boson-telemetry
cargo clippy -p boson-telemetry --all-targets -- -D warnings
cargo doc -p boson-telemetry --no-deps
```

## Targets

- Preserve or improve Sentrux `quality_signal`
- Keep crate free of product persistence deps
