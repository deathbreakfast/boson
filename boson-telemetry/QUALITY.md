# boson-telemetry Quality Gates

Cargo quality gates for this crate (`fmt` / `clippy` / `test` / `doc`).

## Baseline (Phase 1)

- `scan(path="boson-telemetry")` → `cargo` fmt/clippy/test/doc gates
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

- Preserve or improve quality review `cargo` fmt/clippy/test/doc gates
- Keep crate free of product persistence deps
