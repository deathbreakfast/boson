# boson-runtime Quality Gates

Sentrux structure-health signal for this crate.

## Baseline (Phase 3)

- `scan` on `boson-runtime` → `quality_signal`: **7278**
- Tests: `cargo test -p boson-runtime`
- Clippy: `cargo clippy -p boson-runtime --all-targets -- -D warnings`

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-extract
cargo test -p boson-runtime
cargo clippy -p boson-runtime --all-targets -- -D warnings
cargo doc -p boson-runtime --no-deps
```

## Targets

- Preserve or improve Sentrux `quality_signal`
- Zero circular dependencies; no file > 450 LOC; max CC ≤ 25 per function
- `#![deny(missing_docs)]` on public API
- Layering: depends on `boson-core`, `boson-telemetry`, `quark` only
