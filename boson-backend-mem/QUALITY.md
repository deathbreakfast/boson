# boson-backend-mem Quality Gates

Sentrux structure-health signal for this crate.

## Baseline (Phase 2)

- `scan(path="boson-backend-mem")` → `quality_signal`: **7219**
- Tests: `cargo test -p boson-backend-mem`
- Clippy: `cargo clippy -p boson-backend-mem --all-targets -- -D warnings`

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-extract
cargo test -p boson-backend-mem
cargo clippy -p boson-backend-mem --all-targets -- -D warnings
cargo doc -p boson-backend-mem --no-deps
```

## Targets

- Preserve or improve Sentrux `quality_signal`
- Zero circular dependencies; no file > 450 LOC
- `#![deny(missing_docs)]` on public API
- Layering: depends on `boson-core` only
