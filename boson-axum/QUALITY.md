# boson-axum Quality Gates

Sentrux structure-health signal for this crate.

## Baseline (Phase 3)

- `scan(path="boson-axum")` → `quality_signal`: **7008**
- Tests: `cargo test -p boson-axum`
- Clippy: `cargo clippy -p boson-axum --all-targets -- -D warnings`

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-extract
cargo test -p boson-axum
cargo clippy -p boson-axum --all-targets -- -D warnings
cargo doc -p boson-axum --no-deps
```

## Targets

- Preserve or improve Sentrux `quality_signal`
- Handlers split per resource; no god files
- Layering: depends on `boson-runtime`, `boson-core`, `axum` only
