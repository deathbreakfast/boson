# boson Quality Gates

Sentrux structure-health signal for this crate.

## Baseline (Phase 3)

- `scan(path="boson")` → `quality_signal`: **9574**
- Check: `cargo check -p uf-boson --features mem,telemetry-console,axum`

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-extract
cargo check -p uf-boson --features mem,telemetry-console,axum
cargo doc -p uf-boson --features mem,telemetry-console,axum --no-deps
```

## Targets

- Thin `lib.rs`; re-exports in `prelude.rs`
- Feature-forward only — no runtime logic in this crate
