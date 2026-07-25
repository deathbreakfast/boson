# boson-axum Quality Gates

Cargo quality gates for this crate (`fmt` / `clippy` / `test` / `doc`).

## Baseline (Phase 3)

- `scan(path="boson-axum")` → `cargo` fmt/clippy/test/doc gates
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

- Preserve or improve quality review `cargo` fmt/clippy/test/doc gates
- Handlers split per resource; no god files
- Layering: depends on `boson-runtime`, `boson-core`, `axum` only
