# boson-backend-mem Quality Gates

Cargo quality gates for this crate (`fmt` / `clippy` / `test` / `doc`).

## Baseline (Phase 2)

- `scan(path="boson-backend-mem")` → `cargo` fmt/clippy/test/doc gates
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

- Preserve or improve quality review `cargo` fmt/clippy/test/doc gates
- Zero circular dependencies; no file > 450 LOC
- `#![deny(missing_docs)]` on public API
- Layering: depends on `boson-core` only
