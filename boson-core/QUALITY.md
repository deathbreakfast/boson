# boson-core Quality Gates

Cargo quality gates for this crate (`fmt` / `clippy` / `test` / `doc`).

## Baseline (modularity pass)

- `scan(path="boson-core")` → `cargo` fmt/clippy/test/doc gates
- Workspace: **7268** (`cross_module_edges`: 53)
- Tests: `cargo test -p boson-core`
- Clippy: `cargo clippy -p boson-core --all-targets -- -D warnings`

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=target-boson-extract
cargo test -p boson-core
cargo clippy -p boson-core --all-targets -- -D warnings
cargo doc -p boson-core --no-deps
```

## Targets

- Preserve or improve quality review `cargo` fmt/clippy/test/doc gates
- Zero circular dependencies; no file > 450 LOC
- `#![deny(missing_docs)]` via workspace `[lints]` inheritance
