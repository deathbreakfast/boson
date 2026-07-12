# boson-testkit Quality Gates

Sentrux structure-health signal for this crate.

## Baseline (modularity pass)

- `scan(path="boson-testkit")` → `quality_signal`: **7235**
- Workspace: **7268** (`cross_module_edges`: 53, bottleneck: modularity)
- Tests: `cargo test -p boson-testkit`
- Clippy: `cargo clippy -p boson-testkit --all-targets -- -D warnings`

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-extract
cargo test -p boson-testkit
cargo clippy -p boson-testkit --all-targets -- -D warnings
cargo doc -p boson-testkit --no-deps
```

## Targets

- Preserve or improve Sentrux `quality_signal`
- CI default: `backend=mem`, `topology=isolated-lab`, `telemetry=off`
- Layering: depends on `boson-runtime`, `boson-backend-mem`, `boson-core` only — must not depend on `boson-e2e` / `boson-bench`
