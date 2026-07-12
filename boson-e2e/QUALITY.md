# boson-e2e Quality Gates

Sentrux MCP structure-health signal for matrix correctness integration tests.

## Baseline (Phase 5)

- `scan(path="boson-e2e")` → `quality_signal`: **10000**
- Tests: `cargo test -p boson-e2e`
- CI: upstream `.github/workflows/boson-matrix.yml`

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-e2e
cargo test -p boson-e2e
cargo clippy -p boson-e2e --all-targets -- -D warnings
```

## Targets

- Default CI slice passes without `--ignored`
- Extended rows remain `#[ignore]` for manual/nightly runs
