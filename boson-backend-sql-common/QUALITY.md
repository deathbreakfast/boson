# boson-backend-sql-common Quality Gates

Sentrux structure-health signal for this crate.

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-sqlite
cargo test -p boson-backend-sql-common
cargo clippy -p boson-backend-sql-common --all-targets -- -D warnings
cargo doc -p boson-backend-sql-common --no-deps
```

## Targets

- Preserve or improve Sentrux `quality_signal`
- `#![deny(missing_docs)]` via workspace `[lints]` inheritance
- Layering: depends on `boson-core` and `sqlx` only
