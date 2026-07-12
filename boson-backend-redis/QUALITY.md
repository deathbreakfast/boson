# boson-backend-redis Quality Gates

Sentrux structure-health signal for this crate.

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-redis
cargo test -p boson-backend-redis
cargo clippy -p boson-backend-redis --all-targets -- -D warnings
cargo doc -p boson-backend-redis --no-deps
```

## Targets

- Preserve or improve Sentrux `quality_signal`
- `#![deny(missing_docs)]` via workspace `[lints]` inheritance
- Layering: depends on `boson-core` only
