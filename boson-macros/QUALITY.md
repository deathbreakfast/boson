# boson-macros Quality Gates

Sentrux structure-health signal for this crate.

## Local commands

```bash
cd "$(git rev-parse --show-toplevel)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-extract
cargo test -p boson-macros
cargo clippy -p boson-macros --all-targets -- -D warnings
cargo doc -p boson-macros --no-deps
```

## Targets

- Preserve or improve Sentrux `quality_signal`
- `#![deny(missing_docs)]` via workspace `[lints]` inheritance
- Proc-macro crate: no runtime deps on boson-core
