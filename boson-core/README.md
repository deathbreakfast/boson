# boson-core

Shared types, [`QueueBackend`](src/backend/queue_backend.rs) trait, router, errors, and identity hooks.

Task authors usually start at [`boson`](https://docs.rs/uf-boson). This crate holds shared DTOs and the
persistence trait for custom backends.

## Role

- `Job`, `Run`, `TaskConfig`, status enums — shared data
- **`ExecutionContext` / `ExecutionContextFactory`** — handler identity (factory installed at boot)
- **`QueueBackend`** — stable async trait for queue persistence
- **`QueueRouter`** — register named backends at host boot (see [`boson`](https://docs.rs/uf-boson) Getting started)

Third-party crates implement **`QueueBackend`** only against DTOs exported here. See
[`QueueBackend`](src/backend/queue_backend.rs) rustdoc (**How to implement**, method groups, skeleton).

## Identity

Task handlers take `Box<dyn ExecutionContext>` as the first argument. Factory choice and boot
wiring live in [`boson-runtime`](https://docs.rs/boson-runtime) and [`src/identity.rs`](https://docs.rs/boson-core/latest/boson_core/identity/index.html).

## Related crates

- [`boson`](https://docs.rs/uf-boson) — main crate (re-exports `task`, `JsonExecutionContextFactory`)
- [`boson-macros`](https://docs.rs/boson-macros) — `#[boson::task]` attribute macro
- [`boson-runtime`](https://docs.rs/boson-runtime) — worker runtime built on this trait
- [Root README](https://github.com/unified-field-dev/boson/blob/main/README.md) — overview
