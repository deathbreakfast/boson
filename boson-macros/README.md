# boson-macros

Proc macros for Boson background work.

Provides `#[boson::task]` for defining tasks with typed enqueue APIs.

## Creating tasks

1. Annotate an async function with `#[boson::task(name = "...")]`.
2. First parameter must be `Box<dyn ExecutionContext>`.
3. Add crate dependencies (below).
4. Enqueue with `<TaskName>::send_with(actor_json, params)` once the process has called
   `configure` (Mode 1 embedded **or** Mode 2 enqueue host).

Optional policy attributes: `priority`, `pool`, `idempotency_mode`, `max_attempts`, `base_delay_ms`,
`backoff_multiplier`, `max_delay_ms`, `max_in_flight`, `max_enqueue_per_second` — see rustdoc on
`task` for defaults.

## Project setup (once per handler crate)

- Add dependencies (below).
- If handlers live in a library crate, link that crate into the **worker** binary
  (for example `use my_worker as _;`).

## Boot once (not per task)

Worker / enqueue-host boot — `BosonBuilder`, `.auto_registry()`, identity factory, and `configure`
before `send_with` — is documented on the [`boson`](https://docs.rs/uf-boson) crate
[Getting started](https://docs.rs/uf-boson/latest/boson/index.html#getting-started) and in
[`boson-runtime`](https://docs.rs/boson-runtime). You do not repeat boot steps for each new task.

Runnable first-boot example: [`task_macro`](https://github.com/unified-field-dev/boson/blob/main/boson/examples/task_macro.rs).

## Consumer dependencies

Crates that define tasks need:

```toml
boson-macros = "0.1.1"
quark = { package = "uf-quark", version = "0.1.1" }
boson-runtime = "0.1.1"
boson-core = "0.1.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

Or depend on `boson` with the macro re-exported:

```toml
boson = { package = "uf-boson", version = "0.1.1", features = ["mem"] }
```

## Identity in handlers

Handlers receive `Box<dyn ExecutionContext>`. Choosing and installing an
[`ExecutionContextFactory`](https://docs.rs/boson-core/latest/boson_core/trait.ExecutionContextFactory.html)
happens at worker boot — see [`boson-core` identity docs](https://docs.rs/boson-core/latest/boson_core/identity/index.html) and
[`boson-runtime`](https://docs.rs/boson-runtime).

## Documentation

```bash
cargo doc -p boson-macros --no-deps --open
```
