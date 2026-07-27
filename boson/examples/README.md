# Boson examples

Runnable proofs for embedded workers, split enqueue hosts, and HTTP admin mounts. The canonical path below matches the crate README; secondary examples cover idempotency, manual enqueue, and Axum integration.

Full multi-terminal runbooks: [`../README.md` — How to run examples](../README.md#how-to-run-examples).

## Canonical path

### 1. Embedded — [`task_macro.rs`](task_macro.rs)

One process, mem backend — proves `#[task]`, `auto_registry`, and worker drain in the smallest loop.

```bash
cargo run -p uf-boson --example task_macro --features mem
```

Success: `greet world (actor=…)`.

### 2. Remote worker (SQLite) — [`remote_worker.rs`](remote_worker.rs) · [`remote_enqueue.rs`](remote_enqueue.rs)

Workers claim from a shared file; enqueue host submits jobs — models split binaries before you move to Postgres or fleet backends.

```bash
export BOSON_SQLITE_PATH=/tmp/boson-remote.db
BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example remote_worker --features sqlite
# cargo run -p uf-boson --example remote_enqueue --features sqlite
```

Success: worker `listening (path=…)`; enqueue `enqueued job_id=…`.

### 3. Remote worker (Postgres) — [`postgres_worker.rs`](postgres_worker.rs) · [`postgres_enqueue.rs`](postgres_enqueue.rs)

Same split against a shared `DATABASE_URL` — production-shaped durable backend.

```bash
export DATABASE_URL=postgres://localhost/boson
BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example postgres_worker --features postgres
# cargo run -p uf-boson --example postgres_enqueue --features postgres
```

Success: worker `listening (lease_ttl_secs=…)`; enqueue `enqueued job_id=…`.

## Host-mount sketches

These examples show how Boson nests into an existing Axum app — not a standalone admin server.

### [`axum_admin.rs`](axum_admin.rs)

Mounts `/api/boson` under your router for enqueue, job inspection, and task config. Set `BOSON_EXAMPLE_SERVE=1` to listen on loopback — useful when wiring admin UI or curl smoke before you add `AdminAuth`.

```bash
BOSON_EXAMPLE_SERVE=1 cargo run -p uf-boson --example axum_admin --features mem,axum
```

Success: `listening on http://127.0.0.1:3000/api/boson`.

**Production:** Boson does not authenticate `/api/boson/*` by itself — install host [`AdminAuth`](https://docs.rs/uf-boson/latest/boson/trait.AdminAuth.html) and prefer `BOSON_REQUIRE_ADMIN_AUTH=1` (see repository [`SECURITY.md`](../../SECURITY.md)).

## Other examples

| Example | When you'd open it | Command | Success signal |
|---------|-------------------|---------|----------------|
| [`minimal_enqueue.rs`](minimal_enqueue.rs) | Manual registry + `Boson::enqueue` without macro | `cargo run -p uf-boson --example minimal_enqueue --features mem` | `task ran (actor=…)` then `enqueued job …` |
| [`idempotency_and_rate_limit.rs`](idempotency_and_rate_limit.rs) | LWT idempotency key + `max_in_flight` rejection | `cargo run -p uf-boson --example idempotency_and_rate_limit --features mem` | `idempotency: both enqueues returned job …`; `rate limit: second enqueue rejected as expected` |

Shared handler for remote examples: [`shared/remote_ping.rs`](shared/remote_ping.rs) (`remote_ping: … (actor=…)` on worker drain).

Topology reference: [Embedded](https://docs.rs/uf-boson/latest/boson/index.html#embedded-one-binary) · [Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).
