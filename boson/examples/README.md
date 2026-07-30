# Boson examples

Runnable proofs for embedded workers, split enqueue hosts, fleet backends, and HTTP admin mounts. The canonical path below matches the crate README; secondary examples cover idempotency, manual enqueue, and Axum integration.

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

**Docker quickstart** (no local Postgres install): the repo ships a Compose stack at
[`infra/postgres/`](../../infra/postgres/README.md).

```bash
cd infra/postgres && docker compose up -d
export DATABASE_URL="postgres://boson:bench@127.0.0.1:5433/boson_bench"
cd - # back to repo root, then run the worker/enqueue pair above
```

Stop with `docker compose -f infra/postgres/docker-compose.yml down`.

### 4. Remote worker (Redis fleet) — [`redis_fleet_worker.rs`](redis_fleet_worker.rs) · [`redis_fleet_enqueue.rs`](redis_fleet_enqueue.rs)

Broker-backed fleet backend — same split shape, many enqueue hosts and workers sharing Redis.
**Not a `uf-boson` feature** — depends on [`boson-backend-redis`](https://docs.rs/boson-backend-redis)
directly (a path dev-dependency in this crate; production apps add it to `[dependencies]`).

```bash
docker run -d --name boson-redis -p 6379:6379 redis:7
export BOSON_REDIS_URL=redis://127.0.0.1:6379
BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example redis_fleet_worker
# cargo run -p uf-boson --example redis_fleet_enqueue
```

Success: worker `listening (url=…, lease_ttl_secs=…)`; enqueue `enqueued job_id=…`.

### 5. Remote worker (NATS `WorkQueue`) — [`nats_workqueue_worker.rs`](nats_workqueue_worker.rs) · [`nats_workqueue_enqueue.rs`](nats_workqueue_enqueue.rs)

Broker-backed fleet backend on NATS `JetStream` `WorkQueue` streams via
[`connect_auto`](https://docs.rs/boson-backend-nats/latest/boson_backend_nats/fn.connect_auto.html)
with `BOSON_NATS_QUEUE_MODE=workqueue`. **Not a `uf-boson` feature** — depends on
[`boson-backend-nats`](https://docs.rs/boson-backend-nats) directly. `WorkQueue` pool discovery is
per-process, so cross-process workers must pin `BOSON_WORKER_POOLS` (the `#[task]` default pool is
`global`).

```bash
docker run -d --name boson-nats -p 4222:4222 nats:2.10 -js
export BOSON_NATS_URL=nats://127.0.0.1:4222
export BOSON_NATS_QUEUE_MODE=workqueue
BOSON_WORKER_POOLS=global BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example nats_workqueue_worker
# cargo run -p uf-boson --example nats_workqueue_enqueue
```

Success: worker `listening (url=…, lease_ttl_secs=…)`; enqueue `enqueued job_id=…`.

## Host-mount sketches

These examples show how Boson nests into an existing Axum app.

### [`axum_admin.rs`](axum_admin.rs)

Mounts `/api/boson` under your router for enqueue, job inspection, and task config. Set `BOSON_EXAMPLE_SERVE=1` to listen on loopback — useful when wiring admin UI or curl smoke before you add `AdminAuth`.

```bash
BOSON_EXAMPLE_SERVE=1 cargo run -p uf-boson --example axum_admin --features mem,axum
```

Success: `listening on http://127.0.0.1:3000/api/boson`.

**Production:** Boson does not authenticate `/api/boson/*` by itself — install host [`AdminAuth`](https://docs.rs/uf-boson/latest/boson/trait.AdminAuth.html) and prefer `BOSON_REQUIRE_ADMIN_AUTH=1` (see repository [`SECURITY.md`](../../SECURITY.md)).

### [`admin_auth_policy.rs`](admin_auth_policy.rs)

Fail-closed variant of `axum_admin.rs` — always installs `StaticTokenAdminAuth` and
`require_admin_auth(true)` (no env opt-out), then proves `401` without a token and `200` with
`x-boson-admin-token` before ever binding a socket. Set `BOSON_EXAMPLE_SERVE=1` to keep listening
for manual curl testing afterward.

```bash
cargo run -p uf-boson --example admin_auth_policy --features mem,axum
```

Success: `fail-closed admin auth proven: 401 without token, 200 with token`.

## Other examples

| Example | When you'd open it | Command | Success signal |
|---------|-------------------|---------|----------------|
| [`minimal_enqueue.rs`](minimal_enqueue.rs) | Manual registry + `Boson::enqueue` without macro | `cargo run -p uf-boson --example minimal_enqueue --features mem` | `task ran (actor=…)` then `enqueued job …` |
| [`idempotency_and_rate_limit.rs`](idempotency_and_rate_limit.rs) | LWT idempotency key + `max_in_flight` rejection | `cargo run -p uf-boson --example idempotency_and_rate_limit --features mem` | `idempotency: both enqueues returned job …`; `rate limit: second enqueue rejected as expected` |
| [`custom_queue_backend_stub.rs`](custom_queue_backend_stub.rs) | Decorator `QueueBackend` wrapping `mem` to validate/audit enqueue | `cargo run -p uf-boson --example custom_queue_backend_stub --features mem` | `blank task_name rejected; drained job through AuditingQueueBackend` |

Shared handler for remote examples: [`shared/remote_ping.rs`](shared/remote_ping.rs) (`remote_ping: … (actor=…)` on worker drain).

Topology reference: [Embedded](https://docs.rs/uf-boson/latest/boson/index.html#embedded-one-binary) · [Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).
