# boson-backend-nats

[NATS JetStream](https://nats.io/) [`QueueBackend`](https://docs.rs/boson-core/latest/boson_core/trait.QueueBackend.html) for fleet-scale Boson deployments.

## Role

Implements [`QueueBackend`](https://docs.rs/boson-core/latest/boson_core/trait.QueueBackend.html) using NATS JetStream KV and WorkQueue streams. Two backend modes:

- **`NatsQueueBackend`** — KV-backed (default when `BOSON_NATS_QUEUE_MODE` is unset)
- **`NatsWorkQueueBackend`** — WorkQueue stream mode (`BOSON_NATS_QUEUE_MODE=workqueue` or `wq`)

This crate is a **separate workspace dependency** — not enabled via the `boson` facade features. Depend on it directly when you need NATS persistence.

## Compose

Single NATS node (auto-selects KV or WorkQueue from env):

```rust
use std::sync::Arc;

use boson::{Boson, JsonExecutionContextFactory};
use boson_backend_nats::connect_auto;

let backend = Arc::new(connect_auto("nats://127.0.0.1:4222").await?);
let boson = Boson::builder()
    .queue_backend(backend)
    .execution_context_factory(JsonExecutionContextFactory)
    .build()?;
```

Fleet (multiple standalone NATS nodes):

```rust
use boson_backend_nats::connect_fleet_from_env;

let backend = connect_fleet_from_env().await?;
```

## Bootstrap

```rust
use boson_backend_nats::install_default_nats_backend;
use boson_core::default_backend_from_global;

let _backend = install_default_nats_backend("nats://127.0.0.1:4222").await?;
let resolved = default_backend_from_global()?;
```

## Environment

### Connection

| Variable | Default | Purpose |
|----------|---------|---------|
| `BOSON_TEST_NATS_URL` | `nats://127.0.0.1:4222` | Test URL resolution |
| `BOSON_BENCH_NATS_URL` | — | Bench URL override |
| `BOSON_NATS_KEY_PREFIX` | `boson` | Key/bucket namespace prefix |
| `BOSON_NATS_QUEUE_MODE` | KV mode | Set to `workqueue` or `wq` for WorkQueue backend |

### Fleet routing

When connecting a multi-broker fleet, URL resolution follows this **precedence** (first match wins):

1. **`BOSON_NATS_POOL_ROUTING`** — explicit pool-to-URL mapping: `pool_0=nats://host1:4222,pool_1=nats://host2:4222`
2. **`BOSON_NATS_URLS`** — comma-separated broker URLs; pools hash to `urls[i % N]`

If both are set, `BOSON_NATS_POOL_ROUTING` takes precedence over `BOSON_NATS_URLS`.

### Enqueue tuning (read at connect time)

| Variable | Default | Purpose |
|----------|---------|---------|
| `BOSON_NATS_ENQUEUE_MODE` | `dual` | `dual` (KV + stream pointer) or `stream` (stream-first) |
| `BOSON_NATS_SYNC_ACK` | `1` | Await JetStream publish ack before returning |
| `BOSON_NATS_MAX_INFLIGHT` | `256` (sync) / `512` (async) | Max concurrent in-flight publishes |
| `BOSON_NATS_FETCH_BATCH` | `1` | Pull consumer batch size on claim |
| `BOSON_NATS_STREAM_REPLICAS` | `1` | JetStream stream replica count |
| `BOSON_NATS_STREAM_SYNC_KV` | off | Block enqueue until KV mirror completes (Stream mode) |

## Implementing a backend

See **How to implement** on [`QueueBackend`](https://docs.rs/boson-core/latest/boson_core/trait.QueueBackend.html) for the full adapter contract. This crate is a reference implementation for NATS-backed persistence.

## Related crates

- [`boson-core`](https://docs.rs/boson-core) — `QueueBackend` trait definition
- [`boson-backend-redis`](https://docs.rs/boson-backend-redis) — Redis alternative
- [`boson`](https://docs.rs/boson) — runtime facade (use `mem`/`sqlite`/`postgres` for simpler setups)
