# boson-backend-redis

[Redis](https://redis.io/) [`QueueBackend`](https://docs.rs/boson-core/latest/boson_core/trait.QueueBackend.html) for fleet-scale Boson deployments.

## Role

Implements [`QueueBackend`](https://docs.rs/boson-core/latest/boson_core/trait.QueueBackend.html) using Redis ZSET ready queues and JSON job bodies. Supports single-node and multi-broker fleet routing by logical pool.

This crate is a **separate workspace dependency** — not enabled via the `boson` facade features (`mem`, `sqlite`, `postgres`). Depend on it directly when you need Redis persistence.

## Compose

Single Redis node:

```rust
use std::sync::Arc;

use boson::{Boson, JsonExecutionContextFactory};
use boson_backend_redis::{RedisQueueBackend, RedisQueueConfig};

let config = RedisQueueConfig {
    url: "redis://127.0.0.1:6379".into(),
    key_prefix: "boson".into(),
};
let backend = Arc::new(RedisQueueBackend::connect(config).await?);
let boson = Boson::builder()
    .queue_backend(backend)
    .execution_context_factory(JsonExecutionContextFactory)
    .build()?;
```

Fleet (multiple standalone Redis nodes):

```rust
use boson_backend_redis::connect_fleet_from_env;

let backend = connect_fleet_from_env().await?;
```

## Bootstrap

```rust
use boson_backend_redis::install_default_redis_backend;
use boson_core::default_backend_from_global;

let _backend = install_default_redis_backend("redis://127.0.0.1:6379").await?;
let resolved = default_backend_from_global()?;
```

## Environment

### Connection

| Variable | Default | Purpose |
|----------|---------|---------|
| `BOSON_TEST_REDIS_URL` | `redis://127.0.0.1:6379` | Test URL resolution |
| `BOSON_BENCH_REDIS_URL` | — | Bench URL override |
| `BOSON_REDIS_KEY_PREFIX` | `boson` | Key namespace prefix |

### Fleet routing

When connecting a multi-broker fleet, URL resolution follows this **precedence** (first match wins):

1. **`BOSON_REDIS_POOL_ROUTING`** — explicit pool-to-URL mapping: `pool_0=redis://host1:6379,pool_1=redis://host2:6379`
2. **`BOSON_REDIS_URLS`** — comma-separated broker URLs; pools hash to `urls[i % N]`

If both are set, `BOSON_REDIS_POOL_ROUTING` takes precedence over `BOSON_REDIS_URLS`.

## Implementing a backend

See **How to implement** on [`QueueBackend`](https://docs.rs/boson-core/latest/boson_core/trait.QueueBackend.html) for the full adapter contract. This crate is a reference implementation for Redis-backed persistence.

## Related crates

- [`boson-core`](https://docs.rs/boson-core) — `QueueBackend` trait definition
- [`boson-backend-nats`](https://docs.rs/boson-backend-nats) — NATS JetStream alternative
- [`boson`](https://docs.rs/boson) — runtime facade (use `mem`/`sqlite`/`postgres` for simpler setups)
