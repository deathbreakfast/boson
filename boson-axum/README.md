# boson-axum

HTTP admin API for Boson under [`/api/boson`](https://docs.rs/boson-axum/latest/boson_axum/fn.boson_router.html).

## Role

- Job enqueue, list, get, and cancel
- Run inspection and task config CRUD
- [`boson_router`](https://docs.rs/boson-axum/latest/boson_axum/fn.boson_router.html) — mountable Axum router
- [`BosonState`](https://docs.rs/boson-axum/latest/boson_axum/struct.BosonState.html) — shared handler state (`FromRef` into host router)

## Mount

```rust
use axum::Router;
use boson_axum::{boson_router, BosonState, NEST_PATH};

let app = Router::new()
    .nest(NEST_PATH, boson_router())
    .with_state(BosonState { boson: shared_boson });
```

Enable via the `boson` crate's `axum` feature:

```toml
boson = { package = "uf-boson", version = "0.1.1", features = ["mem", "axum"] }
```

## Routes

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/jobs/enqueue` | Enqueue a job |
| `GET` | `/jobs` | List jobs |
| `GET` | `/jobs/{id}` | Get job by id |
| `POST` | `/jobs/{id}/cancel` | Cancel job |
| `GET` | `/runs` | List runs |
| `GET` | `/tasks` | List registered tasks |
| `GET/PUT` | `/tasks/{name}/config` | Task configuration |

## Related crates

- [`boson`](https://docs.rs/uf-boson) — enable with `features = ["axum"]`
- [`boson-runtime`](https://docs.rs/boson-runtime) — `Boson` instance wired into state
