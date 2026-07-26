# boson-axum

HTTP admin API for Boson under [`/api/boson`](https://docs.rs/boson-axum/latest/boson_axum/fn.boson_router.html).

## Role

- Job enqueue, list, get, and cancel (list `limit` capped at 500)
- Run inspection and task config CRUD (retry fields capped on upsert)
- [`boson_router`](https://docs.rs/boson-axum/latest/boson_axum/fn.boson_router.html) — mountable Axum router
- [`BosonState`](https://docs.rs/boson-axum/latest/boson_axum/struct.BosonState.html) / builder — shared state + optional [`AdminAuth`](https://docs.rs/boson-axum/latest/boson_axum/trait.AdminAuth.html)
- Default HTTP enqueue actor is non-System: `{"Service":{"name":"boson_api"}}`

Set `BOSON_REQUIRE_ADMIN_AUTH=1` and install an `AdminAuth` verifier for production mounts.

## Mount

```rust
use std::sync::Arc;

use axum::{extract::FromRef, Router};
use boson_axum::{
    boson_router, BosonState, StaticTokenAdminAuth, NEST_PATH,
};

#[derive(Clone)]
struct AppState {
    boson: BosonState,
}

impl FromRef<AppState> for BosonState {
    fn from_ref(state: &AppState) -> Self {
        state.boson.clone()
    }
}

# fn mount(boson: std::sync::Arc<boson_runtime::Boson>) -> Result<Router<AppState>, boson_axum::BosonAxumError> {
let state = BosonState::builder(boson)
    .admin_auth(Arc::new(StaticTokenAdminAuth::new("lab-token")))
    .require_admin_auth(true)
    .build()?;
let app = Router::new()
    .nest(NEST_PATH, boson_router())
    .with_state(AppState { boson: state });
# Ok(app)
# }
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
| `GET/POST` | `/tasks/{name}/config` | Task configuration |

## Related crates

- [`boson`](https://docs.rs/uf-boson) — enable with `features = ["axum"]`
- [`boson-runtime`](https://docs.rs/boson-runtime) — `Boson` instance wired into state
