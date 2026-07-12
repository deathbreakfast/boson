# Postgres lab infra for embedded backend campaigns (Phase D)

Docker Compose stack for third-party `boson-backend-postgres` adapter benchmarks.

## Usage

```bash
cd infra/postgres
docker compose up -d
export BOSON_BENCH_POSTGRES_URL="postgres://boson:bench@127.0.0.1:5433/boson_bench"
cargo run -p boson-bench -- matrix --subset embedded-lab --backend postgres --hardware aws-t3-medium
```

## Env

| Variable | Default |
|----------|---------|
| `BOSON_BENCH_POSTGRES_URL` | `postgres://boson:bench@127.0.0.1:5433/boson_bench` |

See [`boson-bench/EXPERIMENTS.md`](../boson-bench/EXPERIMENTS.md) embedded backend protocol.
