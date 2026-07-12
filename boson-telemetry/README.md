# boson-telemetry

`OpsLog` telemetry for Boson self-metrics and ops events.

## Adapters (this crate)

| Adapter | Notes |
|---------|-------|
| `ConsoleOpsLog` | stderr / structured console |
| `NoOpsLog` | default no-op |

Hosts may install other `OpsLog` implementations from separate adapter crates at boot.

## Environment

- `BOSON_TELEMETRY=off|console` (default `console`)

## Related crates

- [`boson`](https://docs.rs/boson) — re-exports and `telemetry-console` feature
- [`boson-runtime`](https://docs.rs/boson-runtime) — records enqueue/complete/fail metrics via `OpsLog`
