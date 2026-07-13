# boson-e2e Enhancement Roadmap

Status after the Photon-bar maturity pass (split PR CI, coverage artifact, mem contract suite, extended catalog).

## Completed (upstream workspace)

| Item | Location | Notes |
|------|----------|-------|
| Rate limit rejection | `scenarios_full` + testkit | `max_in_flight`, `max_eps` → `RateLimited` |
| Task not found | `scenarios_full` | `AssertEnqueueError(TaskNotFound)` |
| Handler failure | `scenarios_full` | Failing fixture → job `Failed` |
| Retry then success | `scenarios_full` + `RetryBackoff` step | Fail N, then `Success` |
| Retry exhaustion | `scenarios_full` | `retry_exhaustion` sad path |
| Idempotency after terminal | `scenarios_full` | Re-enqueue after success |
| Cancel queued job | `scenarios_full` | `JobStatus::Canceled` |
| Multi-job drain | `scenarios_full` | N jobs + handler hit count |
| Restart runtime | `scenarios_full` | `RestartRuntime` step |
| Split-boson-server leases | `scenarios_full` | Drain + lease contention |
| Console telemetry boot | `scenarios_full` | Adapter installs; no event assert yet |
| Admin list/count/get | `scenarios_full` | Runtime APIs |
| Task config override | `scenarios_full` | Upsert affects rate limit |
| Task run stats | `scenarios_full` | `task_run_stats` catalog row |
| List pagination at depth | `scenarios_full` | `list_jobs_pagination` via `AdminListCount` |
| Retry run count | `scenarios_full` | `retry_run_count` + `AssertRunCount` |
| Signature mismatch | `scenarios_full` | Runtime check + sad catalog row |
| Smoke handler assert | `ScenarioSpec::enqueue_and_drain` | `AssertHandlerHits { count: 1 }` |
| HTTP integration | `boson-axum/tests/http_api.rs` | All documented routes + sad paths |
| mem contract suite | `boson-backend-mem/tests/mem_queue_backend.rs` | `backend_contract_suite!` |
| CI split | `boson-matrix.yml` | Parallel PR jobs + full broker e2e on every PR |
| Coverage artifact | `boson-matrix.yml` coverage job | `scripts/coverage.sh`, `docs/VERIFICATION.md` |
| Examples in CI | `boson-matrix.yml` examples job | 4 facade examples |

## Remaining — Priority 1 (host integrations)

| Matrix row | Scenario | Notes |
|------------|----------|-------|
| `server-apps-remote` | HTTP coordinator | `RemoteEnqueue` step stub; needs axum harness |

## Remaining — Priority 2 (deeper coverage)

| Scenario | Notes |
|----------|-------|
| Cancel running job | Flaky with spawn worker; defer or use manual stall fixture |
| `count_runs_since` | API exists; no dedicated catalog row yet |
| Sustained load (BM-BL*) | Bench-only; not e2e |

## Remaining — Priority 3 (bench parity)

Implement BM-B2–B8 / BM-BL* runners aligned with e2e scenarios in [`boson-bench`](../boson-bench/README.md).

## Parameter dimensions still to vary

- Running-job cancel (vs queued cancel)
- Pagination offsets on list APIs (beyond count-at-depth)
- Config revision history (HTTP stub route returns empty list today)
- Multiple workers / pools concurrently
- Redis/NATS fleet routing (manual 2-broker scripts under `infra/native-aws/scripts/`)

## CI tiers

| Tier | Trigger | What runs |
|------|---------|-----------|
| PR | `boson-matrix.yml` | Full matrix: mem/sqlite/postgres/redis/nats contracts + e2e `--include-ignored`; deny; clippy; axum; examples; coverage |
| AWS remote | `~/aws/boson/run-remote-ci.sh` | PR subset without broker containers (deny, clippy, crate tests, mem/sqlite e2e) |
| Manual | `infra/native-aws/scripts/` | Scylla cloud, fleet routing, extended broker campaigns |
