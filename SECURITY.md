# Security Policy

## Supported versions

Security fixes are applied to the latest published release on the `main` branch.
Older tags may not receive backports unless a release is still actively supported
in the changelog.

## Reporting a vulnerability

Please report security issues privately. Do **not** open a public GitHub issue for
exploitable vulnerabilities.

1. Email the maintainers using the address listed on the GitHub organization or
   repository profile, with subject line `[boson] security report`.
2. Include reproduction steps, affected versions, and impact assessment when possible.
3. Allow a reasonable window for a fix and coordinated disclosure before public
   discussion.

We aim to acknowledge reports within 7 days and to share a remediation plan or
mitigation guidance once the issue is confirmed.

## Scope notes

Boson is a job-work runtime with pluggable persistence and an optional HTTP admin
surface. Reports that matter most:

- Unauthorized enqueue, claim, cancel, or admin access
- Cross-tenant or cross-pool data exposure via mis-scoped backends
- Dependency supply-chain issues affecting published crates
- Secrets or credentials leaked through logs, ops events, or error messages

Memory-safety bugs in safe Rust code are welcome, but prefer reports that show a
reachable path from public APIs or adapters.

## Operator hardening (L0)

| Area | Guidance |
|------|----------|
| HTTP admin | Install a host `AdminAuth` verifier. Set `BOSON_REQUIRE_ADMIN_AUTH=1` so mounts fail closed without one. Boson does not ship Soliton HMAC/mTLS. |
| HTTP enqueue actor | Default `actor_json` is `{"Service":{"name":"boson_api"}}` (not System). Host identity kits must not elevate this marker. |
| Actor provenance | Optional `ActorJsonPolicy` / `RejectExternalSystemActor` rejects System-shaped JSON on `EnqueueTrust::External`. In-process enqueue remains trusted. |
| List limits | HTTP list `limit` is clamped to 500 (`MAX_LIST_LIMIT`). |
| Leases | Mode 2: `lease_ttl_secs > 0` + unique `worker_id`. Workers heartbeat `extend_lease` during handlers. |
| Cancel | Cancel is cooperative: in-flight handlers are aborted via status watch; finish does not overwrite to Success. |
| Errors | Handler errors are sanitized/truncated before run rows and telemetry; do not log `params_json` / `actor_json`. |
| Rate limits | Enqueue rate limits are process-local unless the backend provides shared counters. Retry fields via HTTP config are capped. |
| Lab infra | Bench Redis `protected-mode no` / Postgres password `bench` under `infra/` are lab-only — not production templates. |

See also crate docs on `uf-boson` (§ Features / § 5 Mount HTTP admin).
