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
