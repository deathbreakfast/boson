# Supply chain policy

Boson pins third-party crates through `Cargo.lock` and enforces dependency policy with
[`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny) (`deny.toml`).

## What CI checks

The `deny` job in `.github/workflows/boson-matrix.yml` runs `cargo deny check` on every
push and pull request to `main`. That covers:

- RustSec advisories (with documented ignores in `deny.toml`)
- Allowed license set
- Allowed crate sources (crates.io only; no Git deps)

## Git dependencies

No Git sources are currently allowed. Unknown registries and unknown Git remotes are
denied. Prefer crates.io packages (for example `quark = { package = "uf-quark", version = "…" }`).

To add a Git dependency:

1. Justify it in the PR (why crates.io is insufficient)
2. Add the exact HTTPS URL under `[sources].allow-git` in `deny.toml`
3. Pin a tag or commit in the workspace `Cargo.toml`
4. Update this document

## Advisory ignores

Ignored advisories must include a `reason` in `deny.toml`. Prefer fixing or upgrading
when a safe path exists. Current ignores target transitive broker SDK / `async-nats`
lines that cannot be bumped without a coordinated adapter upgrade.

## Verification

Run `cargo deny check` locally, or via remote CI on a provisioned native-aws host:

```bash
~/aws/boson/run-remote-ci.sh
```

That remote script installs `cargo-deny` if needed and runs `cargo deny check` before
the rest of the PR matrix subset.
