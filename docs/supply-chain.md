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
when a safe path exists. As of `async-nats` 0.49, the prior `rustls-webpki` 0.102
RUSTSEC-2026-* ignores were removed (dependency now pulls `rustls-webpki` 0.103).
Remaining ignore: unmaintained `paste` via broker SDK transitive edges.

## Verification

Run `cargo deny check` locally. Maintainers also re-run deny as part of remote CI on AWS.
