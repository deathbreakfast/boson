# Native AWS tooling

Scripts under `scripts/` provision EC2 hosts, sync the workspace, and run benches or remote
verification on those hosts.

## Verification (remote CI)

Mirror the PR CI subset (deny, clippy, crate tests, mem/sqlite e2e, examples, docs) on the bench host from your laptop (no local `cargo`):

```bash
~/aws/boson/run-remote-ci.sh [manifest-name]   # set BOSON_ROOT if not ~/boson
```

Defaults to manifest `boson-redis-1` under `~/aws/boson/manifests/` (or `BOSON_NATIVE_MANIFEST`). See Cursor skill `aws-remote-verify`.

Broker-backed contracts against a live fleet:

```bash
./infra/native-aws/scripts/run-redis-e2e.sh
./infra/native-aws/scripts/run-nats-e2e.sh
./infra/native-aws/scripts/run-scylla-e2e.sh
```

Build/deploy `boson-bench`: `~/aws/boson/build-on-ec2.sh`, `build-al2023-local.sh`, `deploy-bench-binary.sh`.

Full GitHub Actions (including postgres/redis/nats service containers) remains the merge gate:
[`.github/workflows/boson-matrix.yml`](../../.github/workflows/boson-matrix.yml).
