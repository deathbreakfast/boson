# Native AWS tooling

Scripts under `scripts/` provision EC2 hosts, sync the workspace, and run benches or remote
verification on those hosts.

## Verification (remote CI)

Mirror the PR CI subset (deny, clippy, crate tests, mem/sqlite e2e, examples, docs) on the bench host:

```bash
./infra/native-aws/scripts/run-remote-ci.sh [manifest-name]
```

Defaults to manifest `boson-redis-1` (or `BOSON_NATIVE_MANIFEST`). Requires
`BOSON_NATIVE_AWS_KEY_PATH` and a provisioned manifest under `manifests/`.

Broker-backed contracts against a live fleet:

```bash
./infra/native-aws/scripts/run-redis-e2e.sh
./infra/native-aws/scripts/run-nats-e2e.sh
./infra/native-aws/scripts/run-scylla-e2e.sh
```

Build only `boson-bench` on EC2: [`scripts/build-on-ec2.sh`](scripts/build-on-ec2.sh).

Full GitHub Actions (including postgres/redis/nats service containers) remains the merge gate:
[`.github/workflows/boson-matrix.yml`](../../.github/workflows/boson-matrix.yml).
