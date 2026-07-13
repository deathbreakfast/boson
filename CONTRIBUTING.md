# Contributing

## Before opening a PR

1. Read [`docs/VERIFICATION.md`](docs/VERIFICATION.md).
2. Run verification locally, or mirror the PR subset on a provisioned native-aws host:

```bash
~/aws/boson/run-remote-ci.sh
```

3. For broker-backed contracts against a provisioned fleet, chain:

```bash
./infra/native-aws/scripts/run-redis-e2e.sh
./infra/native-aws/scripts/run-nats-e2e.sh
```

4. Confirm GitHub Actions `boson-matrix` is green on your branch (full postgres/redis/nats e2e runs there).

## Supply chain

Dependency and license policy lives in [`deny.toml`](deny.toml) and [`docs/supply-chain.md`](docs/supply-chain.md). The PR CI `deny` job must pass.

## Security

See [`SECURITY.md`](SECURITY.md) for private vulnerability reporting. Do not open public issues for exploitable bugs.
