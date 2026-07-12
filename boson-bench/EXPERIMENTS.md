# Boson benchmark experiment registry

Pre-registered experiment IDs, phase status, and runner commands.

**Methodology:** [`PERFORMANCE_STUDY.md`](PERFORMANCE_STUDY.md). **Archived campaigns:** [`EXPERIMENTS-ARCHIVE.md`](EXPERIMENTS-ARCHIVE.md).

---

## Validation status

| Tier | Backend | Status | Authoritative hardware |
|------|---------|--------|------------------------|
| **Enqueue capacity** | Redis | **Measured** | `aws-c6i-large` (BE4 F1+F2) |
| **Enqueue capacity** | NATS stream-first | **Measured** | `aws-c6i-large` (BE4 publisher/shard/fleet/multibench) |
| **Dequeue capacity** | Redis | **Measured** | `aws-c6i-large` (BD2 D0–D3 partial) |
| **Dequeue capacity** | NATS WorkQueue | **Measured** | `aws-c6i-large` (BD2 D0–D3 + E1) |
| **Tier 3 reference** | Redis + NATS | **Measured** | `aws-t3-medium` (BE/BD gates) |
| **Scylla** | scylla | **Paused** | `aws-t3-medium` — archived |
| **Tier 1–2** | mem/sqlite/postgres | **Done** | `aws-t3-medium` — archived |

---

## Authoritative test environment

Decision-grade Tier 3 numbers use **`aws-c6i-large`** bench, **`t3.medium`** broker(s), `isolated-lab`, telemetry off, prefill 10k, `IdempotencyMode::None`. Retained reports are AWS-only under `profiling/boson-bench/reports/`.

| Field | Value |
|-------|-------|
| Bench | 1× `c6i.large` per embed client |
| Broker | 1× or N× `t3.medium` |
| NATS | `BOSON_NATS_ENQUEUE_MODE=stream`, WorkQueue drain |
| Fleet | `BOSON_NATS_URLS` / `BOSON_REDIS_URLS` |
| Reports | `profiling/boson-bench/reports/scaling-curve-*-aws-c6i-large-{nats,redis}.json` |

Interactive ladders: Cursor canvas `boson-bench-aws-c6i-all` (local IDE artifact; not checked into this repo).

### Artifact policy

| Keep in Git | Publish outside Git |
|-------------|---------------------|
| `boson-bench` harness, experiment IDs, pass criteria, report schema | Heavy JSON reports, flamegraphs, perf captures |
| Methodology docs (`EXPERIMENTS.md`, `PERFORMANCE_STUDY.md`) | Machine-specific scaling runs as CI/release assets |
| Small golden fixtures only when needed for assertions | Full decision-grade AWS report bundles |

Default write path: `profiling/boson-bench/reports/`. Prefer AWS decision-grade retention; discard local smoke outputs.

---

## Experiment taxonomy

Three tracks — do not mix metrics:

| Track | IDs | Worker | Metric | Use |
|-------|-----|--------|--------|-----|
| **Enqueue capacity** | BM-BE1/BE2/BE4 | **None** | `achieved_ops_per_sec` | Publisher ceiling |
| **Dequeue capacity** | BM-BD1/BD2 | W parallel | `drain_ops_per_sec` | Claim ceiling |
| **Soak** | BM-BL* | 1 background | paced enqueue | Regression — **not** capacity |

### Sweep dimensions

| Knob | Default (BE4) | Isolates |
|------|---------------|----------|
| `client_count` (C) | 64 | Publisher concurrency |
| `pool_count` (K) | 10 | Partition spread |
| `worker_count` (W) | 10 (BD2: 32) | Parallel drain |
| `fleet_size` (N) | 1 | Broker fleet |
| `bench_client_count` (bc) | 1 | Multi-embed |

Suggested sweeps: hot partition (K=1) → partition scale (K grid) → drain scale (W grid) → fleet (N) → multibench (bc).

---

## Phase status — Tier 3 capacity (aws-c6i.large)

### Enqueue (BM-BE4)

| Phase | Shape | NATS peak | Redis peak | Verdict | Artifact |
|-------|-------|-----------|------------|---------|----------|
| Publisher K=1 | C sweep | **28,299/s** @ C=256 | **89,335/s** @ C=512 | sat C≥32 / scaling | `scaling-curve-be4-publishers-*` |
| Shard | K ∈ {1,4,10,32}, C=256 | 28,539/s @ K=1 | **66,210/s** @ K=10 | `broker_saturated` | `scaling-curve-be4-shards-*` |
| Fleet | N ∈ {1,2,4}, C=256 | **37,985/s** @ N=2 | — | `fleet_saturated` | `scaling-curve-be4-fleet-aws-c6i-large-nats.json` |
| Multibench | bc ∈ {1,2,4}, N=4 | **58,441/s** @ bc=4 | — | `sublinear` | `scaling-curve-be4-multibench-aws-c6i-large-nats.json` |
| Cluster ref | RAFT K=4 | 28,915/s | — | embed-limited | `bm-be4-cluster-k4-c256-*.json` |

### Dequeue (BM-BD2)

| Phase | Shape | NATS peak | Redis peak | Verdict | Artifact |
|-------|-------|-----------|------------|---------|----------|
| D0 Workers | W sweep, K=1 | **4,523/s** @ W=64 | **10,537/s** @ W=32 | worker_scaling / saturated@16 | `scaling-curve-bd2-workers-*` |
| D1 Shards | K sweep | **5,002/s** @ K=1 | **6,895/s** @ K=32 | `shard_saturated` | `scaling-curve-bd2-shards-*` |
| D2 Fleet | N ∈ {1,2,4}, K=N | **6,472/s** @ N=4 | **9,918/s** @ N=1 W=32+pin | `fleet_saturated` | `scaling-curve-bd2-fleet-*` |
| D3 Multibench | bc, N=4, K=4 | **10,711/s** @ bc=4 | **4,465/s** @ bc=2 | embed_sublinear | `scaling-curve-bd2-multibench-*` |
| E1 NATS bc retry | bc=2,4 @ W=16 | 4,756 / 10,711/s | — | partition fix | same multibench curve |
| E2c Redis bc=2 | W=16, pin | — | 4,465/s | quota-safe | `scaling-curve-bd2-multibench-aws-c6i-large-redis.json` |

### Optimization (post-D)

| ID | Knob | Result | Status |
|----|------|--------|--------|
| D7 | Fetch batch | batch=1 best | **Neutral** — production default |
| D8 | Skip claim KV | 1.11× AWS | **Deferred** — bench flag only |
| D9 | Pool pinning | +157% @ N=4 fleet | **Adopt** — pin workers when fleet N>1 |
| D5 | Stream replicas | ~1.0–1.1× | **Neutral** — rep=1 default |
| D4 | Cluster drain | — | **Gated** (quota) |
| D6 | Redis read replica | — | **Deferred** |
| D10 | Drain-only multibench | harness ready | **Gated** (quota) |
| D11 | c6i.large broker BD2 | — | **Gated** (quota) |

---

## Tier 3 reference (`aws-t3-medium`)

Dedicated broker per backend. Reference only — not mixed with c6i.large curves.

| Gate | Redis | NATS WorkQueue |
|------|-------|----------------|
| BE4 (C=64, K=10) | **29,107/s** | **9,346/s** |
| BD2 W=1 / W=4 | 647 / 1,394/s | 520 / 1,162/s |
| BL3 soak | ~770/s | ~101/s (KV) |

Campaign IDs: `boson-tier3-capacity-20260705-r2`, `boson-tier3-capacity-sweep-20260706`, `boson-tier3-capacity-wq-20260706`.

---

## Question coverage matrix

| Question | Primary experiments |
|----------|---------------------|
| Serial vs concurrent enqueue | BM-BE1, BM-BE2 |
| Multi-pool enqueue ceiling | BM-BE4 |
| Worker drain scaling | BM-BD2 D0 |
| Shard drain on one broker | BM-BD2 D1 |
| Broker fleet drain | BM-BD2 D2 |
| Multi-embed drain | BM-BD2 D3 / E1 |
| Publisher saturation | BE4 publisher sweep |
| Broker shard saturation | BE4 shard sweep |
| Fleet enqueue scaling | BE4 fleet + multibench |
| Pool pinning production lever | D9 |

---

## Harness and configuration

Bench knobs use **`BenchRunConfig`** ([`src/config.rs`](src/config.rs)) with per-experiment defaults via `BenchRunConfig::for_experiment(id)`. CLI flags map onto the same struct ([`src/cli/bench_config.rs`](src/cli/bench_config.rs)).

```bash
boson-bench run --experiment bm-be4 --backend redis \
  --client-count 64 --pool-count 10 --pool-layout distinct \
  --prefill-count 10000 --worker-count 10 --worker-poll-ms 0
```

Subsets: `tier3-capacity` (bm-be1, be2, be4, bd1, bd2), `mem-scale` (bm-bp1, bp2, bm1–bm4).

Storage backends connect via `BootstrapSession` with explicit `RedisQueueConfig` / `ScyllaQueueConfig` — see [`boson-testkit`](../boson-testkit/README.md).

---

## Decision knobs (campaign env)

| Variable | Purpose |
|----------|---------|
| `BOSON_NATS_URLS` / `BOSON_REDIS_URLS` | Fleet pool routing |
| `BOSON_BD2_PIN_WORKER_POOLS=1` | Bench pool pin (D9) |
| `BOSON_WORKER_POLL_MS=0` | Capacity drain |
| `BOSON_SKIP_RUN_ROWS=1` | Bench-only — skip run persistence |
| `BOSON_NATS_ENQUEUE_MODE=stream` | Stream-first enqueue |
| `BOSON_BENCH_CLIENT_INDEX/COUNT` | Multibench partition |
| `BOSON_BD2_MULTIBENCH_BC` | bc values for D3/E1 |

Full campaign env documented in infra scripts under `infra/native-aws/broker-fleet/`.

---

## Run commands

```bash
# AWS BE4 multibench (NATS)
bash infra/native-aws/broker-fleet/run-be4-multibench-sweep-aws.sh

# AWS BD2 full ladder (NATS D0→D3)
bash infra/native-aws/broker-fleet/run-bd2-campaign-aws.sh

# AWS Redis BE4 fill + BD2 replay
bash infra/native-aws/broker-fleet/run-be4-redis-replay-aws.sh
bash infra/native-aws/broker-fleet/run-bd2-redis-replay-aws.sh

# Projection curves (local, from reports/)
cargo run -p boson-bench -- be4-publisher-curve --hardware aws-c6i-large --backend nats
cargo run -p boson-bench -- bd2-fleet-curve --hardware aws-c6i-large --backend redis
```

Tier 3 t3.medium lab: `infra/native-aws/scripts/run-broker-lab.sh`. Scylla: see [`EXPERIMENTS-ARCHIVE.md`](EXPERIMENTS-ARCHIVE.md).

---

## Prior art (do not re-run)

Continuum measured engine ceiling, Scylla saturation, and topic fan-out on the same `aws-t3-medium` class. Boson does not schedule those campaigns.

| Topic | Reference |
|-------|-----------|
| Scylla appendices F–H | [Continuum PERFORMANCE_STUDY](https://github.com/unified-field-dev/continuum/blob/main/continuum-bench/PERFORMANCE_STUDY.md) |
| Track T/U/W/X/Z IDs | [Continuum EXPERIMENTS](https://github.com/unified-field-dev/continuum/blob/main/continuum-bench/EXPERIMENTS.md) |

---

## Host constraints

- Multi-node AWS campaigns run only in `infra/native-aws` — not on local multi-node Docker.
- Local builds: `CARGO_BUILD_JOBS=1`, dedicated `CARGO_TARGET_DIR`.
- Reports: `profiling/boson-bench/reports/{id}-{backend}-{topology}-{hardware}.json`
