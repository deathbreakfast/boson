# Boson queue performance — Redis vs NATS

Pre-registered workloads and runner commands: [`EXPERIMENTS.md`](EXPERIMENTS.md).

---

## Executive summary

Boson is a **typed job queue runtime** with pluggable storage adapters. Tier 3 hyperscale evaluation focuses on **Redis** and **NATS JetStream WorkQueue** under concurrent enqueue (BM-BE4) and drain (BM-BD2) capacity workloads.

**Measured scope (primary row: `aws-c6i-large` bench, `t3.medium` broker, `isolated-lab`, prefill 10k, `IdempotencyMode::None`)**

| Surface | Status | Key number |
|---------|--------|------------|
| **Redis enqueue** (BE4) | **Measured** | **89,335 ops/s** publisher peak (K=1, C=512); **66,210 ops/s** shard aggregate @ K=10 |
| **NATS enqueue** (BE4 stream-first) | **Measured** | **28,299 ops/s** @ C=256; **58,441 ops/s** aggregate @ bc=4 multibench |
| **Redis drain** (BD2) | **Measured** | **10,537 ops/s** @ W=32 single broker; **9,918 ops/s** fleet N=1 W=32+pin |
| **NATS drain** (BD2) | **Measured** | **4,523 ops/s** @ W=64; **10,711 ops/s** aggregate @ bc=4 multibench |
| **Tier 3 reference** (`t3.medium`) | **Measured** | Redis BE4 **29,107/s**; NATS WorkQueue BE4 **9,346/s** |

**Sizing rule of thumb**

- **Enqueue:** One embed on a 4-broker pool-routed fleet sustains **~29k/s (NATS)** or **~89k/s (Redis)** single-broker publisher peak. Multi-embed (bc=4) raises NATS aggregate to **~58k/s**; embed count binds before brokers at N=4 on one bench host.
- **Drain:** Fill÷drain gap is **~6× (NATS)** and **~8–10× (Redis)** at single-broker peaks. Horizontal drain scales sublinearly: NATS bc=4 **10.7k/s** vs Redis bc=2 **4.5k/s** on shared N=4 fleet.
- **330k enqueue target:** ~12 embed hosts @ NATS bc=1 fleet peak (~29k/embed), or fewer embeds if bc>1 sublinearity holds. Drain at 330k requires multibench + fleet tuning beyond current **~11k/s** NATS ceiling.

**Backend guidance:** Redis wins raw enqueue and single-broker drain; NATS stream-first passes the **≥28k/s** aws gate and has validated fleet + multibench scaling. Default Tier 3 pick remains **context-dependent** — see §7.

**Not measured:** Subscriber-style fanout at queue rates; Redis bc=4 multibench (16 vCPU quota); JetStream cluster drain (D4); Redis read-replica claim path (D6).

Full benchmark ladders: Cursor canvas `boson-bench-aws-c6i-all` (local IDE artifact; not checked into this repo).

---

## 1. Scope

| In scope | Out of scope |
|----------|--------------|
| Redis + NATS JetStream WorkQueue on AWS | Scylla hyperscale (paused — see [`EXPERIMENTS-ARCHIVE.md`](EXPERIMENTS-ARCHIVE.md)) |
| BM-BE* enqueue / BM-BD* drain capacity | BM-BL* paced soak as capacity gate |
| Fleet (`N`) and multibench (`bc`) scaling | Local smoke (not retained) |
| `aws-c6i-large` decision-grade curves | NATS KV drain path (superseded by WorkQueue) |

Tier 1–2 (mem, sqlite, postgres) baselines are complete on `aws-t3-medium`; they do not gate Tier 3 decisions. Archived detail: [`EXPERIMENTS-ARCHIVE.md`](EXPERIMENTS-ARCHIVE.md).

---

## 2. Authoritative test environment

All **decision-grade** Tier 3 numbers use this primary row on **`aws-c6i-large`**, in-VPC. Do not mix with local smoke or `aws-t3-medium` except where labeled reference.

| Field | Value |
|-------|-------|
| **Bench host** | 1× `c6i.large` (2 vCPU, 4 GiB) per embed client |
| **Broker** | 1× `t3.medium` (single-broker sweeps) or N× `t3.medium` (fleet) |
| **Topology** | `isolated-lab`, telemetry `off` |
| **NATS enqueue** | `BOSON_NATS_ENQUEUE_MODE=stream`, WorkQueue retention |
| **Prefill** | 10,000 jobs before BD2 drain |
| **BD2 workers** | `BOSON_WORKER_POLL_MS=0`; W* = 32 (Redis D1–D3) or 16–64 (NATS) |
| **Fleet routing** | `BOSON_NATS_URLS` / `BOSON_REDIS_URLS` → `PoolRoutedBackend` |
| **Reports** | `profiling/boson-bench/reports/scaling-curve-*-aws-c6i-large-{nats,redis}.json` |

Runbooks: `infra/native-aws/broker-fleet/run-be4-multibench-sweep-aws.sh`, `run-bd2-campaign-aws.sh`, `run-bd2-redis-replay-aws.sh`, `run-bd2-redis-multibench-sweep-aws.sh`.

---

## 3. Findings summary

| Finding | Configuration | Result | Verdict |
|---------|---------------|--------|---------|
| Redis enqueue dominates NATS | BE4 publisher K=1 | **89,335/s** vs **28,299/s** | `publisher_scaling` |
| Multi-stream on one broker flatlines | BE4 K sweep | ~23–28k/s aggregate regardless of K | `broker_saturated` |
| Pool-routed fleet scales enqueue (N=2) | BE4 N=2, C=256 | **37,985/s** (eff **0.70**) | `fleet_saturated` |
| Multi-embed raises NATS aggregate | bc=4, N=4 fleet | **58,441/s** (2.0× bc=1) | `sublinear` |
| Redis drain scales with W | BD2 D0, K=1 | **10,537/s** @ W=32 | `worker_scaling` |
| NATS drain knee at W=16 | BD2 D0, K=1 | **3,387/s** @ W=16; **4,523/s** @ W=64 | `worker_saturated_at_w=16` |
| Redis fleet N>1 collapses | D2 W=32+pin | N=1 **9,918/s** → N=4 **4,837/s** | `fleet_saturated` |
| NATS multibench drain scales | bc=4, N=4 | **10,711/s** aggregate | `embed_sublinear` |
| Pool pinning helps fleet drain | D9, N=4 NATS | **1,825/s** pinned vs **710/s** unpinned | production default |
| Claim-path batching neutral | D7/D8 AWS | skip-KV **1.11×** (<2× bar) | deferred |

---

## 4. Methodology

### 4.1 Metric taxonomy

Three tracks — **do not mix metrics**:

| Track | IDs | Worker | Metric | Use |
|-------|-----|--------|--------|-----|
| **Enqueue capacity** | BM-BE1/BE2/BE4 | None | `achieved_ops_per_sec` | Publisher + adapter ceiling |
| **Dequeue capacity** | BM-BD1/BD2 | W parallel | `drain_ops_per_sec` | Claim + lifecycle ceiling |
| **Soak** | BM-BL* | 1 background | paced enqueue rate | Regression only — **not** capacity |

### 4.2 Scaling dimensions

| Knob | Symbol | Isolates |
|------|--------|----------|
| `client_count` | C | Publisher concurrency |
| `pool_count` | K | Partition / stream spread |
| `worker_count` | W | Parallel drain |
| `fleet_size` | N | Standalone brokers (pool-routed) |
| `bench_client_count` | bc | Independent embed hosts |

Sweep order for capacity: **W → K → N → bc** (BD2 mirrors BE4).

### 4.3 Pass criteria

- Error rate &lt; 0.1% on capacity cells
- Efficiency = aggregate / (N × single-unit peak) or multibench equivalent
- Verdict tags: `worker_scaling`, `shard_saturated`, `broker_saturated`, `fleet_saturated`, `embed_sublinear`, `neutral`

---

## 5. Results

### 5.1 Enqueue capacity (BM-BE4)

#### Publisher sweep (K=1 shared pool)

| C | Redis ops/s | NATS ops/s |
|---|-------------|------------|
| 256 | 65,198 | **28,299** |
| 512 | **89,335** | 26,937 |

NATS saturates publishers at C≥32 (`publisher_saturation_at_c=32`). Artifact: [`scaling-curve-be4-publishers-aws-c6i-large-nats-k1-shared.json`](../profiling/boson-bench/reports/scaling-curve-be4-publishers-aws-c6i-large-nats-k1-shared.json), [`scaling-curve-be4-publishers-aws-c6i-large-redis-k1-shared.json`](../profiling/boson-bench/reports/scaling-curve-be4-publishers-aws-c6i-large-redis-k1-shared.json).

#### Shard sweep (C=256)

| K | Redis agg/s | NATS agg/s | NATS shard_eff |
|---|-------------|------------|----------------|
| 1 | 51,932 | **28,539** | 1.00 |
| 10 | **66,210** | 26,556 | 0.09 |
| 32 | 61,744 | 23,300 | 0.03 |

**Anti-pattern:** Adding K streams on **one broker** does not raise aggregate throughput — `broker_saturated`. Use **pool-routed fleet** (N brokers) or larger broker instance.

Artifact: [`scaling-curve-be4-shards-aws-c6i-large-nats.json`](../profiling/boson-bench/reports/scaling-curve-be4-shards-aws-c6i-large-nats.json), [`scaling-curve-be4-shards-aws-c6i-large-redis.json`](../profiling/boson-bench/reports/scaling-curve-be4-shards-aws-c6i-large-redis.json).

#### Broker fleet (N standalone, C=256)

| N | NATS agg/s | fleet_eff |
|---|------------|-----------|
| 1 | 27,022 | 1.00 |
| 2 | **37,985** | 0.70 |
| 4 | 29,213 | 0.27 |

N=4 on one bench host is **embed-limited** (~29k) before brokers saturate.

Artifact: [`scaling-curve-be4-fleet-aws-c6i-large-nats.json`](../profiling/boson-bench/reports/scaling-curve-be4-fleet-aws-c6i-large-nats.json).

#### Multi-embed (N=4 fleet, K=4, C=256)

| bc | NATS aggregate | vs bc=1 |
|----|------------------|---------|
| 1 | 29,279/s | 1.00× |
| 2 | 48,129/s | 1.64× |
| 4 | **58,441/s** | 2.00× |

Artifact: [`scaling-curve-be4-multibench-aws-c6i-large-nats.json`](../profiling/boson-bench/reports/scaling-curve-be4-multibench-aws-c6i-large-nats.json).

#### Broker sizing note

Upgrading broker from t3.medium → c6i.large buys **~35%** per-node headroom (BE4 **38,203/s** @ C=512 vs **28,299/s**) but does **not** fix multi-stream saturation on one node.

#### JetStream RAFT cluster reference

4-node cluster K=4 vs pool-routed fleet N=4: **28,915/s** vs **29,213/s** — embed-limited; cluster overhead negligible at this scale.

---

### 5.2 Dequeue capacity (BM-BD2)

Primary metric: `drain_ops_per_sec` = prefill_count / drain_elapsed.

#### D0 — Worker scaling (K=1, N=1)

| W | Redis drain/s | NATS drain/s |
|---|---------------|--------------|
| 16 | 5,301 | 3,387 |
| 32 | **10,537** | 2,451 |
| 64 | — | **4,523** |

Redis: `worker_scaling`. NATS: knee at W=16, recovery at W=64.

Artifact: [`scaling-curve-bd2-workers-aws-c6i-large-redis.json`](../profiling/boson-bench/reports/scaling-curve-bd2-workers-aws-c6i-large-redis.json), [`scaling-curve-bd2-workers-aws-c6i-large-nats.json`](../profiling/boson-bench/reports/scaling-curve-bd2-workers-aws-c6i-large-nats.json).

#### D1 — Shard scaling (fixed W)

| K | Redis @ W=16 | NATS @ W=32 |
|---|--------------|-------------|
| 1 | 5,201/s | **5,002/s** |
| 32 | **6,895/s** | 290/s |

Redis shard sweep remains useful on one broker; NATS collapses at high K — `shard_saturated`.

Artifact: [`scaling-curve-bd2-shards-aws-c6i-large-redis.json`](../profiling/boson-bench/reports/scaling-curve-bd2-shards-aws-c6i-large-redis.json), [`scaling-curve-bd2-shards-aws-c6i-large-nats.json`](../profiling/boson-bench/reports/scaling-curve-bd2-shards-aws-c6i-large-nats.json).

#### D2 — Broker fleet (K=N, W=32+pin Redis / W=32–64 NATS)

| N | Redis W=32+pin | NATS |
|---|----------------|------|
| 1 | **9,918/s** | 5,280/s |
| 2 | 6,329/s | 5,490/s |
| 4 | 4,837/s | **6,472/s** |

Redis fleet does **not** scale with N despite W=32+pin doubling N=1 vs W=16 (~4.4k → ~9.9k). The ~4.8k N=4 plateau is **shared fleet / broker contention**, not bench CPU.

Artifact: [`scaling-curve-bd2-fleet-aws-c6i-large-redis.json`](../profiling/boson-bench/reports/scaling-curve-bd2-fleet-aws-c6i-large-redis.json), [`scaling-curve-bd2-fleet-aws-c6i-large-nats.json`](../profiling/boson-bench/reports/scaling-curve-bd2-fleet-aws-c6i-large-nats.json).

#### D3 — Multi-embed (N=4, K=4)

| bc | NATS aggregate | Redis aggregate |
|----|----------------|-----------------|
| 1 | 6,459/s | — |
| 2 | 4,756/s | **4,465/s** |
| 4 | **10,711/s** | blocked (quota) |

Multibench requires pool-partition fix ([`drain.rs`](src/drain.rs): disjoint prefill + `worker_pools` per `BOSON_BENCH_CLIENT_INDEX`).

Artifact: [`scaling-curve-bd2-multibench-aws-c6i-large-nats.json`](../profiling/boson-bench/reports/scaling-curve-bd2-multibench-aws-c6i-large-nats.json), [`scaling-curve-bd2-multibench-aws-c6i-large-redis.json`](../profiling/boson-bench/reports/scaling-curve-bd2-multibench-aws-c6i-large-redis.json).

#### Fill ÷ drain parity (single broker peaks)

| Metric | NATS | Redis | Redis ÷ NATS |
|--------|------|-------|--------------|
| BE4 publisher peak | 28,299/s | 89,335/s | 3.16× |
| BD2 drain peak (D0) | 4,523/s @ W=64 | 10,537/s @ W=32 | 2.33× |
| Fill ÷ drain | ~6.3× | ~8.5× | — |

---

### 5.3 Optimization knobs

| Knob | AWS result | Production |
|------|------------|------------|
| **Pool pinning** (D9) | +157% @ N=4 fleet (1,825 vs 710/s) | Enable `BOSON_WORKER_POOLS` when fleet N>1, K=N |
| **Skip claim KV** (D8) | 1.11× (3,323 vs 2,990/s) | **Do not enable** — bench flag only |
| **Fetch batch** (D7) | batch=1 best | Keep default batch=1 |
| **Stream replicas** (D5) | ~1.0–1.1× | **`rep=1`** default for capacity |
| **Poll interval** | BD2 uses 0 | `BOSON_WORKER_POLL_MS=0` for capacity drain |

Production drain defaults: pool pinning when fleet N>1 (`BOSON_WORKER_POOLS`), `BOSON_WORKER_POLL_MS=0` for capacity drain, do not enable skip-claim-KV.

---

### 5.4 Tier 3 reference (`aws-t3-medium`)

Dedicated broker per backend (`redis-1` / `nats-1`). Use for medium-SKU context only.

| Gate | Redis | NATS WorkQueue |
|------|-------|----------------|
| BE4 (C=64, K=10) | **29,107/s** | **9,346/s** |
| BD2 W=1 / W=4 | 647 / 1,394/s | 520 / 1,162/s |
| BL3 soak (not capacity) | ~770/s | ~101/s (KV) |

Historical NATS **KV** drain failed at scale; all decision-grade NATS drain uses **WorkQueue** mode.

---

## 6. Discussion

### 6.1 Binding constraints

1. **Embed-limited enqueue:** N=4 fleet on one c6i.large flatlines ~29k/s NATS — add embed hosts (bc>1) before adding brokers.
2. **Broker-saturated sharding:** K>1 on one broker does not linearly scale; route pools to standalone brokers via `PoolRoutedBackend`.
3. **Drain vs enqueue gap:** Worker claim path limits drain to ~10–20% of enqueue peak; optimize W, pinning, and multibench before broker hardware upgrades.
4. **Redis fleet paradox:** N=1 reaches ~10k/s drain; N>1 regresses to ~4.8k — shared fleet contention dominates. Not a client-CPU ceiling.

### 6.2 Backend choice

| Choose **Redis** when | Choose **NATS** when |
|------------------------|----------------------|
| Raw enqueue >>28k/s on one broker | Stream-first ops model + JetStream ecosystem |
| Drain >>5k/s single broker matters | Fleet + multibench scaling validated in-prod topology |
| Dragonfly/Redis ops already present | Need WorkQueue semantics + queue groups native |

Both adapters support fleet routing (`BOSON_REDIS_URLS` / `BOSON_NATS_URLS`). Scylla remains paused for hyperscale — see strategy doc.

### 6.3 Anti-patterns

- Using **BM-BL3 soak** (~770/s Redis) as enqueue capacity — use BM-BE4.
- **Multi-stream sharding on one broker** expecting linear K scaling — aggregate flatlines.
- **Unpinned workers** on N>1 fleet — G1 showed +157% with pinning.
- **`num_replicas > 1`** on write-heavy JetStream streams for capacity.

---

## 7. Hardware sizing guide

### Enqueue — NATS (pool-routed fleet, stream-first)

| Target R | Embed clients | Brokers | Notes |
|----------|---------------|---------|-------|
| ≤ 29k | 1 | 4 (K=4) | Single-embed fleet bc=1 |
| ~58k | 4 | 4 | Measured bc=4 multibench |
| ~330k | ~12 | 4+ | Extrapolate ~29k/embed; validate with campaign |

### Enqueue — Redis (single broker publisher)

| Target R | Brokers | Notes |
|----------|---------|-------|
| ≤ 89k | 1 | Publisher peak @ C=512 |
| > 89k | Fleet N>1 | Fleet curve not yet linear — validate per deployment |

### Drain

| Target R | Path | Measured anchor |
|----------|------|-----------------|
| ≤ 6.5k | NATS N=4 fleet W=64 | 6,472/s |
| ≤ 10.7k | NATS bc=4 multibench | 10,711/s |
| ≤ 10.5k | Redis single broker W=32 | 10,537/s |

**Worked example:** Need **58k enqueue/s** on NATS → **4 embed hosts** on **4-broker / 4-pool** fleet; measured bc=4 aggregate **58,441/s**. For **330k/s**, extrapolate ~12 embed hosts @ bc=1 (~29k/embed) — validate before production commit.

---

## 8. Limitations

- **Artifact policy:** keep harness code and methodology in Git; publish heavy JSON reports and flamegraphs as CI/release assets. Decision-grade AWS reports under `profiling/boson-bench/reports/` are ephemeral machine outputs, not durable source of truth.
- **Local smoke** is not retained under `profiling/boson-bench/reports/` and is not comparable to AWS in-VPC.
- **Invalid r2 sweep:** `boson-tier3-capacity-20260705-r2` Phase B used W=10 despite `-w1`/`-w4` filenames — excluded from all scaling curves.
- **Pre-fix Redis BD2 ~47/s** (20260705-r2) superseded by hot-path fixes and c6i.large ladder.
- **NATS KV drain** failures superseded by WorkQueue campaign (`boson-tier3-capacity-wq-20260706`).
- **Redis bc=4 multibench**, **D4 cluster drain**, **D11 c6i.large broker BD2**, **G2 drain-only multibench** — gated on 16 vCPU AWS quota.
- **E2E:** Redis catalog 26/26 pass; NATS 21/26 (split-topology WorkQueue gaps) — not capacity-gating.
- **Scylla / Tier 1–2 detail** archived in [`EXPERIMENTS-ARCHIVE.md`](EXPERIMENTS-ARCHIVE.md).

---

## Appendix — References

- [`EXPERIMENTS.md`](EXPERIMENTS.md) — experiment registry and phase status
- [`EXPERIMENTS-ARCHIVE.md`](EXPERIMENTS-ARCHIVE.md) — Scylla, postgres, campaign debug history
- `profiling/boson-bench/reports/` — JSON artifacts
- Cursor canvas `boson-bench-aws-c6i-all` — interactive ladder charts (local IDE artifact)
