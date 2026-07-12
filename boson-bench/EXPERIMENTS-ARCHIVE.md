# Boson benchmark archive

Historical campaign detail for **paused or superseded** backends and **debug narratives**. Not used for Tier 3 decisions — see [`PERFORMANCE_STUDY.md`](PERFORMANCE_STUDY.md) and [`EXPERIMENTS.md`](EXPERIMENTS.md).

---

## Tier 1–2 baseline (`boson-baseline-t3m-20260703`)

Topology: 1× t3.medium per backend, `isolated-lab`, telemetry off. Postgres: colocated Docker `postgres:16-alpine` on bench host.

Reports: `profiling/boson-bench/reports/*-{mem,sqlite,postgres}-*-aws-t3-medium.json`.

### Enqueue latency (BM-B0)

| Backend | p50 (ms) | p99 (ms) |
|---------|----------|----------|
| mem | 0.003 | 0.008 |
| sqlite | 0.318 | 0.549 |
| postgres | 2.217 | 2.745 |

### Sustained load (BM-BL*)

| Backend | BL3 peak | CPU @ peak |
|---------|----------|------------|
| mem | ~5.8k/s | ~198% |
| sqlite | ~1.4k/s | ~144% |
| postgres | ~328/s | ~30% |

Postgres ceiling is colocated Docker bound (~2.5ms RTT), not CPU-saturated. Tier 2 promotion stands on schema fit, not this throughput number.

---

## Scylla campaigns (`boson-scylla-1-20260703`)

**Status:** Paused for hyperscale — see [`PERFORMANCE_STUDY.md`](PERFORMANCE_STUDY.md).

Topology: 1× bench + 1× Scylla t3.medium (`scylladb/scylla:6.2`, `--smp 1 --memory 750M`).

### Baseline (pre–Track C)

| Metric | Value |
|--------|-------|
| BM-B0 p50/p99 | 1.378 / 1.883 ms |
| BM-BL3 paced | ~540–590/s (cpu 60–80%) |

### Track C — adapter concurrency (`boson-scylla-1-track-c-20260703`)

Levers: `SHARD_CONCURRENCY=32`, parallel writes, run-start upsert skip, `noop_0..noop_{K-1}` registration, `POOL_PER_SHARD=1`.

| ID | Baseline | Track C | Delta |
|----|----------|---------|-------|
| BM-C1 (BL1) | 590/s | **893/s** | +51% |
| BM-C2 (BL3) | 542/s | **892/s** | +65% |
| BM-C3 (BM3) | 3224/s | **4231/s** | +31% |
| BM-C4 (BM4) | FAIL err≈99.6% | **8650/s PASS** | registration fix |
| BM-C5 none/lwt | 564/156/s | **875/175/s** | none/lwt **5.0×** |

Reports: `profiling/boson-bench/reports/*-track-c*.json`.

**Decision:** Track C raised paced throughput ~65% while lowering BL3 CPU ~80% → ~47%. Still below sqlite (~1.4k/s); Scylla-as-hyperscale paused without hot-path redesign.

### Other Scylla tracks

| Track | ID | Result |
|-------|-----|--------|
| Hot pool | BM-BM3 (1 shard) | 3224/s baseline → 4231/s Track C |
| Shard spread | BM-BM4 | FAIL pre–Track C (missing task registration); PASS 8650/s Track C |
| Idempotency | BM-BI1 | lwt 156/s vs none 564/s (3.6×) |
| Task fan-out | BM-BF2 | T=1 1879/s, T=64 2715/s |

### Scylla cloud run

```bash
export BOSON_NATIVE_CAMPAIGN=boson-scylla-1-$(date -u +%Y%m%d)
export BOSON_NATIVE_MANIFEST=boson-scylla-1
infra/native-aws/scripts/provision-scylla-1.sh
infra/native-aws/scripts/run-scylla-lab.sh
infra/native-aws/scripts/teardown-fleet.sh boson-scylla-1
```

Cost reference (t3.medium $0.0416/hr): at 892/s BL3, ~**$0.026/M ops** (bench + Scylla).

---

## Tier 3 campaign debug history

### Invalid r2 worker sweep (`boson-tier3-capacity-20260705-r2`)

Phase B `-w1`/`-w4` reports used **W=10** despite filenames — `BOSON_BENCH_WORKER_COUNT=10` overrode CLI. Fixed: CLI wins; Phase B unsets env in `run-broker-lab.sh`. **Exclude from scaling analysis.**

### Pre-fix Redis BD2 (~47/s)

Campaign r2 Phase A reported BD2 W=10 at **~47/s** before hot-path fixes (`pop_claim`, `BOSON_SKIP_RUN_ROWS`, pool pin). Superseded by `boson-tier3-capacity-sweep-20260706` (647–1394/s W=1–4) and aws-c6i.large BD2 ladder.

### NATS KV drain failures

`boson-tier3-capacity-20260705-r2` Phase A: BD1/BD2 **FAIL** on NATS KV at N=10k prefill. Superseded by WorkQueue campaign `boson-tier3-capacity-wq-20260706` (BD2 **2412/s** W=10).

### Local NATS smoke (2026-07-06) — not retained

Historical local BE4 stream ~33k–40k/s and publisher grid ~38k/s @ C=512 were exploratory only. AWS gate on c6i.large: **28,299/s**. Local report artifacts removed; decision-grade curves are AWS-only.

### Claim-path local smoke (Phase F) — not retained

Local @ W=16 showed skip-KV ~1.44×; AWS @ W=16: skip-KV **3,323/s** vs **2,990/s** (1.11×) — below 2× production bar. Local reports removed.

### Phase F AWS numbers (reference)

| Knob | drain ops/s |
|------|-------------|
| batch=1 baseline | 2,990 (skip-KV off) |
| skip-KV on | 3,323 |

D7 batch=1 cited as **2,694/s** in prior campaign log — use report files `bm-bd2-skipkv*-b1-w16-nats-*.json` for cell-level values.

---

## Continuum prior art (do not re-run)

| Topic | Continuum reference |
|-------|---------------------|
| Raw engine spread-key | ~14.8k/s (1n) / ~29.5k (2n) t3.medium |
| Continuum+Scylla M4 id-on | ~3.1–3.3k/s |
| Continuum hot-stream | ~68–184/s (partition-bound) |

Full registry: [Continuum EXPERIMENTS](https://github.com/unified-field-dev/continuum/blob/main/continuum-bench/EXPERIMENTS.md).

---

## E2E parity notes (not capacity-gating)

| Layer | NATS | Redis |
|-------|------|-------|
| Catalog matrix | 21/26 pass | 26/26 pass |
| Known NATS gaps | `*_split` WorkQueue scenarios; 3 workqueue contract failures | — |

See adapter tests and `run-nats-e2e.sh` / `run-redis-e2e.sh`.
