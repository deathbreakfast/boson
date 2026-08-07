# Boson broker lab scripts

Local NATS/Redis sweeps for Tier 3 capacity studies (no AWS required).

## BM-BE4 publisher sweep (JetStream single-stream saturation)

Find peak enqueue ops/s vs concurrent publisher count before adding WorkQueue stream sharding.

```bash
# Smoke (C ∈ {1, 8, 32, 64}), single stream K=1 shared
bash infra/broker/scripts/run-be4-sweep.sh

# Full grid including C=128,256,512
BOSON_BE4_SWEEP_SMOKE=0 bash infra/broker/scripts/run-be4-sweep.sh

# Multi-stream BE4 shape (K=10 distinct pools)
BOSON_BE4_SWEEP_POOLS=distinct bash infra/broker/scripts/run-be4-sweep.sh
```

**Env:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `BOSON_BE4_SWEEP_SMOKE` | `1` | Reduced publisher grid |
| `BOSON_BE4_SWEEP_POOLS` | `single` | `single` = K=1 one stream; `distinct` = K=10 |
| `BOSON_NATS_ENQUEUE_MODE` | `stream` | Stream-first enqueue |
| `BOSON_NATS_SYNC_ACK` | `0` | Async publish ack for peak throughput |
| `BOSON_NATS_MAX_INFLIGHT` | `256` | Publish pipeline depth |
| `BOSON_BENCH_CMD` | `cargo run -p boson-bench --release --` | Bench binary |
| `BOSON_TEST_NATS_URL` | `nats://127.0.0.1:4222` | NATS server |
| `BOSON_BENCH_HARDWARE` | `local` | Hardware slug (smoke only) |
| `BOSON_BENCH_REPORTS` | `profiling/boson-bench/smoke` | Output dir (gitignored; AWS reports stay under `reports/`) |

**Output (smoke):** `profiling/boson-bench/smoke/bm-be4-c{C}-nats-*.json` and  
`scaling-curve-be4-publishers-{hardware}-nats-{k1-shared|k10-distinct}.json`

**Aggregate curve (retained AWS reports):**

```bash
cargo run -p boson-bench -- be4-publisher-curve \
  --hardware aws-c6i-large --backend nats \
  --reports-dir profiling/boson-bench/reports
```

Compare to Photon BM-PFH for raw firehose ingress vs Boson job-queue enqueue.

## AWS campaigns

Maintainers also run c6i.large broker sizing, RAFT cluster comparisons, multi-embed fleet
sweeps, and Phase D dequeue campaigns on AWS. Those scripts are not shipped in this
repository. Aggregate retained reports with the `*-curve` commands below (and in BM-BD2).

N=4 fleet shapes need **≥10 vCPU** free (1× c6i.large + 4× t3.medium). Account default
limit is 16 vCPU — terminate other campaigns first.

Local smoke (2 Docker NATS):

```bash
bash infra/broker/scripts/run-be4-fleet-sweep.sh
```

Fleet curve:

```bash
cargo run -p boson-bench -- be4-fleet-curve --hardware aws-c6i-large --backend nats
```

## BM-BE4 shard sweep (multi-stream scaling)

Fixed C=256, K ∈ {1, 4, 10, 32}. Run cells locally or on AWS.

**Aggregate curve:**

```bash
cargo run -p boson-bench -- be4-shard-curve \
  --hardware aws-c6i-large --backend nats \
  --reports-dir profiling/boson-bench/reports
```

## BM-BD2 drain sweep (dequeue capacity)

Find peak drain ops/s vs worker count (W), pool shards (K), broker fleet (N), and multi-embed (bc). Analogous to BE4 enqueue ladder.

```bash
# Local smoke (W ∈ {1,4,10} or full W grid)
bash infra/broker/scripts/run-bd2-sweep.sh
BOSON_BD2_SWEEP_SMOKE=0 bash infra/broker/scripts/run-bd2-sweep.sh

# K shard sweep @ W=10 (smoke: K=1,4)
BOSON_BD2_SWEEP_PHASE=shard BOSON_BD2_WORKER_COUNT=10 bash infra/broker/scripts/run-bd2-sweep.sh
```

**Env:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `BOSON_BD2_SWEEP_SMOKE` | `1` | Reduced W/K grid |
| `BOSON_BD2_SWEEP_PHASE` | `worker` | `worker` or `shard` |
| `BOSON_BD2_WORKER_COUNT` | `32` | W for shard/fleet phases |
| `BOSON_BENCH_PREFILL_COUNT` | `10000` | Prefill before drain |
| `BOSON_BENCH_CMD` | `cargo run -p boson-bench --release --` | Bench binary |
| `BOSON_BENCH_HARDWARE` | `local` | Hardware slug (smoke only) |
| `BOSON_BENCH_REPORTS` | `profiling/boson-bench/smoke` | Output dir (gitignored) |

**Output (smoke):** `profiling/boson-bench/smoke/bm-bd2-w{W}-*.json`, `scaling-curve-bd2-workers-{hardware}-nats.json`

**Aggregate curves (retained AWS reports):**

```bash
cargo run -p boson-bench -- bd2-worker-curve --hardware aws-c6i-large --backend nats
cargo run -p boson-bench -- bd2-shard-curve --hardware aws-c6i-large --backend nats
cargo run -p boson-bench -- bd2-fleet-curve --hardware aws-c6i-large --backend nats
cargo run -p boson-bench -- bd2-multibench-curve --hardware aws-c6i-large --backend nats
```
