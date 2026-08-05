# Boson performance

Measured on AWS (`c6i.large` bench hosts with `t3.medium` brokers unless noted). Boson is a typed job queue with pluggable backends. The figures below cover **Redis** and **NATS JetStream WorkQueue** enqueue and drain paths applications use in production topologies. Full ladders live in the private `uf-live-cloud-lab` boson performance study.

## Enqueue

On a 4-broker pool-routed fleet (`isolated-lab`, prefill 10k): Redis publisher peak **89,335 ops/s** (K=1, C=512); NATS stream-first **28,299 ops/s** @ C=256. NATS multibench aggregate reaches **~58k ops/s** @ bc=4 before embed count binds.

## Drain

Redis single-broker drain **~10.5k ops/s** @ W=32. NATS single-broker **~4.5k ops/s** @ W=64; multibench aggregate **~10.7k ops/s** @ bc=4. Plan fill÷drain gaps of about **6× (NATS)** and **8–10× (Redis)** when sizing consumer fleets.

## Guidance

Redis leads raw enqueue and single-broker drain. NATS clears a high enqueue gate and shows validated multi-publisher scaling. Choose by topology (ops familiarity, multi-tenant stream needs, drain shape), not by a single headline number.

## How to read these results

Quote AWS-tagged hardware. Treat older curves taken before claim/lease hardening as historical until re-measured on current code.
