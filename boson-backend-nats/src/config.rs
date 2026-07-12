//! Resolved `NATS` `WorkQueue` enqueue settings (from env at connect time).

/// `BOSON_NATS_ENQUEUE_MODE`: `dual` (default) | `stream`.
pub const ENQUEUE_MODE_ENV: &str = "BOSON_NATS_ENQUEUE_MODE";

/// `BOSON_NATS_SYNC_ACK`: `1` (default) | `0`.
pub const SYNC_ACK_ENV: &str = "BOSON_NATS_SYNC_ACK";

/// `BOSON_NATS_MAX_INFLIGHT`: max concurrent in-flight `JetStream` publishes.
pub const MAX_INFLIGHT_ENV: &str = "BOSON_NATS_MAX_INFLIGHT";

/// `BOSON_NATS_FETCH_BATCH`: pull consumer batch size on claim (default 1).
pub const FETCH_BATCH_ENV: &str = "BOSON_NATS_FETCH_BATCH";

/// `BOSON_BENCH_SKIP_CLAIM_KV`: bench-only; skip KV `save_job` on claim when `1`.
pub const SKIP_CLAIM_KV_ENV: &str = "BOSON_BENCH_SKIP_CLAIM_KV";

/// `BOSON_NATS_STREAM_REPLICAS`: `JetStream` stream `num_replicas` (default 1).
pub const STREAM_REPLICAS_ENV: &str = "BOSON_NATS_STREAM_REPLICAS";

/// How enqueue writes job data to `JetStream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnqueueMode {
    /// KV job body, then stream pointer (job id).
    #[default]
    Dual,
    /// Full job JSON in one pipelined stream publish; async KV mirror.
    Stream,
}

/// Resolved enqueue pipeline settings.
#[derive(Debug, Clone, Copy)]
pub struct NatsEnqueueConfig {
    /// KV+pointer (`Dual`) or stream-first full job publish (`Stream`).
    pub enqueue_mode: EnqueueMode,
    /// Await `JetStream` publish ack before returning from enqueue.
    pub sync_ack: bool,
    /// Max concurrent in-flight `JetStream` publishes.
    pub max_inflight: u32,
    /// When `Stream` mode: block enqueue until KV mirror completes (admin API consistency).
    pub sync_kv_mirror: bool,
}

impl NatsEnqueueConfig {
    /// Load from `BOSON_NATS_*` environment variables.
    #[must_use]
    pub fn from_env() -> Self {
        let sync_ack = sync_ack_from_env();
        Self {
            enqueue_mode: enqueue_mode_from_env(),
            sync_ack,
            max_inflight: max_inflight_from_env(sync_ack),
            sync_kv_mirror: sync_kv_mirror_from_env(),
        }
    }

    /// Explicit settings (tests).
    #[must_use]
    pub const fn new(
        enqueue_mode: EnqueueMode,
        sync_ack: bool,
        max_inflight: u32,
        sync_kv_mirror: bool,
    ) -> Self {
        Self {
            enqueue_mode,
            sync_ack,
            max_inflight: if max_inflight == 0 { 1 } else { max_inflight },
            sync_kv_mirror,
        }
    }
}

fn enqueue_mode_from_env() -> EnqueueMode {
    match std::env::var(ENQUEUE_MODE_ENV)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "stream" | "fast" | "firehose" => EnqueueMode::Stream,
        _ => EnqueueMode::Dual,
    }
}

fn sync_ack_from_env() -> bool {
    !matches!(
        std::env::var(SYNC_ACK_ENV)
            .unwrap_or_else(|_| "1".into())
            .as_str(),
        "0" | "false" | "off" | "no"
    )
}

fn max_inflight_from_env(sync_ack: bool) -> u32 {
    if let Ok(raw) = std::env::var(MAX_INFLIGHT_ENV) {
        if let Ok(n) = raw.parse::<u32>() {
            return n.max(1);
        }
    }
    if sync_ack { 256 } else { 512 }
}

fn sync_kv_mirror_from_env() -> bool {
    matches!(
        std::env::var("BOSON_NATS_STREAM_SYNC_KV").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

/// Pull consumer `max_messages` for claim path (bench tuning, default 1).
#[must_use]
pub fn fetch_batch_from_env() -> usize {
    std::env::var(FETCH_BATCH_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1)
}

/// When true, skip KV job mirror on claim (bench A/B only).
#[must_use]
pub fn skip_claim_kv_from_env() -> bool {
    matches!(
        std::env::var(SKIP_CLAIM_KV_ENV).ok().as_deref(),
        Some(v) if matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    )
}

/// `JetStream` `WorkQueue` stream replica count (default 1).
#[must_use]
pub fn stream_replicas_from_env() -> usize {
    std::env::var(STREAM_REPLICAS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_caps_inflight_at_one() {
        let cfg = NatsEnqueueConfig::new(EnqueueMode::Stream, true, 0, false);
        assert_eq!(cfg.max_inflight, 1);
    }
}
