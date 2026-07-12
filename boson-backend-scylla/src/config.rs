//! Connection and tuning settings for [`crate::ScyllaQueueBackend`].

/// Connection settings for [`crate::ScyllaQueueBackend`].
#[derive(Debug, Clone)]
pub struct ScyllaQueueConfig {
    /// Contact points (`host:port`). Driver discovers full topology.
    pub contact_points: Vec<String>,
    /// CQL keyspace for Boson tables.
    pub keyspace: String,
    /// Optional datacenter for DC-aware routing.
    pub datacenter: Option<String>,
    /// Optional username.
    pub username: Option<String>,
    /// Optional password.
    pub password: Option<String>,
    /// Keyspace replication factor for schema bootstrap.
    pub replication_factor: u32,
    /// Ready-queue shard count (partition key spread within a pool). Default **256**.
    pub ready_shard_count: u32,
    /// Max in-flight ready-shard SELECTs during claim (default **32**).
    pub shard_concurrency: u32,
    /// Issue independent writes in parallel where safe (default **true**).
    pub parallel_writes: bool,
    /// Optional driver pool size per shard (`PoolSize::PerShard(n)`).
    pub pool_per_shard: Option<u32>,
}

impl Default for ScyllaQueueConfig {
    fn default() -> Self {
        Self {
            contact_points: vec!["127.0.0.1:9042".into()],
            keyspace: "boson".into(),
            datacenter: None,
            username: None,
            password: None,
            replication_factor: 1,
            ready_shard_count: 256,
            shard_concurrency: 32,
            parallel_writes: true,
            pool_per_shard: None,
        }
    }
}

/// Stable shard id for a job id in `0..shard_count`.
#[must_use]
pub fn shard_for_job(job_id: &str, shard_count: u32) -> i32 {
    let n = shard_count.max(1);
    let mut h: u32 = 2_166_136_261;
    for b in job_id.as_bytes() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(16_777_619);
    }
    i32::try_from(h % n).unwrap_or(0)
}

/// Expiry bucket for lease reaper (`expires_at` unix secs / 60).
#[must_use]
pub fn expiry_bucket(expires_at_secs: i64) -> i32 {
    i32::try_from(expires_at_secs.div_euclid(60)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_in_range() {
        let s = shard_for_job("job-abc", 256);
        assert!((0..256).contains(&s));
    }

    #[test]
    fn expiry_bucket_minute() {
        assert_eq!(expiry_bucket(120), 2);
        assert_eq!(expiry_bucket(119), 1);
    }
}
