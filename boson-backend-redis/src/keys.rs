//! Redis key naming for the Boson queue adapter.

/// Namespaced Redis keys for one backend instance.
#[derive(Debug, Clone)]
pub struct Keyspace {
    prefix: String,
}

impl Keyspace {
    /// Default prefix (`boson`) or `BOSON_REDIS_KEY_PREFIX` when set.
    #[must_use]
    pub fn from_env() -> Self {
        Self::new(std::env::var("BOSON_REDIS_KEY_PREFIX").unwrap_or_else(|_| "boson".into()))
    }

    /// Explicit prefix (isolated tests / multi-tenant).
    #[must_use]
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Unique prefix for one e2e / contract run (`{prefix}_{uuid}`).
    #[must_use]
    pub fn isolated(prefix: &str) -> Self {
        Self::new(format!("{prefix}_{}", uuid::Uuid::new_v4().simple()))
    }

    /// Shared bench prefix when `BOSON_REDIS_KEY_PREFIX` is set; otherwise a unique e2e prefix.
    #[must_use]
    pub fn from_env_or_isolated() -> Self {
        if std::env::var("BOSON_REDIS_KEY_PREFIX").is_ok() {
            Self::from_env()
        } else {
            Self::isolated("boson_e2e")
        }
    }

    /// `{prefix}:job:*` SCAN pattern.
    #[must_use]
    pub fn job_pattern(&self) -> String {
        format!("{}:job:*", self.prefix)
    }

    /// `{prefix}:run:*` SCAN pattern.
    #[must_use]
    pub fn run_pattern(&self) -> String {
        format!("{}:run:*", self.prefix)
    }

    /// `{prefix}:lease:*` SCAN pattern (lease rows only).
    #[must_use]
    pub fn lease_pattern(&self) -> String {
        format!("{}:lease:*", self.prefix)
    }

    /// SCAN match pattern for all keys in this namespace.
    #[must_use]
    pub fn scan_pattern(&self) -> String {
        format!("{}:*", self.prefix)
    }

    /// Job body JSON: `{prefix}:job:{job_id}`.
    #[must_use]
    pub fn job(&self, id: &str) -> String {
        format!("{}:job:{}", self.prefix, id)
    }

    /// Prefix for `{prefix}:job:` keys (Lua pop-claim).
    #[must_use]
    pub fn job_key_prefix(&self) -> String {
        format!("{}:job:", self.prefix)
    }

    /// Ready ZSET for a pool: `{prefix}:ready:{pool}`.
    #[must_use]
    pub fn ready(&self, pool: &str) -> String {
        format!("{}:ready:{}", self.prefix, pool)
    }

    /// Idempotency key mapping: `{prefix}:idem:{key}`.
    #[must_use]
    pub fn idempotency(&self, key: &str) -> String {
        format!("{}:idem:{}", self.prefix, key)
    }

    /// Run JSON: `{prefix}:run:{run_id}`.
    #[must_use]
    pub fn run(&self, id: &str) -> String {
        format!("{}:run:{}", self.prefix, id)
    }

    /// Task config JSON: `{prefix}:taskcfg:{name}`.
    #[must_use]
    pub fn task_config(&self, name: &str) -> String {
        format!("{}:taskcfg:{}", self.prefix, name)
    }

    /// Lease JSON: `{prefix}:lease:{lease_id}`.
    #[must_use]
    pub fn lease(&self, id: &str) -> String {
        format!("{}:lease:{}", self.prefix, id)
    }

    /// Job id index for lease lookup: `{prefix}:lease-by-job:{job_id}`.
    #[must_use]
    pub fn lease_by_job(&self, job_id: &str) -> String {
        format!("{}:lease-by-job:{}", self.prefix, job_id)
    }

    /// Set of pools with queued work: `{prefix}:pools`.
    #[must_use]
    pub fn pools_set(&self) -> String {
        format!("{}:pools", self.prefix)
    }
}

/// ZSET score: lower priority integer first, then older jobs first.
#[must_use]
#[allow(clippy::cast_precision_loss)] // Redis ZSET scores are f64; priority×1e15+ms is intentional.
pub fn ready_score(priority: i32, created_at_ms: i64) -> f64 {
    (i64::from(priority) as f64).mul_add(1e15, created_at_ms as f64)
}

#[cfg(test)]
mod tests {
    use super::Keyspace;

    #[test]
    fn isolated_prefixes_are_unique() {
        let a = Keyspace::isolated("boson_e2e");
        let b = Keyspace::isolated("boson_e2e");
        assert_ne!(a.job("1"), b.job("1"));
    }

    #[test]
    fn from_env_or_isolated_shares_prefix_when_set() {
        let prefix = format!("bm-bc1-test-{}", std::process::id());
        std::env::set_var("BOSON_REDIS_KEY_PREFIX", &prefix);
        let a = Keyspace::from_env_or_isolated();
        let b = Keyspace::from_env_or_isolated();
        std::env::remove_var("BOSON_REDIS_KEY_PREFIX");
        assert_eq!(a.job("x"), b.job("x"));
        assert!(a.job("x").starts_with(&format!("{prefix}:job:")));
    }

    #[test]
    fn from_env_or_isolated_is_unique_without_prefix() {
        std::env::remove_var("BOSON_REDIS_KEY_PREFIX");
        let a = Keyspace::from_env_or_isolated();
        let b = Keyspace::from_env_or_isolated();
        assert_ne!(a.job("1"), b.job("1"));
    }
}
