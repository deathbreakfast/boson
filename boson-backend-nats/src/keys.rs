//! Key paths for `JetStream` KV (mirrors Redis layout).

/// Namespaced KV keys for one backend instance.
#[derive(Debug, Clone)]
pub struct Keyspace {
    prefix: String,
}

impl Keyspace {
    /// Default prefix (`boson`) or `BOSON_NATS_KEY_PREFIX` when set.
    #[must_use]
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("BOSON_NATS_KEY_PREFIX").unwrap_or_else(|_| "boson".into()),
        )
    }

    /// Explicit prefix (isolated tests).
    #[must_use]
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: sanitize_kv_token(prefix.into()),
        }
    }

    /// Unique KV token prefix for one e2e run.
    #[must_use]
    pub fn isolated_prefix(base: &str) -> String {
        sanitize_kv_token(format!("{base}_{}", uuid::Uuid::new_v4().simple()))
    }

    /// `JetStream` KV bucket name (alphanumeric, max 255).
    #[must_use]
    pub fn bucket(&self) -> String {
        format!("{}_kv", self.prefix.replace(':', "_"))
    }

    /// Namespace prefix.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// `{prefix}.job.*` key prefix for listing jobs.
    #[must_use]
    pub fn job_prefix(&self) -> String {
        format!("{}.job.", self.prefix)
    }

    /// `{prefix}.run.*` key prefix for listing runs.
    #[must_use]
    pub fn run_prefix(&self) -> String {
        format!("{}.run.", self.prefix)
    }

    /// `{prefix}.lease.*` key prefix for lease rows.
    #[must_use]
    pub fn lease_prefix(&self) -> String {
        format!("{}.lease.", self.prefix)
    }

    /// `{prefix}.pool.*` key prefix for pool markers.
    #[must_use]
    pub fn pool_prefix(&self) -> String {
        format!("{}.pool.", self.prefix)
    }

    /// Prefix for all keys in this namespace (`{prefix}.`).
    #[must_use]
    pub fn namespace_prefix(&self) -> String {
        format!("{}.", self.prefix)
    }

    /// Job body JSON: `{prefix}.job.{job_id}`.
    #[must_use]
    pub fn job(&self, id: &str) -> String {
        format!("{}.job.{}", self.prefix, sanitize_kv_token(id))
    }

    /// Ready queue entry: `{prefix}.ready.{pool}.{priority}.{created_at_ms}.{job_id}`.
    #[must_use]
    pub fn ready(&self, pool: &str, priority: i32, created_at_ms: i64, job_id: &str) -> String {
        format!(
            "{}.ready.{}.{:010}.{:020}.{}",
            self.prefix,
            sanitize_kv_token(pool),
            priority,
            created_at_ms,
            sanitize_kv_token(job_id)
        )
    }

    /// Ready key prefix for one pool.
    #[must_use]
    pub fn ready_prefix(&self, pool: &str) -> String {
        format!("{}.ready.{}.", self.prefix, sanitize_kv_token(pool))
    }

    /// Idempotency key mapping: `{prefix}.idem.{key}`.
    #[must_use]
    pub fn idempotency(&self, key: &str) -> String {
        format!("{}.idem.{}", self.prefix, sanitize_kv_token(key))
    }

    /// Run JSON: `{prefix}.run.{run_id}`.
    #[must_use]
    pub fn run(&self, id: &str) -> String {
        format!("{}.run.{}", self.prefix, sanitize_kv_token(id))
    }

    /// Task config JSON: `{prefix}.taskcfg.{name}`.
    #[must_use]
    pub fn task_config(&self, name: &str) -> String {
        format!("{}.taskcfg.{}", self.prefix, sanitize_kv_token(name))
    }

    /// Lease JSON: `{prefix}.lease.{lease_id}`.
    #[must_use]
    pub fn lease(&self, id: &str) -> String {
        format!("{}.lease.{}", self.prefix, sanitize_kv_token(id))
    }

    /// Job id index for lease lookup: `{prefix}.lease_by_job.{job_id}`.
    #[must_use]
    pub fn lease_by_job(&self, job_id: &str) -> String {
        format!(
            "{}.lease_by_job.{}",
            self.prefix,
            sanitize_kv_token(job_id)
        )
    }

    /// Pool marker key: `{prefix}.pool.{pool}`.
    #[must_use]
    pub fn pool_marker(&self, pool: &str) -> String {
        format!("{}.pool.{}", self.prefix, sanitize_kv_token(pool))
    }
}

/// Normalize a token for `JetStream` KV keys (alphanumeric, `-`, `_`, `/`, `=`, `.`).
fn sanitize_kv_token(value: impl Into<String>) -> String {
    value
        .into()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '/' | '=' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
