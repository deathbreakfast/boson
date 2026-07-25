//! HTTP list limit clamping.

/// Default `limit` when the query omits it.
pub const DEFAULT_LIST_LIMIT: usize = 100;

/// Hard maximum rows returned by list endpoints.
pub const MAX_LIST_LIMIT: usize = 500;

/// Clamp an optional list limit into `[1, MAX_LIST_LIMIT]` (default [`DEFAULT_LIST_LIMIT`]).
#[must_use]
pub fn clamp_list_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT)
}

/// Maximum `RetryPolicy::max_attempts` accepted via HTTP config upsert.
pub const MAX_RETRY_ATTEMPTS: u32 = 100;

/// Minimum `base_delay_ms` / `max_delay_ms` via HTTP (milliseconds).
pub const MIN_RETRY_DELAY_MS: u64 = 10;

/// Maximum retry delay via HTTP (milliseconds).
pub const MAX_RETRY_DELAY_MS: u64 = 86_400_000;

/// Validate and clamp retry policy fields for HTTP upsert.
///
/// # Errors
///
/// Returns a static message when values are out of range after clamping checks.
pub fn clamp_retry_policy(
    mut policy: boson_core::RetryPolicy,
) -> Result<boson_core::RetryPolicy, &'static str> {
    if policy.max_attempts > MAX_RETRY_ATTEMPTS {
        return Err("max_attempts exceeds allowed maximum");
    }
    if policy.base_delay_ms > MAX_RETRY_DELAY_MS || policy.max_delay_ms > MAX_RETRY_DELAY_MS {
        return Err("retry delay exceeds allowed maximum");
    }
    if policy.backoff_multiplier < 1.0 || policy.backoff_multiplier > 100.0 {
        return Err("backoff_multiplier out of range");
    }
    policy.base_delay_ms = policy.base_delay_ms.max(MIN_RETRY_DELAY_MS);
    policy.max_delay_ms = policy.max_delay_ms.max(policy.base_delay_ms);
    Ok(policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_huge_limit() {
        assert_eq!(clamp_list_limit(Some(1_000_000)), MAX_LIST_LIMIT);
    }

    #[test]
    fn default_limit() {
        assert_eq!(clamp_list_limit(None), DEFAULT_LIST_LIMIT);
    }

    #[test]
    fn rejects_huge_max_attempts() {
        let p = boson_core::RetryPolicy {
            max_attempts: 10_000,
            base_delay_ms: 100,
            backoff_multiplier: 2.0,
            max_delay_ms: 1000,
        };
        assert!(clamp_retry_policy(p).is_err());
    }
}
