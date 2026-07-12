//! Test harness macros for adapter contract suites and the e2e scenario matrix.

/// Expand every [`backend_contract`](crate::backend_contract) check for one adapter.
///
/// `$setup` must be an async function `() -> Option<Arc<dyn QueueBackend>>`.
/// When it returns `None`, the test skips (service env unset).
///
/// # Examples
///
/// Always-on (sqlite):
///
/// ```ignore
/// async fn fresh() -> Option<Arc<dyn QueueBackend>> { Some(...) }
/// boson_testkit::backend_contract_suite!(fresh, "sqlite");
/// ```
///
/// Service-gated (postgres / scylla):
///
/// ```ignore
/// boson_testkit::backend_contract_suite!(
///     fresh,
///     "postgres",
///     ignore = "requires BOSON_TEST_POSTGRES_URL — run with --include-ignored"
/// );
/// ```
#[macro_export]
macro_rules! backend_contract_suite {
    ($setup:ident, $label:literal) => {
        $crate::__backend_contract_one!($setup, $label, enqueue_inserts_and_lists);
        $crate::__backend_contract_one!($setup, $label, idempotency_reuses_nonterminal);
        $crate::__backend_contract_one!($setup, $label, try_claim_atomic);
        $crate::__backend_contract_one!($setup, $label, pool_priority_order);
        $crate::__backend_contract_one!($setup, $label, max_in_flight_rate_limit);
        $crate::__backend_contract_one!($setup, $label, max_enqueue_per_second);
        $crate::__backend_contract_one!($setup, $label, run_lifecycle);
        $crate::__backend_contract_one!($setup, $label, global_router_resolves);
        $crate::__backend_contract_one!($setup, $label, lease_contention);
        $crate::__backend_contract_one!($setup, $label, extend_lease_refreshes_ttl);
        $crate::__backend_contract_one!($setup, $label, expired_lease_pairs);
    };
    ($setup:ident, $label:literal, ignore = $ignore_msg:literal) => {
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, enqueue_inserts_and_lists);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, idempotency_reuses_nonterminal);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, try_claim_atomic);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, pool_priority_order);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, max_in_flight_rate_limit);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, max_enqueue_per_second);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, run_lifecycle);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, global_router_resolves);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, lease_contention);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, extend_lease_refreshes_ttl);
        $crate::__backend_contract_one_ignored!($setup, $label, $ignore_msg, expired_lease_pairs);
    };
}

/// Internal: one always-on `contract_*` test.
#[macro_export]
#[doc(hidden)]
macro_rules! __backend_contract_one {
    ($setup:ident, $label:literal, $contract:ident) => {
        $crate::__paste::paste! {
            #[tokio::test]
            async fn [<contract_ $contract>]() {
                let Some(backend) = $setup().await else {
                    eprintln!(
                        "backend contract {}/{}: setup returned None — skipping",
                        $label,
                        stringify!($contract)
                    );
                    return;
                };
                let env = $crate::BackendEnv::new($label);
                $crate::backend_contract::$contract(backend, &env).await;
            }
        }
    };
}

/// Internal: one ignored `contract_*` test.
#[macro_export]
#[doc(hidden)]
macro_rules! __backend_contract_one_ignored {
    ($setup:ident, $label:literal, $ignore_msg:literal, $contract:ident) => {
        $crate::__paste::paste! {
            #[tokio::test]
            #[ignore = $ignore_msg]
            async fn [<contract_ $contract>]() {
                let Some(backend) = $setup().await else {
                    eprintln!(
                        "backend contract {}/{}: setup returned None — skipping",
                        $label,
                        stringify!($contract)
                    );
                    return;
                };
                let env = $crate::BackendEnv::new($label);
                $crate::backend_contract::$contract(backend, &env).await;
            }
        }
    };
}

/// Expand the full correctness catalog for every backend in [`e2e_storage_backends`](crate::e2e_storage_backends).
///
/// Emits `{entry_id}_{backend}` tests. Mem/sqlite use full-matrix ignore; postgres/scylla use
/// service-env ignore messages. All backends **skip** at runtime when the service env is unset.
#[macro_export]
macro_rules! matrix_scenario_suite {
    () => {
        $crate::matrix_scenario_suite!(@all
            enqueue_and_drain,
            enqueue_only,
            multi_job_drain,
            enqueue_unknown_task,
            rate_limit_in_flight,
            rate_limit_eps,
            task_config_rate_limit,
            idempotency_smoke,
            idempotency_reuse_while_queued,
            idempotency_after_terminal,
            idempotency_none_allows_dup,
            run_lifecycle,
            handler_failure_terminal,
            retry_then_success,
            retry_exhaustion,
            cancel_queued_job,
            cancel_missing_job,
            restart_runtime_drain,
            pool_priority_drain,
            list_and_count_jobs,
            get_job_not_found,
            enqueue_and_drain_split,
            lease_contention_drain,
            multi_job_drain_split,
            restart_runtime_drain_split,
            enqueue_and_drain_console,
            task_run_stats,
            list_jobs_pagination,
            retry_run_count,
            signature_mismatch,
        );
    };
    (@all $($entry:ident),+ $(,)?) => {
        $(
            $crate::matrix_one_catalog_entry!($entry);
        )+
    };
}

/// Expand the PR smoke catalog (mem + sqlite active; postgres/scylla ignored).
#[macro_export]
macro_rules! matrix_smoke_suite {
    () => {
        $crate::matrix_smoke_suite!(@all
            enqueue_and_drain,
            enqueue_only,
            run_lifecycle,
            idempotency_smoke,
        );
    };
    (@all $($entry:ident),+ $(,)?) => {
        $(
            $crate::matrix_one_smoke_entry!($entry);
        )+
    };
}

/// Internal: one catalog entry × all e2e backends (full matrix, all ignored on PR).
#[macro_export]
#[doc(hidden)]
macro_rules! matrix_one_catalog_entry {
    ($entry:ident) => {
        $crate::__paste::paste! {
            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "full-matrix — run with --include-ignored"]
            async fn [<$entry _mem>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Mem,
                    stringify!($entry),
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "full-matrix — run with --include-ignored"]
            async fn [<$entry _sqlite>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Sqlite,
                    stringify!($entry),
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires BOSON_TEST_POSTGRES_URL — run with --include-ignored"]
            async fn [<$entry _postgres>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Postgres,
                    stringify!($entry),
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires BOSON_TEST_SCYLLA_CONTACT_POINTS — run with --include-ignored"]
            async fn [<$entry _scylla>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Scylla,
                    stringify!($entry),
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires BOSON_TEST_REDIS_URL — run with --include-ignored"]
            async fn [<$entry _redis>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Redis,
                    stringify!($entry),
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires BOSON_TEST_NATS_URL — run with --include-ignored"]
            async fn [<$entry _nats>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Nats,
                    stringify!($entry),
                )
                .await;
            }
        }
    };
}

/// Internal: smoke entry — mem/sqlite active, postgres/scylla ignored.
#[macro_export]
#[doc(hidden)]
macro_rules! matrix_one_smoke_entry {
    ($entry:ident) => {
        $crate::__paste::paste! {
            #[tokio::test(flavor = "multi_thread")]
            async fn [<$entry _mem>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Mem,
                    stringify!($entry),
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn [<$entry _sqlite>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Sqlite,
                    stringify!($entry),
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires BOSON_TEST_POSTGRES_URL — run with --include-ignored"]
            async fn [<$entry _postgres>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Postgres,
                    stringify!($entry),
                )
                .await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires BOSON_TEST_SCYLLA_CONTACT_POINTS — run with --include-ignored"]
            async fn [<$entry _scylla>]() {
                $crate::run_named_catalog_entry(
                    $crate::BackendAdapter::Scylla,
                    stringify!($entry),
                )
                .await;
            }
        }
    };
}
