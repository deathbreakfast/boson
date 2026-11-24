//! Shared [`QueueBackend`] contract assertions.

use std::sync::Arc;

use boson_core::{
    BosonError, Job, JobEnqueueDisposition, JobFilter, JobKey, JobStatus, QueueBackend,
    RateLimitPolicy, Run, RunFilter, RunKey, RunStatus, TaskConfig,
};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;

/// Fixture environment for backend contract tests.
pub struct BackendEnv {
    /// Optional label for debugging.
    pub label: &'static str,
}

impl BackendEnv {
    /// New fixture env.
    #[must_use]
    pub const fn new(label: &'static str) -> Self {
        Self { label }
    }
}

fn task_config(task_name: &str) -> TaskConfig {
    TaskConfig::default_for(task_name)
}

fn task_config_with_rate_limit(task_name: &str, max_in_flight: u32, max_eps: u32) -> TaskConfig {
    let mut config = TaskConfig::default_for(task_name);
    config.rate_limit_policy = RateLimitPolicy {
        max_in_flight,
        max_enqueue_per_second: max_eps,
    };
    config
}

fn sample_job(
    task_name: &str,
    pool: &str,
    priority: i32,
    idempotency_key: Option<&str>,
) -> boson_core::Job {
    boson_core::Job::new(
        task_name,
        json!({"label": "test"}),
        json!({}),
        priority,
        pool,
        0,
        idempotency_key.map(str::to_string),
    )
}

async fn enqueue(
    backend: &dyn QueueBackend,
    job: boson_core::Job,
    config: &TaskConfig,
) -> (String, JobEnqueueDisposition) {
    backend
        .enqueue_with_policies(job, config)
        .await
        .expect("enqueue")
}

/// Asserts enqueue inserts and lists the job.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn enqueue_inserts_and_lists(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    let (job_id, disp) = enqueue(&*b, job, &config).await;
    assert_eq!(disp, JobEnqueueDisposition::InsertedNew);
    let listed = b.list_jobs(Some(JobStatus::Queued), 0, 10).await.unwrap();
    assert!(listed.iter().any(|j| j.job_id == job_id));
}

/// Asserts idempotency reuses non-terminal jobs.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn idempotency_reuses_nonterminal(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job1 = sample_job("echo", "global", 1, Some("idem-1"));
    let (id1, _) = enqueue(&*b, job1, &config).await;
    let job2 = sample_job("echo", "global", 1, Some("idem-1"));
    let (id2, disp) = enqueue(&*b, job2, &config).await;
    assert_eq!(disp, JobEnqueueDisposition::ReusedIdempotent);
    assert_eq!(id1, id2);
}

/// Asserts atomic job claim semantics.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn try_claim_atomic(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    let (job_id, _) = enqueue(&*b, job, &config).await;
    assert!(b.try_claim_job(&job_id).await.unwrap().is_some());
    assert!(b.try_claim_job(&job_id).await.unwrap().is_none());
}

/// Asserts two concurrent claimers produce exactly one success.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn try_claim_parallel(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    let (job_id, _) = enqueue(&*b, job, &config).await;
    let b1 = Arc::clone(&b);
    let b2 = Arc::clone(&b);
    let id1 = job_id.clone();
    let id2 = job_id.clone();
    let (r1, r2) = tokio::join!(b1.try_claim_job(&id1), b2.try_claim_job(&id2));
    let c1 = r1.expect("claimer a");
    let c2 = r2.expect("claimer b");
    let wins = usize::from(c1.is_some()) + usize::from(c2.is_some());
    assert_eq!(wins, 1, "exactly one parallel claimer must win");
    assert!(
        b.try_claim_job(&job_id).await.unwrap().is_none(),
        "third claim must fail"
    );
}

/// Asserts claim CAS keys off structured job status, not a substring in `params_json`.
///
/// Regression for Redis substring `"status":"queued"` matching inside attacker-controlled params.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn claim_ignores_status_in_params(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let mut job = sample_job("echo", "global", 1, None);
    let poisoned = json!({
        "note": "attacker \"status\":\"queued\" in params",
        "nested": { "status": "queued" },
    });
    job.params_json = poisoned.clone();
    let (job_id, _) = enqueue(&*b, job, &config).await;
    let claimed = b
        .try_claim_job(&job_id)
        .await
        .unwrap()
        .expect("first claim must succeed despite poisoned params");
    assert_eq!(claimed.status, JobStatus::Running);
    assert_eq!(claimed.params_json, poisoned);
    assert!(
        b.try_claim_job(&job_id).await.unwrap().is_none(),
        "second claim must fail"
    );
    let loaded = b.get_job(&job_id).await.unwrap().expect("job");
    assert_eq!(loaded.status, JobStatus::Running);
    assert_eq!(loaded.params_json, poisoned);
}

/// Asserts pool priority ordering for queued jobs.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn pool_priority_order(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let low = sample_job("echo", "workers", 1, None);
    let high = sample_job("echo", "workers", 5, None);
    enqueue(&*b, high, &config).await;
    enqueue(&*b, low, &config).await;
    let queued = b.list_queued_for_pool_sorted("workers", 10).await.unwrap();
    assert_eq!(queued.len(), 2);
    assert!(queued[0].priority <= queued[1].priority);
}

/// Asserts max in-flight rate limit enforcement.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn max_in_flight_rate_limit(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config_with_rate_limit("echo", 1, 0);
    let job1 = sample_job("echo", "global", 1, None);
    enqueue(&*b, job1, &config).await;
    let job2 = sample_job("echo", "global", 1, None);
    let err = b.enqueue_with_policies(job2, &config).await.unwrap_err();
    assert!(matches!(err, boson_core::BosonError::RateLimited(_)));
}

/// Asserts max enqueue per second rate limit enforcement.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn max_enqueue_per_second(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config_with_rate_limit("echo", 0, 1);
    let job1 = sample_job("echo", "global", 1, None);
    enqueue(&*b, job1, &config).await;
    let job2 = sample_job("echo", "global", 1, None);
    let err = b.enqueue_with_policies(job2, &config).await.unwrap_err();
    assert!(matches!(err, boson_core::BosonError::RateLimited(_)));
}

/// Asserts run lifecycle persistence.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn run_lifecycle(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    let (job_id, _) = enqueue(&*b, job, &config).await;
    let run = boson_core::Run::new(&job_id, "echo", 1);
    let run_id = run.run_id.clone();
    b.upsert_run(&run).await.unwrap();
    b.finish_run(&run_id, RunStatus::Success, Some(10), None)
        .await
        .unwrap();
    let loaded = b.get_run(&run_id).await.unwrap().expect("run");
    assert_eq!(loaded.status, RunStatus::Success);
}

/// Asserts the backend can be installed as the process-global default and resolved.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn global_router_resolves(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(Arc::clone(&b)));
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    enqueue(&*b, job, &config).await;
    let resolved = boson_core::default_backend_from_global().unwrap();
    assert!(Arc::ptr_eq(&b, &resolved));
}

/// Asserts lease contention blocks a second worker claim.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn lease_contention(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let job_id = "job-1";
    let l1 = b
        .try_claim_run_lease(job_id, "worker-a", 120)
        .await
        .unwrap()
        .expect("first claim");
    assert!(b
        .try_claim_run_lease(job_id, "worker-b", 120)
        .await
        .unwrap()
        .is_none());
    b.release_lease(&l1).await.unwrap();
    assert!(b
        .try_claim_run_lease(job_id, "worker-b", 120)
        .await
        .unwrap()
        .is_some());
}

/// Asserts lease extension keeps the lease active.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn extend_lease_refreshes_ttl(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let lease_id = b
        .try_claim_run_lease("job-2", "worker-a", 60)
        .await
        .unwrap()
        .expect("claim");
    b.extend_lease(&lease_id, 300).await.unwrap();
    assert_eq!(b.expired_lease_job_pairs().await.unwrap(), Vec::new());
}

/// Asserts expired leases appear in expired pairs listing.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn expired_lease_pairs(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let lease_id = b
        .try_claim_run_lease("job-3", "worker-a", -1)
        .await
        .unwrap()
        .expect("claim with negative ttl => already expired");
    let pairs = b.expired_lease_job_pairs().await.unwrap();
    assert!(pairs
        .iter()
        .any(|(lid, jid)| lid == &lease_id && jid == "job-3"));
}

/// Asserts reaper semantics: expired lease + release/revert returns job to queued for reclaim.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn reclaim_expired_lease_requeues_job(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let mut job = sample_job("reclaim-task", "global", 0, None);
    job.status = JobStatus::Running;
    let job_id = job.job_id.clone();
    b.upsert_job(&job).await.unwrap();
    let lease_id = b
        .try_claim_run_lease(&job_id, "worker-a", -1)
        .await
        .unwrap()
        .expect("expired lease");
    assert!(b
        .expired_lease_job_pairs()
        .await
        .unwrap()
        .iter()
        .any(|(lid, jid)| lid == &lease_id && jid == &job_id));
    b.release_lease(&lease_id).await.unwrap();
    b.revert_job_to_queued(&job_id).await.unwrap();
    let restored = b.get_job(&job_id).await.unwrap().expect("job");
    assert_eq!(restored.status, JobStatus::Queued);
    assert!(b.try_claim_job(&job_id).await.unwrap().is_some());
}

/// Whole-millisecond timestamp an hour in the past, so every engine stores it exactly
/// (Scylla keeps milliseconds, Postgres microseconds) and fresh enqueues sort after it.
fn keyset_base() -> DateTime<Utc> {
    let ms = Utc::now().timestamp_millis() - 3_600_000;
    Utc.timestamp_millis_opt(ms)
        .single()
        .expect("valid timestamp")
}

/// Offset `i / 3` milliseconds from `base`, so every timestamp is shared by three rows and the
/// id tie-break decides their order.
fn keyset_time(base: DateTime<Utc>, i: usize) -> DateTime<Utc> {
    base + chrono::Duration::milliseconds(i64::try_from(i / 3).expect("small index"))
}

/// Page `list_jobs_after` to the end and return every job in scan order.
///
/// `mid_scan` runs once after the third page.
async fn scan_jobs_after(
    b: &dyn QueueBackend,
    status: Option<JobStatus>,
    page_size: usize,
    mut mid_scan: impl FnMut() -> Option<Vec<Job>>,
) -> Vec<Job> {
    let mut out: Vec<Job> = Vec::new();
    let mut cursor: Option<JobKey> = None;
    let mut pages = 0_usize;
    loop {
        let page = b
            .list_jobs_after(
                &status.map_or_else(JobFilter::default, JobFilter::status),
                cursor.as_ref(),
                page_size,
            )
            .await
            .expect("list_jobs_after");
        assert!(page.len() <= page_size, "page exceeds limit");
        let Some(last) = page.last() else { break };
        cursor = Some(JobKey::from(last));
        out.extend(page);
        pages += 1;
        if pages == 3 {
            if let Some(jobs) = mid_scan() {
                for job in jobs {
                    enqueue(b, job, &task_config("keyset")).await;
                }
            }
        }
    }
    out
}

/// Checks one jobs scan: keys strictly increase, and among this test's jobs (`prefix`) the seeded
/// ones come first in `seeded` order, followed by exactly the `tail` ids in any order.
fn assert_jobs_scan(scanned: &[Job], prefix: &str, seeded: &[&str], tail: &[String], what: &str) {
    for pair in scanned.windows(2) {
        assert!(
            JobKey::from(&pair[0]).precedes(&pair[1]),
            "{what}: out of order: {} then {}",
            pair[0].job_id,
            pair[1].job_id
        );
    }
    let ids: Vec<&str> = scanned
        .iter()
        .map(|j| j.job_id.as_str())
        .filter(|id| id.starts_with(prefix))
        .collect();
    assert_eq!(
        ids.len(),
        seeded.len() + tail.len(),
        "{what}: each job once"
    );
    assert_eq!(
        ids[..seeded.len()],
        seeded[..],
        "{what}: seeded jobs in key order"
    );
    let mut got_tail: Vec<&str> = ids[seeded.len()..].to_vec();
    got_tail.sort_unstable();
    assert_eq!(got_tail, tail, "{what}: mid-scan enqueues at the end");
}

/// Asserts `list_jobs_after` pages 2,500 jobs in `created_at ASC, job_id ASC` order, once each,
/// applies the status filter, and puts jobs enqueued mid-scan at the end exactly once.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn keyset_jobs_pages_in_order(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    const TOTAL: usize = 2_500;
    const PAGE: usize = 100;
    const STATUSES: [JobStatus; 5] = [
        JobStatus::Queued,
        JobStatus::Running,
        JobStatus::Success,
        JobStatus::Failed,
        JobStatus::Canceled,
    ];
    let prefix = format!("ks-{}", uuid::Uuid::new_v4().simple());
    let base = keyset_base();
    let seeded: Vec<String> = (0..TOTAL).map(|i| format!("{prefix}-{i:05}")).collect();

    // Insert out of order (7 is coprime with 2,500) so insertion order cannot pass for sort order.
    for n in 0..TOTAL {
        let i = (n * 7) % TOTAL;
        let mut job = sample_job("keyset", "global", 1, None);
        job.job_id.clone_from(&seeded[i]);
        job.created_at = keyset_time(base, i);
        job.status = STATUSES[i % STATUSES.len()];
        b.upsert_job(&job).await.expect("upsert_job");
    }

    // Enqueued (status queued, created now) after the third page of the unfiltered scan.
    let new_ids: Vec<String> = (0..10).map(|k| format!("{prefix}-new-{k:02}")).collect();
    let mut pending = Some(
        new_ids
            .iter()
            .map(|id| {
                let mut job = sample_job("keyset", "global", 1, None);
                job.job_id.clone_from(id);
                job
            })
            .collect::<Vec<_>>(),
    );
    let scanned = scan_jobs_after(&*b, None, PAGE, || pending.take()).await;
    let all: Vec<&str> = seeded.iter().map(String::as_str).collect();
    assert_jobs_scan(&scanned, &prefix, &all, &new_ids, "unfiltered");

    for status in [JobStatus::Queued, JobStatus::Failed] {
        let filtered = scan_jobs_after(&*b, Some(status), PAGE, || None).await;
        assert!(filtered.iter().all(|j| j.status == status), "status filter");
        let expected: Vec<&str> = all
            .iter()
            .enumerate()
            .filter(|(i, _)| STATUSES[i % STATUSES.len()] == status)
            .map(|(_, id)| *id)
            .collect();
        let tail: &[String] = if status == JobStatus::Queued {
            &new_ids
        } else {
            &[]
        };
        assert_jobs_scan(&filtered, &prefix, &expected, tail, &status.to_string());
    }
}

/// Asserts `list_runs_after` pages one job's 250 runs (of 500) in `started_at DESC, run_id DESC`
/// order, once each, and skips a run that starts mid-scan.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn keyset_runs_pages_in_order(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    const TOTAL: usize = 500;
    const PAGE: usize = 40;
    let prefix = format!("ks-{}", uuid::Uuid::new_v4().simple());
    let job_even = format!("{prefix}-job-even");
    let job_odd = format!("{prefix}-job-odd");
    let base = keyset_base();
    let run_id = |i: usize| format!("{prefix}-{i:05}");

    for n in 0..TOTAL {
        let i = (n * 7) % TOTAL;
        let job = if i.is_multiple_of(2) {
            &job_even
        } else {
            &job_odd
        };
        let mut run = boson_core::Run::new(job, "keyset", 1);
        run.run_id = run_id(i);
        run.started_at = keyset_time(base, i);
        b.upsert_run(&run).await.expect("upsert_run");
    }

    let mut scanned: Vec<boson_core::Run> = Vec::new();
    let mut cursor: Option<RunKey> = None;
    let mut pages = 0_usize;
    let newer_id = format!("{prefix}-newer");
    loop {
        let page = b
            .list_runs_after(&RunFilter::job(&job_even), cursor.as_ref(), PAGE)
            .await
            .expect("list_runs_after");
        assert!(page.len() <= PAGE, "page exceeds limit");
        let Some(last) = page.last() else { break };
        cursor = Some(RunKey::from(last));
        scanned.extend(page);
        pages += 1;
        if pages == 2 {
            let mut run = boson_core::Run::new(&job_even, "keyset", 2);
            run.run_id.clone_from(&newer_id);
            b.upsert_run(&run).await.expect("upsert_run mid-scan");
        }
    }

    for pair in scanned.windows(2) {
        assert!(
            RunKey::from(&pair[0]).precedes(&pair[1]),
            "scan out of order: {} then {}",
            pair[0].run_id,
            pair[1].run_id
        );
    }
    assert!(scanned.iter().all(|r| r.job_id == job_even), "job filter");
    let ids: Vec<&str> = scanned.iter().map(|r| r.run_id.as_str()).collect();
    let expected: Vec<String> = (0..TOTAL)
        .rev()
        .filter(|i| i.is_multiple_of(2))
        .map(run_id)
        .collect();
    assert_eq!(
        ids, expected,
        "each run once, newest first; newer run not returned"
    );

    let fresh = b
        .list_runs_after(&RunFilter::job(&job_even), None, 1)
        .await
        .expect("list_runs_after fresh scan");
    assert_eq!(
        fresh.first().map(|r| r.run_id.as_str()),
        Some(newer_id.as_str()),
        "a new scan starts at the newest run"
    );
}

/// Page `list_jobs_after` with `filter` to the end and return the job ids in scan order.
async fn scan_job_ids(b: &dyn QueueBackend, filter: &JobFilter, page_size: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor: Option<JobKey> = None;
    loop {
        let page = b
            .list_jobs_after(filter, cursor.as_ref(), page_size)
            .await
            .expect("list_jobs_after");
        assert!(page.len() <= page_size, "page exceeds limit");
        let Some(last) = page.last() else { break };
        cursor = Some(JobKey::from(last));
        out.extend(page.into_iter().map(|j| j.job_id));
    }
    out
}

/// Page `list_runs_after` with `filter` to the end and return the run ids in scan order.
async fn scan_run_ids(b: &dyn QueueBackend, filter: &RunFilter, page_size: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor: Option<RunKey> = None;
    loop {
        let page = b
            .list_runs_after(filter, cursor.as_ref(), page_size)
            .await
            .expect("list_runs_after");
        assert!(page.len() <= page_size, "page exceeds limit");
        let Some(last) = page.last() else { break };
        cursor = Some(RunKey::from(last));
        out.extend(page.into_iter().map(|r| r.run_id));
    }
    out
}

/// Whole-millisecond "now", so every engine stores it exactly.
fn now_ms() -> DateTime<Utc> {
    Utc.timestamp_millis_opt(Utc::now().timestamp_millis())
        .single()
        .expect("valid timestamp")
}

const FILTER_JOBS: usize = 10_000;
const FILTER_STATUSES: [JobStatus; 5] = [
    JobStatus::Queued,
    JobStatus::Running,
    JobStatus::Success,
    JobStatus::Failed,
    JobStatus::Canceled,
];

/// Seeded layout for [`filtered_jobs_paging_10k`]: job `i` has task `i % 2`, pool `i % 3`,
/// status `i % 5`, and failed jobs with `i % 7 == 0` are dropped.
struct FilterSeed {
    prefix: String,
    tasks: [String; 2],
    pools: [String; 3],
    base: DateTime<Utc>,
}

impl FilterSeed {
    fn new() -> Self {
        let prefix = format!("fp-{}", uuid::Uuid::new_v4().simple());
        Self {
            tasks: [format!("{prefix}-alpha"), format!("{prefix}-beta")],
            pools: [
                format!("{prefix}-p0"),
                format!("{prefix}-p1"),
                format!("{prefix}-p2"),
            ],
            base: keyset_base(),
            prefix,
        }
    }

    fn id(&self, i: usize) -> String {
        format!("{}-{i:05}", self.prefix)
    }

    const fn status(i: usize) -> JobStatus {
        FILTER_STATUSES[i % FILTER_STATUSES.len()]
    }

    fn dropped(i: usize) -> bool {
        Self::status(i) == JobStatus::Failed && i.is_multiple_of(7)
    }

    fn task(&self, t: usize) -> JobFilter {
        JobFilter {
            task: Some(self.tasks[t].clone()),
            ..JobFilter::default()
        }
    }

    async fn insert(&self, b: &dyn QueueBackend) {
        // 7,919 is coprime with 10,000, so insertion order cannot pass for sort order.
        for n in 0..FILTER_JOBS {
            let i = (n * 7_919) % FILTER_JOBS;
            let mut job = sample_job(&self.tasks[i % 2], &self.pools[i % 3], 1, None);
            job.job_id = self.id(i);
            job.created_at = keyset_time(self.base, i);
            job.status = Self::status(i);
            if Self::dropped(i) {
                job.dropped_at = Some(self.base);
            }
            b.upsert_job(&job).await.expect("upsert_job");
        }
    }

    /// Scan with `filter` and check it returns exactly the seeded jobs `keep` selects, in order,
    /// and that the count agrees.
    async fn check(
        &self,
        b: &dyn QueueBackend,
        filter: JobFilter,
        keep: impl Fn(usize) -> bool,
        what: &str,
    ) {
        let expected: Vec<String> = (0..FILTER_JOBS)
            .filter(|i| keep(*i))
            .map(|i| self.id(i))
            .collect();
        let scanned = scan_job_ids(b, &filter, 250).await;
        assert_eq!(
            scanned, expected,
            "{what}: each matching job once, in order"
        );
        let count = b.count_jobs_matching(&filter).await.expect("count");
        assert_eq!(
            count,
            u64::try_from(expected.len()).unwrap(),
            "{what}: count matches the scan"
        );
    }
}

/// Asserts filtered keyset paging over 10,000 jobs returns each matching job once, in order, and
/// that `count_jobs_matching` agrees; covers status sets, task, pool, time range, id match and
/// dropped jobs.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn filtered_jobs_paging_10k(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let seed = FilterSeed::new();
    seed.insert(&*b).await;
    let b = &*b;
    let status = FilterSeed::status;
    let dropped = FilterSeed::dropped;

    let filter = JobFilter {
        statuses: vec![JobStatus::Failed, JobStatus::Success],
        pool: Some(seed.pools[0].clone()),
        ..seed.task(0)
    };
    let keep = |i: usize| {
        i % 6 == 0 && matches!(status(i), JobStatus::Failed | JobStatus::Success) && !dropped(i)
    };
    seed.check(b, filter, keep, "status set + task + pool")
        .await;

    let filter = JobFilter {
        statuses: vec![JobStatus::Failed],
        include_dropped: true,
        ..seed.task(1)
    };
    let keep = |i: usize| i % 2 == 1 && status(i) == JobStatus::Failed;
    seed.check(b, filter, keep, "failed incl. dropped").await;

    let filter = JobFilter {
        created_from: Some(keyset_time(seed.base, 3_000)),
        created_to: Some(keyset_time(seed.base, 6_000)),
        ..seed.task(0)
    };
    let keep = |i: usize| i % 2 == 0 && (3_000..6_000).contains(&i) && !dropped(i);
    seed.check(b, filter, keep, "time range").await;

    let with_id = |m: String| JobFilter {
        id_match: Some(m),
        ..seed.task(0)
    };
    seed.check(b, with_id("04242".into()), |i| i == 4_242, "id suffix")
        .await;
    seed.check(b, with_id(seed.id(4_242)), |i| i == 4_242, "whole id")
        .await;
    let upper = seed.id(4_242).to_uppercase();
    seed.check(b, with_id(upper), |_| false, "id match is case-sensitive")
        .await;
}

/// Which seeded runs a [`RunFilter`] case should return.
type KeepRun = Box<dyn Fn(usize) -> bool>;

/// Asserts filtered keyset paging over runs (status set including timeout, task, job id, time
/// range, id match) returns each matching run once, newest first, and `count_runs_matching`
/// agrees.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn filtered_runs_paging_and_count(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    const TOTAL: usize = 600;
    const STATUSES: [RunStatus; 5] = [
        RunStatus::Running,
        RunStatus::Success,
        RunStatus::Failed,
        RunStatus::Canceled,
        RunStatus::Timeout,
    ];
    let prefix = format!("fr-{}", uuid::Uuid::new_v4().simple());
    let tasks = [format!("{prefix}-x"), format!("{prefix}-y")];
    let jobs = [
        format!("{prefix}-job-a"),
        format!("{prefix}-job-b"),
        format!("{prefix}-job-c"),
    ];
    let base = keyset_base();
    let id = |i: usize| format!("{prefix}-{i:05}");
    let status_of = |i: usize| STATUSES[i % STATUSES.len()];

    for n in 0..TOTAL {
        let i = (n * 7) % TOTAL;
        let mut run = Run::new(&jobs[i % 3], &tasks[i % 2], 1);
        run.run_id = id(i);
        run.started_at = keyset_time(base, i);
        run.status = status_of(i);
        b.upsert_run(&run).await.expect("upsert_run");
    }

    let cases: Vec<(RunFilter, KeepRun, &str)> = vec![
        (
            RunFilter {
                statuses: vec![RunStatus::Failed, RunStatus::Timeout],
                task: Some(tasks[0].clone()),
                ..RunFilter::default()
            },
            Box::new(move |i| {
                i % 2 == 0 && matches!(status_of(i), RunStatus::Failed | RunStatus::Timeout)
            }),
            "status set incl. timeout + task",
        ),
        (
            RunFilter {
                job_id: Some(jobs[1].clone()),
                started_from: Some(keyset_time(base, 150)),
                started_to: Some(keyset_time(base, 450)),
                ..RunFilter::default()
            },
            Box::new(|i| i % 3 == 1 && (150..450).contains(&i)),
            "job + time range",
        ),
        (
            RunFilter {
                task: Some(tasks[1].clone()),
                id_match: Some("00321".into()),
                ..RunFilter::default()
            },
            Box::new(|i| i == 321),
            "id suffix",
        ),
    ];
    for (filter, keep, what) in cases {
        let expected: Vec<String> = (0..TOTAL).rev().filter(|i| keep(*i)).map(id).collect();
        let scanned = scan_run_ids(&*b, &filter, 17).await;
        assert_eq!(
            scanned, expected,
            "{what}: each matching run once, newest first"
        );
        let count = b.count_runs_matching(&filter).await.expect("count");
        assert_eq!(
            count,
            u64::try_from(expected.len()).unwrap(),
            "{what}: count matches the scan"
        );
    }
}

/// Asserts `retry_job` refuses jobs that aren't failed and turns a failed job into a fresh queued
/// job at attempt 1, run as the new actor, on the task type's current config, linked back to
/// the original, which stays as it was.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn retry_failed_job(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let task = format!("retry-{}", uuid::Uuid::new_v4().simple());
    let mut config = task_config(&task);
    config.pool = format!("{task}-pool");
    config.priority = 7;
    b.upsert_task_config(&config).await.expect("upsert config");

    let mut job = sample_job(&task, "old-pool", 2, None);
    job.params_json = json!({"invoice": 42});
    let old_id = job.job_id.clone();
    assert_retry_refused(&*b, &mut job).await;

    job.status = JobStatus::Failed;
    job.attempt = 3;
    b.upsert_job(&job).await.expect("upsert_job");
    let new_id = b
        .retry_job(&old_id, json!({"user": "bob"}))
        .await
        .expect("retry failed job");
    assert_ne!(new_id, old_id);
    let new = b.get_job(&new_id).await.unwrap().expect("new job");
    assert_eq!(new.status, JobStatus::Queued);
    assert_eq!(new.attempt, 1);
    assert_eq!(new.actor_json, json!({"user": "bob"}));
    assert_eq!(new.params_json, json!({"invoice": 42}));
    assert_eq!(new.retried_from.as_deref(), Some(old_id.as_str()));
    assert_eq!((new.pool.as_str(), new.priority), (config.pool.as_str(), 7));

    let old = b.get_job(&old_id).await.unwrap().expect("old job");
    assert_eq!((old.status, old.attempt), (JobStatus::Failed, 3));
    assert_eq!(old.actor_json, json!({"label": "test"}));

    config.pool = format!("{task}-moved");
    b.upsert_task_config(&config).await.expect("upsert config");
    let again = b
        .retry_job(&old_id, json!({"user": "carol"}))
        .await
        .expect("second retry");
    let again = b.get_job(&again).await.unwrap().expect("second job");
    assert_eq!(again.pool, config.pool, "uses the current config");
}

/// Retry must refuse `job` in every status but `failed`, and an unknown id.
async fn assert_retry_refused(b: &dyn QueueBackend, job: &mut Job) {
    for status in [
        JobStatus::Queued,
        JobStatus::Running,
        JobStatus::Success,
        JobStatus::Canceled,
    ] {
        job.status = status;
        b.upsert_job(job).await.expect("upsert_job");
        let err = b
            .retry_job(&job.job_id, json!({"user": "bob"}))
            .await
            .expect_err("retry of a job that isn't failed");
        assert!(
            matches!(err, BosonError::InvalidConfig(_)),
            "{status}: refused with InvalidConfig, got {err:?}"
        );
    }
    assert!(matches!(
        b.retry_job("no-such-job", json!({})).await,
        Err(BosonError::JobNotFound(_))
    ));
}

/// Asserts `drop_failed_job` flags only failed jobs, keeps them stored, and default list
/// filters leave them out.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn drop_failed_job_hides_from_lists(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let task = format!("drop-{}", uuid::Uuid::new_v4().simple());
    let mut failed = sample_job(&task, "global", 1, None);
    failed.status = JobStatus::Failed;
    b.upsert_job(&failed).await.unwrap();
    let queued = sample_job(&task, "global", 1, None);
    b.upsert_job(&queued).await.unwrap();

    assert!(matches!(
        b.drop_failed_job(&queued.job_id).await,
        Err(BosonError::InvalidConfig(_))
    ));
    assert!(matches!(
        b.drop_failed_job("no-such-job").await,
        Err(BosonError::JobNotFound(_))
    ));
    b.drop_failed_job(&failed.job_id).await.expect("drop");
    b.drop_failed_job(&failed.job_id).await.expect("drop twice");

    let stored = b
        .get_job(&failed.job_id)
        .await
        .unwrap()
        .expect("still stored");
    assert!(stored.dropped_at.is_some());
    let by_task = JobFilter {
        task: Some(task.clone()),
        ..JobFilter::default()
    };
    assert_eq!(
        scan_job_ids(&*b, &by_task, 10).await,
        std::slice::from_ref(&queued.job_id)
    );
    assert_eq!(b.count_jobs_matching(&by_task).await.unwrap(), 1);
    let with_dropped = JobFilter {
        include_dropped: true,
        ..by_task
    };
    assert_eq!(b.count_jobs_matching(&with_dropped).await.unwrap(), 2);
}

/// Asserts `sweep_failed` removes only failed jobs older than the cutoff and keeps their runs.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn sweep_failed_removes_old_failed(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let now = now_ms();
    let task = format!("sweep-{}", uuid::Uuid::new_v4().simple());
    let mk = |status, age_days| {
        let mut job = sample_job(&task, "global", 1, None);
        job.status = status;
        job.created_at = now - chrono::Duration::days(age_days);
        job
    };
    let old_failed = mk(JobStatus::Failed, 30);
    let new_failed = mk(JobStatus::Failed, 1);
    let old_success = mk(JobStatus::Success, 30);
    for job in [&old_failed, &new_failed, &old_success] {
        b.upsert_job(job).await.unwrap();
    }
    let run = Run::new(&old_failed.job_id, &task, 1);
    b.upsert_run(&run).await.unwrap();

    let removed = b
        .sweep_failed(now - boson_core::DEFAULT_FAILED_RETENTION)
        .await
        .expect("sweep");
    assert!(removed >= 1);
    assert!(b.get_job(&old_failed.job_id).await.unwrap().is_none());
    assert!(b.get_job(&new_failed.job_id).await.unwrap().is_some());
    assert!(b.get_job(&old_success.job_id).await.unwrap().is_some());
    assert!(
        b.get_run(&run.run_id).await.unwrap().is_some(),
        "runs are kept"
    );
}

/// Asserts a paused pool is never claimed from, still accepts enqueues, records who paused it,
/// leaves other pools alone, and claims again once resumed.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn paused_pool_is_never_claimed(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let pool = format!("pause-{}", uuid::Uuid::new_v4().simple());
    let other = format!("{pool}-other");
    let config = task_config("pause-task");
    let mut ids = Vec::new();
    for _ in 0..3 {
        let (id, _) = enqueue(&*b, sample_job("pause-task", &pool, 1, None), &config).await;
        ids.push(id);
    }
    let (other_id, _) = enqueue(&*b, sample_job("pause-task", &other, 1, None), &config).await;

    assert_pool_state(&*b, &pool, true, "alice").await;
    for id in &ids {
        assert!(
            b.try_claim_job(id).await.unwrap().is_none(),
            "paused: try_claim_job"
        );
    }
    assert!(
        b.pop_claim_from_pool(&pool).await.unwrap().is_none(),
        "paused: pop"
    );
    let (_, disp) = enqueue(&*b, sample_job("pause-task", &pool, 1, None), &config).await;
    assert_eq!(
        disp,
        JobEnqueueDisposition::InsertedNew,
        "enqueue still works"
    );
    for id in &ids {
        let job = b.get_job(id).await.unwrap().expect("job");
        assert_eq!(job.status, JobStatus::Queued);
    }
    assert!(
        b.try_claim_job(&other_id).await.unwrap().is_some(),
        "other pool claims"
    );

    let pools = b
        .pool_stats(chrono::Duration::hours(1))
        .await
        .expect("stats");
    let paused = pools
        .iter()
        .find(|s| s.pool == pool)
        .expect("pool in stats");
    assert!(paused.paused);
    assert_eq!(paused.waiting, 4);

    assert_pool_state(&*b, &pool, false, "bob").await;
    assert!(
        b.try_claim_job(&ids[0]).await.unwrap().is_some(),
        "resumed: claims"
    );
}

/// Set the pause flag and check both the returned and the stored state record it and who.
async fn assert_pool_state(b: &dyn QueueBackend, pool: &str, paused: bool, who: &str) {
    let returned = b
        .set_pool_paused(pool, paused, who)
        .await
        .expect("set_pool_paused");
    let stored = b.get_pool_state(pool).await.unwrap().expect("stored state");
    for state in [returned, stored] {
        assert_eq!(state.paused, paused);
        assert_eq!(state.changed_by.as_deref(), Some(who));
    }
}

/// Asserts `purge_queued_in_pool` removes the pool's queued jobs only.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn purge_leaves_running_jobs(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let pool = format!("purge-{}", uuid::Uuid::new_v4().simple());
    let other = format!("{pool}-other");
    let config = task_config("purge-task");
    for _ in 0..3 {
        enqueue(&*b, sample_job("purge-task", &pool, 1, None), &config).await;
    }
    let (running, _) = enqueue(&*b, sample_job("purge-task", &pool, 1, None), &config).await;
    assert!(b.try_claim_job(&running).await.unwrap().is_some());
    let mut done = sample_job("purge-task", &pool, 1, None);
    done.status = JobStatus::Success;
    b.upsert_job(&done).await.unwrap();
    let (kept, _) = enqueue(&*b, sample_job("purge-task", &other, 1, None), &config).await;

    assert_eq!(b.purge_queued_in_pool(&pool).await.expect("purge"), 3);
    let left = b.list_queued_for_pool_sorted(&pool, 10).await.unwrap();
    assert_eq!(left.len(), 0, "no queued jobs left");
    let running = b
        .get_job(&running)
        .await
        .unwrap()
        .expect("running job kept");
    assert_eq!(running.status, JobStatus::Running);
    assert!(
        b.get_job(&done.job_id).await.unwrap().is_some(),
        "finished job kept"
    );
    assert!(
        b.get_job(&kept).await.unwrap().is_some(),
        "other pool untouched"
    );
}

/// Asserts `pool_stats` counts waiting, running, oldest waiting, done and added per pool, and
/// lists a pool that only a task config names.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn pool_stats_counts(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let now = now_ms();
    let pool = format!("stats-{}", uuid::Uuid::new_v4().simple());
    let empty = format!("{pool}-empty");
    let mut config = task_config(&format!("{pool}-task"));
    config.pool.clone_from(&empty);
    b.upsert_task_config(&config).await.unwrap();

    let mk = |status, age_mins| {
        let mut job = sample_job("stats-task", &pool, 1, None);
        job.status = status;
        job.created_at = now - chrono::Duration::minutes(age_mins);
        job
    };
    let queued_new = mk(JobStatus::Queued, 1);
    let queued_old = mk(JobStatus::Queued, 120);
    let running = mk(JobStatus::Running, 1);
    let success = mk(JobStatus::Success, 1);
    let failed = mk(JobStatus::Failed, 240);
    for job in [&queued_new, &queued_old, &running, &success, &failed] {
        b.upsert_job(job).await.unwrap();
    }
    let mut ok_run = Run::new(&success.job_id, "stats-task", 1);
    ok_run.status = RunStatus::Success;
    ok_run.finished_at = Some(now);
    let mut old_run = Run::new(&failed.job_id, "stats-task", 1);
    old_run.started_at = now - chrono::Duration::minutes(200);
    old_run.status = RunStatus::Failed;
    old_run.finished_at = Some(now - chrono::Duration::minutes(180));
    b.upsert_run(&ok_run).await.unwrap();
    b.upsert_run(&old_run).await.unwrap();

    let stats = b
        .pool_stats(chrono::Duration::hours(1))
        .await
        .expect("stats");
    let busy = stats.iter().find(|s| s.pool == pool).expect("busy pool");
    assert_eq!((busy.waiting, busy.running), (2, 1));
    assert_eq!(busy.oldest_waiting_at, Some(queued_old.created_at));
    assert_eq!(
        busy.done_in_window, 1,
        "only the success finished in the window"
    );
    assert_eq!(busy.added_in_window, 3);
    assert!(!busy.paused);
    let idle = stats
        .iter()
        .find(|s| s.pool == empty)
        .expect("config-only pool");
    assert_eq!(
        (
            idle.waiting,
            idle.running,
            idle.done_in_window,
            idle.added_in_window
        ),
        (0, 0, 0, 0)
    );
    let names: Vec<&str> = stats.iter().map(|s| s.pool.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "sorted by pool name");
}
