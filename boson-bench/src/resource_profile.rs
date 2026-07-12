//! Process RSS and CPU sampling for cloud hardware profiles.

use serde::{Deserialize, Serialize};
use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

/// Per-run resource usage snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceProfile {
    /// Process RSS at start (MiB).
    pub rss_start_mb: f64,
    /// Process RSS at end (MiB).
    pub rss_end_mb: f64,
    /// Peak process RSS (MiB).
    pub rss_peak_mb: f64,
    /// Mean CPU % during run.
    pub cpu_mean_pct: f64,
    /// Peak CPU % during run.
    pub cpu_peak_pct: f64,
}

/// Sample resource usage during a benchmark run.
pub struct ResourceSampler {
    sys: System,
    pid: sysinfo::Pid,
    rss_start: u64,
    rss_peak: u64,
    cpu_sum: f64,
    cpu_peak: f64,
    samples: u32,
}

impl ResourceSampler {
    /// Start sampling for the current process.
    pub fn start() -> Self {
        let pid = sysinfo::get_current_pid().expect("current pid");
        let mut sys = System::new_with_specifics(
            RefreshKind::nothing()
                .with_memory(MemoryRefreshKind::everything())
                .with_processes(ProcessRefreshKind::everything()),
        );
        sys.refresh_processes(ProcessesToUpdate::All, true);
        let rss_start = process_rss(&sys, pid);
        Self {
            sys,
            pid,
            rss_start,
            rss_peak: rss_start,
            cpu_sum: 0.0,
            cpu_peak: 0.0,
            samples: 0,
        }
    }

    /// Take one sample (call periodically during long runs).
    pub fn sample(&mut self) {
        self.sys.refresh_processes(ProcessesToUpdate::All, true);
        let rss = process_rss(&self.sys, self.pid);
        self.rss_peak = self.rss_peak.max(rss);
        if let Some(proc) = self.sys.process(self.pid) {
            let cpu = f64::from(proc.cpu_usage());
            self.cpu_sum += cpu;
            self.cpu_peak = self.cpu_peak.max(cpu);
            self.samples += 1;
        }
    }

    /// Finish sampling and build profile.
    pub fn finish(mut self) -> ResourceProfile {
        self.sys.refresh_processes(ProcessesToUpdate::All, true);
        let rss_end = process_rss(&self.sys, self.pid);
        self.rss_peak = self.rss_peak.max(rss_end);
        ResourceProfile {
            rss_start_mb: bytes_to_mb(self.rss_start),
            rss_end_mb: bytes_to_mb(rss_end),
            rss_peak_mb: bytes_to_mb(self.rss_peak),
            cpu_mean_pct: if self.samples == 0 {
                0.0
            } else {
                self.cpu_sum / f64::from(self.samples)
            },
            cpu_peak_pct: self.cpu_peak,
        }
    }
}

fn process_rss(sys: &System, pid: sysinfo::Pid) -> u64 {
    sys.process(pid).map_or(0, sysinfo::Process::memory)
}

fn bytes_to_mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}
