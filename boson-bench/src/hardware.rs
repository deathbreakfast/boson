//! Hardware profile capture for report JSON.

use serde::{Deserialize, Serialize};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};

/// Root filesystem mount metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RootMount {
    /// Block device.
    pub device: String,
    /// Mount point path.
    pub mount_point: String,
    /// Filesystem type.
    pub fs_type: String,
    /// Size in GiB.
    pub size_gib: f64,
}

/// Full hardware profile embedded in each report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HardwareDetail {
    /// CPU model string.
    pub cpu_model: String,
    /// Logical CPU count.
    pub cpu_cores: usize,
    /// Total RAM in GiB.
    pub ram_gib: f64,
    /// OS description.
    pub os: String,
    /// Root mount metadata.
    pub root_mount: RootMount,
}

/// Capture live hardware metadata for the current machine.
pub fn capture() -> HardwareDetail {
    let cpu_model = read_cpu_model();
    let cpu_cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);

    let mut sys = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
    );
    sys.refresh_memory();
    let ram_gib = sys.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0);

    HardwareDetail {
        cpu_model,
        cpu_cores,
        ram_gib,
        os: read_os_string(),
        root_mount: capture_root_mount(),
    }
}

fn read_cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .map(|l| l.split_once(':').map_or(l, |(_, v)| v.trim()))
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unknown".into())
}

fn read_os_string() -> String {
    std::process::Command::new("uname")
        .args(["-sr"])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

fn capture_root_mount() -> RootMount {
    RootMount {
        device: "/dev/root".into(),
        mount_point: "/".into(),
        fs_type: "unknown".into(),
        size_gib: 0.0,
    }
}

/// Whether this hardware slug should capture resource profiles.
pub fn captures_resource_profile(hardware: &str) -> bool {
    hardware.starts_with("aws-") || hardware == "ci-small"
}
