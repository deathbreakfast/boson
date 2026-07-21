//! Sync EXPERIMENTS.md Results hints from report JSON (best-effort).

use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ReportFile {
    experiment_id: String,
    notes: String,
    pass: bool,
}

/// Scan reports and print a summary table for manual sync into EXPERIMENTS.md.
pub fn fill_results(reports_dir: &Path) -> Result<()> {
    if !reports_dir.exists() {
        println!("no reports dir: {}", reports_dir.display());
        return Ok(());
    }
    println!("| ID | Notes |");
    println!("|----|-------|");
    for entry in std::fs::read_dir(reports_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("projection-") || n.starts_with("scaling-curve-"))
        {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let report: ReportFile = match serde_json::from_str(&text) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let status = if report.pass { "PASS" } else { "FAIL" };
        println!(
            "| {} | {} ({}) |",
            report.experiment_id, report.notes, status
        );
    }
    println!("\nCopy rows into boson-bench/EXPERIMENTS.md experiment log.");
    Ok(())
}
