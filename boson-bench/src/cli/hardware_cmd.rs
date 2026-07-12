//! Hardware profile capture CLI.

use anyhow::Result;

/// Print captured hardware detail as JSON.
pub fn dispatch_hardware() -> Result<()> {
    let detail = crate::hardware::capture();
    println!("{}", serde_json::to_string_pretty(&detail)?);
    Ok(())
}
