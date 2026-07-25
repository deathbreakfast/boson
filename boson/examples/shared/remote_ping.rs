//! Shared remote-worker demo task — same `name` must exist on the worker for dispatch.
//!
//! Enqueue and worker examples include this module so inventory registers identically.
//! Production apps usually put handlers in a shared crate and `use tasks as _;`
//! from the worker binary.

#![allow(clippy::print_stdout)] // Examples print status to the console.

use boson::{task, ExecutionContext};

#[task(name = "remote_ping")]
#[allow(clippy::unused_async)] // `#[task]` requires async handlers.
pub async fn remote_ping(
    ctx: Box<dyn ExecutionContext>,
    message: String,
) -> boson_core::Result<()> {
    println!("remote_ping: {} (actor={})", message, ctx.label());
    Ok(())
}
