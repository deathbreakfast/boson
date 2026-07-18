//! Shared Mode 2 demo task — same `name` must exist on the worker for dispatch.
//!
//! Both `remote_enqueue` and `remote_worker` include this module so inventory registers
//! identically. Production apps usually put handlers in a shared crate and `use tasks as _;`
//! from the worker binary.

use boson::{task, ExecutionContext};

#[task(name = "remote_ping")]
pub async fn remote_ping(ctx: Box<dyn ExecutionContext>, message: String) -> boson_core::Result<()> {
    println!("remote_ping: {} (actor={})", message, ctx.label());
    Ok(())
}
