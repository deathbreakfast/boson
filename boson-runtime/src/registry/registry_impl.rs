//! Task registry implementation (quark macro output).

#![allow(missing_docs)]

use super::TaskDescriptor;

quark::define_registry! {
    /// Registry of tasks discovered via inventory or manual registration.
    pub struct TaskRegistry for TaskDescriptor;
}
