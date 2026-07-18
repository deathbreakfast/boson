//! Task registry implementation (quark macro output).

#![allow(missing_docs)]

use super::TaskDescriptor;

quark::define_registry! {
    /// Registry of tasks discovered via inventory (`auto_discover`) or [`register`](Self::register).
    /// Prefer [`BosonBuilder::auto_registry`](crate::BosonBuilder::auto_registry) at boot.
    pub struct TaskRegistry for TaskDescriptor;
}
