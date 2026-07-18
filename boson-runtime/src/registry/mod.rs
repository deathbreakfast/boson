//! Task descriptor and registry for handler dispatch.
//!
//! ## Entry points
//!
//! - [`TaskDescriptor`] — static metadata and default policies for one task
//! - [`TaskDefaults`] — grouped retry/rate/priority defaults for descriptors
//! - [`TaskRegistry`] — lookup tasks by name and invoke handlers
//!
//! ## Boot-time registration
//!
//! With [`BosonBuilder::auto_registry`](crate::BosonBuilder::auto_registry), tasks defined with
//! the [`boson_macros::task`] attribute are discovered at link time via [Quark](https://github.com/unified-field-dev/quark)
//! inventory ([`TaskRegistry::auto_discover`](TaskRegistry::auto_discover)).
//!
//! **Link closure:** both **worker** and **enqueue** binaries that call `send_with` /
//! [`BosonBuilder::auto_registry`](crate::BosonBuilder::auto_registry) must depend on every crate
//! that defines tasks (enqueue needs descriptors; workers need handlers).
//! See the [`boson`](https://docs.rs/uf-boson) crate
//! [Mode 1](https://docs.rs/uf-boson/latest/boson/index.html#mode-1--embedded-one-binary) /
//! [Mode 2](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries).
//!
//! ## Manual registration (tests and advanced)
//!
//! [`TaskRegistry::register`] uses the same dispatch path as auto-discovery and is common in unit
//! tests. See [`TaskDescriptor`] for policy fields and signature versioning.
//!
//! ```rust,no_run
//! use boson_runtime::{InvokeFn, TaskDescriptor, TaskDefaults, TaskRegistry};
//!
//! fn register_example(registry: &mut TaskRegistry, invoke: InvokeFn) {
//!     let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::with_defaults(
//!         "example",
//!         invoke,
//!         "{}",
//!         0,
//!         TaskDefaults::standard(),
//!     )));
//!     registry.register(desc);
//! }
//! ```

mod descriptor;
mod ext;
mod registry_impl;

pub use descriptor::{InvokeFn, TaskDefaults, TaskDescriptor};
pub use registry_impl::TaskRegistry;
