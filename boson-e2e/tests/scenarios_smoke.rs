//! CI smoke scenarios — run on every push/PR (mem + sqlite active).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

boson_testkit::matrix_smoke_suite!();
