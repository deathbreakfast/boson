//! Full-matrix scenarios — skipped in PR smoke; run with `cargo test -p boson-e2e -- --include-ignored`.
//!
//! Catalog and expansion live in `boson-testkit` (`correctness_catalog` / `matrix_scenario_suite!`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

boson_testkit::matrix_scenario_suite!();
