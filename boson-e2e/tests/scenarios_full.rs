//! Full-matrix scenarios — skipped in PR smoke; run with `cargo test -p boson-e2e -- --include-ignored`.
//!
//! Catalog and expansion live in `boson-testkit` (`correctness_catalog` / `matrix_scenario_suite!`).

boson_testkit::matrix_scenario_suite!();
