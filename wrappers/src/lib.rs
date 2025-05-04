//! Wrappers is a development framework for Postgres Foreign Data Wrappers (FDW).
//! This crate provides the core functionality and implementations for various FDWs.
//! 
//! # Features
//! - Easy to implement FDW interface
//! - Support for rich data types
//! - Support both sync and async backends
//! - Built on top of pgrx

use pgrx::prelude::*;

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

/// FDW implementations for various data sources
pub mod fdw;
/// Statistics collection and reporting utilities
pub mod stats;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // noop
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
