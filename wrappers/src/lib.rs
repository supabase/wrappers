//! Wrappers is a development framework for Postgres Foreign Data Wrappers (FDW).
//! This crate provides the core functionality and implementations for various FDWs.
//!
//! # Features
//! - Easy to implement FDW interface
//! - Support for rich data types
//! - Support both sync and async backends
//! - Built on top of pgrx

use pgrx::prelude::*;
use std::sync::OnceLock;

// crypto primitive provider initialization required by rustls > v0.22.
// It is not required by every FDW, but only call it when needed.
// ref: https://docs.rs/rustls/latest/rustls/index.html#cryptography-providers
static RUSTLS_CRYPTO_PROVIDER_LOCK: OnceLock<()> = OnceLock::new();

#[allow(dead_code)]
fn setup_rustls_default_crypto_provider() {
    RUSTLS_CRYPTO_PROVIDER_LOCK.get_or_init(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .unwrap()
    });
}

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
