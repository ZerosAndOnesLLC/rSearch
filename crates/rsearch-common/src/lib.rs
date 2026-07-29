//! Shared foundations for the rSearch workspace: configuration loading
//! (TOML + `RSEARCH_` env overrides), node roles, FIPS TLS setup,
//! password/token crypto, error types, and telemetry init.

pub mod config;
pub mod crypto;
pub mod error;
pub mod role;
pub mod telemetry;
pub mod tls;
