#![warn(missing_docs)]
//! Shared foundations for the rSearch workspace: configuration loading
//! (TOML + `RSEARCH_` env overrides), node roles, FIPS TLS setup,
//! password/token crypto, error types, and telemetry init.

/// Configuration types and TOML/env loading.
pub mod config;
pub mod crypto;
/// Shared error type and result alias.
pub mod error;
/// Node roles (ingest/search/control) and role-list parsing.
pub mod role;
/// Tracing/logging initialization.
pub mod telemetry;
/// FIPS-provider rustls setup for servers and clients.
pub mod tls;
