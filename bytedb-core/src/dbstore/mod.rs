//! Per-database on-disk persistence.
//!
//! Layout:
//! ```text
//! <data-dir>/
//!   server.meta            # registry of databases
//!   databases/
//!     <db-name>/
//!       catalog.bin        # schemas, sequences, fk metadata
//!       tables/
//!         <table>.tbl      # raw key/value pairs (binary)
//! ```
//!
//! This module deliberately avoids depending on snapshot machinery so it can
//! be reused for per-table flushes that are independent of snapshot cadence.

pub mod registry;
pub mod catalog;
pub mod tablefile;

pub use registry::DatabaseRegistry;
pub use catalog::{DbCatalog, TableCatalog};
pub use tablefile::TableFile;
