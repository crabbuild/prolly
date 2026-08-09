//! A transactional GlueSQL storage engine backed by [`prolly-map`](prolly).
//!
//! `ProllyStorage` stores a complete logical database in one immutable Prolly
//! tree. GlueSQL transactions mutate a private candidate tree and make it
//! visible by atomically advancing a named branch root at commit.

mod error;
mod layout;
mod storage;

pub use error::{Error, Result};
pub use storage::{
    DatabaseVersion, Diff, FunctionChange, ProllyStorage, ProllyStorageConfig, RowChange,
    SchemaChange,
};

#[cfg(feature = "sqlite")]
pub use storage::SqliteProllyStorage;

pub use gluesql_core;
pub use gluesql_core::prelude::{Glue, Payload};
