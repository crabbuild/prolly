//! Bucket-level versioned object repository over immutable Prolly trees.

mod codec;
mod error;
mod model;
mod object_plane;
mod repository;
mod runtime;
mod store;

pub use codec::{decode_canonical, encode_canonical};
pub use error::{Error, ErrorCode, Result, RetryAdvice};
pub use model::*;
pub use object_plane::*;
pub use repository::{
    version_cursor_after_key, BranchHead, CloneReport, FsckReport, GcDryRun, GcSweepReport,
    MergeConflict, MergePlan, MergePolicy, ObjectDiff, ObjectSummary, RefMoveReceipt, RepairReport,
    Repository, RepositoryOptions, SyncReport, Tag, VersionSummary, WriterLeaseMaintenance,
};
pub use runtime::*;
pub use store::ProllyObjectStore;
