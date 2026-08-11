//! Bucket-level versioned object repository over immutable Prolly trees.

mod codec;
mod content;
mod error;
mod model;
mod object_plane;
mod protection;
mod repository;
mod runtime;
mod store;

pub use codec::{decode_canonical, encode_canonical};
pub use content::{ContentStore, StoredContent};
pub use error::{Error, ErrorCode, Result, RetryAdvice};
pub use model::*;
pub use object_plane::*;
pub use protection::*;
pub use repository::{
    version_cursor_after_key, BranchHead, CloneReport, FsckReport, GcDryRun, GcSweepReport,
    MergeConflict, MergePlan, MergePolicy, MultipartUploadSummary, ObjectDiff, ObjectSummary,
    RefMoveReceipt, RepairReport, Repository, RepositoryOptions, SyncReport, Tag, VersionSummary,
    WriterLeaseMaintenance, MAX_LOGICAL_RETRY_LIMIT,
};
pub use runtime::*;
pub use store::ProllyObjectStore;
