//! Bucket-level versioned object repository over immutable Prolly trees.

mod authority;
mod cache;
mod codec;
mod error;
mod model;
mod object_plane;
mod repository;
mod runtime;
mod store;

pub use authority::{
    AuthorityLeaseStateV2, AuthorityLeaseV2, AuthorityPermitV2, AuthorityScopeV2, AuthorityStampV2,
    BranchRefBarrierV2, PendingAuthorityV2, ShardWriterAuthorityV2, TakeoverRequestV2,
};
pub use cache::{MemoryNodeCache, NodeCache, NodeCacheError, NodeCacheKey};
pub use codec::{decode_canonical, encode_canonical};
pub use error::{Error, ErrorCode, Result, RetryAdvice};
pub use model::*;
pub use object_plane::*;
pub use prolly::Cid;
pub use repository::{
    version_cursor_after_key, BranchHead, BranchPage, CatalogBranchPage, CatalogTagPage,
    CloneReport, CommitGraphAdvanceReport, CommitPage, FirstParentCursor, FirstParentPage,
    FsckReport, GcDryRun, GcEpochStepReport, GcSweepReport, HistoryCursor, IndexFreshness,
    MergeConflict, MergePlan, MergePolicy, NodeIndexAdvanceReport, NodeIndexMaintenance,
    ObjectDiff, ObjectDiffCursor, ObjectDiffPage, ObjectSummary, RefCatalogAdvanceReport,
    RefMoveReceipt, RefVersionCompactionReport, RepairReport, Repository, RepositoryOptions,
    RepositoryPerformanceSnapshot, SyncReport, Tag, TagPage, TraversalBudget, VersionSummary,
    WriterLeaseMaintenance,
};
pub use runtime::*;
pub use store::{NodeCacheSnapshot, ProllyObjectStore};
