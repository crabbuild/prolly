//! Bucket-level versioned object repository over immutable Prolly trees.

mod authority;
mod cache;
mod codec;
mod control_versions;
mod error;
mod journal_indexes_v2;
mod model;
mod object_plane;
mod operation_index_v2;
mod payload_v2;
mod publication_v2;
mod repository;
mod repository_v2;
mod runtime;
mod store;

pub use authority::{
    AuthorityLeaseStateV2, AuthorityLeaseV2, AuthorityPermitV2, AuthorityScopeV2, AuthorityStampV2,
    BranchRefBarrierV2, PendingAuthorityV2, ShardWriterAuthorityV2, TakeoverRequestV2,
};
pub use cache::{MemoryNodeCache, NodeCache, NodeCacheError, NodeCacheKey};
pub use codec::{decode_canonical, encode_canonical};
pub use control_versions::{
    classify_mutable_control_path, ControlVersionCompactionReport, MutableControlKind,
    MutableControlObserver, MutableControlStore, DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};
pub use error::{Error, ErrorCode, Result, RetryAdvice};
pub use journal_indexes_v2::{
    JournalDerivedIndexesV2, JournalIndexAdvanceReportV2,
    DEFAULT_JOURNAL_INDEX_MAX_UNINDEXED_EVENTS,
};
pub use model::*;
pub use object_plane::*;
pub use operation_index_v2::{
    OperationIndexAdvanceReportV2, SegmentedOperationIndexV2, DEFAULT_OPERATION_INDEX_LEAF_ENTRIES,
    DEFAULT_OPERATION_INDEX_MAX_UNINDEXED_EVENTS, DEFAULT_OPERATION_INDEX_MERGE_FANOUT,
};
pub use payload_v2::ImmutablePayloadStoreV2;
pub use prolly::Cid;
pub use publication_v2::{
    AppliedBranchBarrierV2, CommitPublicationV2, LoadedRefV2, PublicationJournalCursorV2,
    PublicationJournalEntryV2, PublicationJournalPageV2, ShardedBranchPublisherV2,
};
pub use repository::{
    validate_branch, version_cursor_after_key, BranchHead, BranchPage, BranchReflogCursor,
    BranchReflogPage, CatalogBranchPage, CatalogTagPage, CloneReport, CommitClosureCleanupReport,
    CommitClosureCursor, CommitClosurePage, CommitGraphAdvanceReport, CommitPage,
    FirstParentCursor, FirstParentPage, FsckReport, GcDryRun, GcEpochStepReport, GcSweepReport,
    HistoryCursor, IndexFreshness, InternalNodePrewarmReport, MergeConflict, MergePlan,
    MergePolicy, NodeIndexAdvanceReport, NodeIndexMaintenance, ObjectDiff, ObjectDiffCursor,
    ObjectDiffPage, ObjectSummary, PhysicalTransferCursor, PhysicalTransferPage,
    RefCatalogAdvanceReport, RefMoveReceipt, RefVersionCompactionReport, RepairReport, Repository,
    RepositoryOptions, RepositoryPerformanceSnapshot, ResumableFsckCursor, ResumableFsckPage,
    ResumableFsckPhase, RetentionPinPage, ShardAuthorityMaintenance, SyncReport, Tag, TagPage,
    TagReflogPage, TraversalBudget, VersionSummary,
};
pub use repository_v2::{
    BranchIndexAdvanceReportV2, CommitReceiptV2, ObjectDataV2, ObjectSummaryV2, RepositoryV2,
    RepositoryV2Options, VersionSummaryV2,
};
#[deprecated(
    since = "0.1.0",
    note = "use ShardAuthorityMaintenance; repository writes use branch/system authority scopes"
)]
pub type WriterLeaseMaintenance = ShardAuthorityMaintenance;
pub use runtime::*;
pub use store::{NodeCacheSnapshot, ProllyObjectStore};
