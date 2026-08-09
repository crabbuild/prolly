use std::collections::BTreeMap;

use prolly::MapVersionId;
use prolly_dynamodb_core::{CommitId, TableId};

/// One accepted logical table-head event. Unchanged writes retain equal
/// before/after identifiers and `applied == false`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableTransitionMetadata {
    pub commit_id: Option<CommitId>,
    pub table_name: String,
    pub table_id: Option<TableId>,
    pub before: Option<MapVersionId>,
    pub after: Option<MapVersionId>,
    pub applied: bool,
}

impl TableTransitionMetadata {
    pub(crate) fn from_update(
        table_name: String,
        update: &prolly::VersionedMapUpdate,
        commit_id: Option<CommitId>,
        table_id: Option<TableId>,
    ) -> Self {
        match update {
            prolly::VersionedMapUpdate::Applied { previous, current } => Self {
                commit_id,
                table_name,
                table_id,
                before: previous.clone(),
                after: Some(current.id.clone()),
                applied: true,
            },
            prolly::VersionedMapUpdate::Unchanged { current }
            | prolly::VersionedMapUpdate::Conflict { current } => {
                let current = current.as_ref().map(|version| version.id.clone());
                Self {
                    commit_id,
                    table_name,
                    table_id,
                    before: current.clone(),
                    after: current,
                    applied: false,
                }
            }
        }
    }
}

/// Ordinary operation output plus the committed logical table version.
#[derive(Clone, Debug)]
pub struct WithMetadata<T> {
    pub output: T,
    /// Durable accepted-event identity when commit recording participates.
    pub commit_id: Option<CommitId>,
    /// Backward-compatible single-table version shortcut.
    pub version_id: Option<MapVersionId>,
    /// Exact immutable version observed or published for every table touched.
    pub table_versions: BTreeMap<String, MapVersionId>,
    /// Ordered accepted write transitions. Reads leave this empty.
    pub transitions: Vec<TableTransitionMetadata>,
}

impl<T> WithMetadata<T> {
    pub(crate) fn single(output: T, table: String, version_id: Option<MapVersionId>) -> Self {
        let mut table_versions = BTreeMap::new();
        if let Some(version) = &version_id {
            table_versions.insert(table, version.clone());
        }
        Self {
            output,
            commit_id: None,
            version_id,
            table_versions,
            transitions: Vec::new(),
        }
    }

    pub(crate) fn single_write(
        output: T,
        table: String,
        version_id: Option<MapVersionId>,
        commit_id: Option<CommitId>,
        transition: TableTransitionMetadata,
    ) -> Self {
        let mut result = Self::single(output, table, version_id);
        result.commit_id = commit_id;
        result.transitions.push(transition);
        result
    }

    pub(crate) fn multiple(output: T, table_versions: BTreeMap<String, MapVersionId>) -> Self {
        Self {
            output,
            commit_id: None,
            version_id: None,
            table_versions,
            transitions: Vec::new(),
        }
    }

    pub(crate) fn multiple_writes(
        output: T,
        table_versions: BTreeMap<String, MapVersionId>,
        transitions: Vec<TableTransitionMetadata>,
    ) -> Self {
        Self {
            output,
            commit_id: None,
            version_id: None,
            table_versions,
            transitions,
        }
    }

    pub(crate) fn transaction_writes(
        output: T,
        commit_id: CommitId,
        table_versions: BTreeMap<String, MapVersionId>,
        transitions: Vec<TableTransitionMetadata>,
    ) -> Self {
        Self {
            output,
            commit_id: Some(commit_id),
            version_id: None,
            table_versions,
            transitions,
        }
    }

    pub fn into_output(self) -> T {
        self.output
    }
}
