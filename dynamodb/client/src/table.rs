use futures_util::{stream, Stream};
use prolly::{
    Diff, MapVersion, MapVersionCursor, MapVersionId, MapVersionPage, StructuralDiffCursor,
    StructuralDiffPage, VersionedMapUpdate,
};
use prolly_dynamodb_core::{
    CommitId, ImportPlan, ImportResult, IndexReconfigurationAuditRecord, IndexReconfigurationPlan,
    IndexReconfigurationPlanId, IndexReconfigurationResult, MaintenanceContext,
    RetentionAuditRecord, RetentionPlan, RetentionPlanId, RetentionPolicy, RetentionResult,
    SecondaryIndexDefinition, TableArchive, TableArchiveLimits, TableCommitPage,
    TransactWriteResult,
};

use crate::operation::{DeleteItem, GetItem, PutItem, Query, Scan, UpdateItem};
use crate::{Client, Error, Result, TableTransitionMetadata, WithMetadata};

#[derive(Clone)]
pub struct Table {
    client: Client,
    name: String,
}

impl Table {
    pub(crate) fn new(client: Client, name: String) -> Self {
        Self { client, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn head(&self) -> Result<MapVersion> {
        Ok(self.client.core().head(&self.name).await?)
    }

    /// Export the currently pinned head as a bounded, self-contained archive.
    pub async fn export(&self, limits: TableArchiveLimits) -> Result<TableArchive> {
        Ok(self
            .client
            .core()
            .export_table(&self.name, None, limits)
            .await?)
    }

    /// Collect and newest-first sort a bounded number of versions.
    ///
    /// Use [`Self::versions`] for arbitrary history sizes. This convenience
    /// method fails instead of allocating beyond the advertised collection cap.
    pub async fn collect_versions(&self) -> Result<Vec<MapVersion>> {
        Ok(self.client.core().versions(&self.name).await?)
    }

    /// Create a bounded paginator in stable version-ID byte order.
    pub fn versions(&self) -> VersionsPaginator {
        VersionsPaginator {
            table: self.clone(),
            cursor: None,
            page_size: prolly_dynamodb_core::MAX_VERSION_PAGE_ITEMS,
            finished: false,
        }
    }

    /// List a bounded ascending page of durable events for this exact current
    /// table incarnation.
    pub async fn commits(
        &self,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<TableCommitPage> {
        Ok(self
            .client
            .core()
            .commits(&self.name, after_sequence, limit)
            .await?)
    }

    /// Resolve a commit only when this table incarnation participated in it.
    pub async fn commit(&self, id: &CommitId) -> Result<Option<TransactWriteResult>> {
        let description = self.client.core().describe_table(&self.name).await?;
        let commit = self.client.core().commit(id).await?;
        match commit {
            Some(commit)
                if commit
                    .transitions
                    .iter()
                    .any(|transition| transition.table_id == description.id) =>
            {
                Ok(Some(commit))
            }
            Some(_) => Err(Error::InvalidRequest(format!(
                "commit {id} does not belong to table {:?} incarnation {}",
                self.name,
                hex_table_id(&description.id.0)
            ))),
            None => Ok(None),
        }
    }

    pub fn at(&self, version: MapVersionId) -> Snapshot {
        Snapshot {
            client: self.client.clone(),
            table_name: self.name.clone(),
            version,
        }
    }

    pub fn put_item(&self) -> PutItem {
        self.client.put_item().table_name(&self.name)
    }

    pub fn delete_item(&self) -> DeleteItem {
        self.client.delete_item().table_name(&self.name)
    }

    pub fn update_item(&self) -> UpdateItem {
        self.client.update_item().table_name(&self.name)
    }

    pub fn if_head(&self, version: MapVersionId) -> ConditionalTable {
        ConditionalTable {
            client: self.client.clone(),
            table_name: self.name.clone(),
            version,
        }
    }

    /// Collect a bounded diff into memory.
    ///
    /// Use [`Self::diff`] for arbitrary diff sizes. This convenience method
    /// fails instead of allocating beyond the advertised collection cap.
    pub async fn collect_diff(
        &self,
        base: &MapVersionId,
        target: &MapVersionId,
    ) -> Result<Vec<Diff>> {
        Ok(self.client.core().diff(&self.name, base, target).await?)
    }

    /// Create a bounded, resumable diff paginator over two immutable versions.
    pub fn diff(&self, base: MapVersionId, target: MapVersionId) -> DiffPaginator {
        DiffPaginator {
            table: self.clone(),
            base,
            target,
            cursor: None,
            page_size: prolly_dynamodb_core::MAX_DIFF_PAGE_ITEMS,
            finished: false,
        }
    }

    pub fn restore(&self, target: MapVersionId) -> Restore {
        Restore {
            client: self.client.clone(),
            table_name: self.name.clone(),
            target,
            expected_head: None,
            request_token: None,
        }
    }

    /// Create a read-only retention planner. Calling this never mutates roots.
    pub fn retention(&self, policy: RetentionPolicy) -> RetentionPlanner {
        RetentionPlanner {
            table: self.clone(),
            policy,
        }
    }

    /// Create a read-only planner for the exact desired secondary-index set.
    /// Planning performs no catalog, table-head, or indexed-root mutation.
    pub fn indexes(&self, desired: Vec<SecondaryIndexDefinition>) -> Indexes {
        Indexes {
            table: self.clone(),
            desired,
        }
    }

    /// Atomically activate a previously reviewed shadow-build plan.
    pub async fn apply_indexes(
        &self,
        plan: &IndexReconfigurationPlan,
        context: MaintenanceContext,
    ) -> Result<IndexReconfigurationResult> {
        if plan.table_name != self.name {
            return Err(Error::InvalidRequest(format!(
                "index plan belongs to table {:?}, not {:?}",
                plan.table_name, self.name
            )));
        }
        Ok(self
            .client
            .core()
            .apply_index_reconfiguration(plan, context)
            .await?)
    }

    /// Resolve durable index-activation evidence for this table incarnation.
    pub async fn indexes_audit(
        &self,
        id: &IndexReconfigurationPlanId,
    ) -> Result<Option<IndexReconfigurationAuditRecord>> {
        let description = self.client.core().describe_table(&self.name).await?;
        let audit = self.client.core().index_reconfiguration_audit(id).await?;
        match audit {
            Some(audit) if audit.plan.table_id == description.id => Ok(Some(audit)),
            Some(_) => Err(Error::InvalidRequest(format!(
                "index plan {id} does not belong to table {:?} incarnation {}",
                self.name,
                hex_table_id(&description.id.0)
            ))),
            None => Ok(None),
        }
    }

    /// Explicitly execute a previously reviewed exact retention plan.
    pub async fn apply_retention(
        &self,
        plan: &RetentionPlan,
        context: MaintenanceContext,
    ) -> Result<RetentionResult> {
        if plan.table_name != self.name {
            return Err(Error::InvalidRequest(format!(
                "retention plan belongs to table {:?}, not {:?}",
                plan.table_name, self.name
            )));
        }
        Ok(self.client.core().apply_retention(plan, context).await?)
    }

    /// Resolve a durable retention audit only for this exact incarnation.
    pub async fn retention_audit(
        &self,
        id: &RetentionPlanId,
    ) -> Result<Option<RetentionAuditRecord>> {
        let description = self.client.core().describe_table(&self.name).await?;
        let audit = self.client.core().retention_audit(id).await?;
        match audit {
            Some(audit) if audit.plan.table_id == description.id => Ok(Some(audit)),
            Some(_) => Err(Error::InvalidRequest(format!(
                "retention plan {id} does not belong to table {:?} incarnation {}",
                self.name,
                hex_table_id(&description.id.0)
            ))),
            None => Ok(None),
        }
    }
}

/// Read-only exact secondary-index configuration planner.
#[derive(Clone)]
pub struct Indexes {
    table: Table,
    desired: Vec<SecondaryIndexDefinition>,
}

impl Indexes {
    pub fn desired(&self) -> &[SecondaryIndexDefinition] {
        &self.desired
    }

    pub async fn plan(self) -> Result<IndexReconfigurationPlan> {
        Ok(self
            .table
            .client
            .core()
            .plan_index_reconfiguration(&self.table.name, self.desired)
            .await?)
    }
}

/// Read-only builder for an exact, bounded retention plan.
#[derive(Clone)]
pub struct RetentionPlanner {
    table: Table,
    policy: RetentionPolicy,
}

impl RetentionPlanner {
    pub async fn plan(self) -> Result<RetentionPlan> {
        Ok(self
            .table
            .client
            .core()
            .plan_retention(&self.table.name, self.policy)
            .await?)
    }
}

/// Explicit read-only-plan/apply workflow for importing one archive.
pub struct Import {
    client: Client,
    archive: TableArchive,
    target_table_name: String,
    limits: TableArchiveLimits,
}

impl Import {
    pub(crate) fn new(
        client: Client,
        archive: TableArchive,
        target_table_name: String,
        limits: TableArchiveLimits,
    ) -> Self {
        Self {
            client,
            archive,
            target_table_name,
            limits,
        }
    }

    pub fn archive(&self) -> &TableArchive {
        &self.archive
    }

    pub fn target_table_name(&self) -> &str {
        &self.target_table_name
    }

    /// Validate without publishing any logical table state.
    pub async fn plan(&self) -> Result<ImportPlan> {
        Ok(self
            .client
            .core()
            .plan_import(&self.archive, &self.target_table_name, self.limits)
            .await?)
    }

    /// Explicitly apply a reviewed plan with required operator attribution.
    pub async fn apply(
        &self,
        plan: &ImportPlan,
        context: MaintenanceContext,
    ) -> Result<ImportResult> {
        if plan.target_table_name != self.target_table_name {
            return Err(Error::InvalidRequest(format!(
                "import plan targets {:?}, not {:?}",
                plan.target_table_name, self.target_table_name
            )));
        }
        Ok(self
            .client
            .core()
            .apply_import(&self.archive, plan, context, self.limits)
            .await?)
    }
}

/// Stateful immutable-version paginator. Pages use version-ID order so the
/// cursor is stable without a server-side session or an unbounded timestamp
/// sort.
#[derive(Clone)]
pub struct VersionsPaginator {
    table: Table,
    cursor: Option<MapVersionCursor>,
    page_size: usize,
    finished: bool,
}

impl VersionsPaginator {
    pub fn page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn cursor(mut self, cursor: MapVersionCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    pub fn set_cursor(mut self, cursor: Option<MapVersionCursor>) -> Self {
        self.cursor = cursor;
        self
    }

    pub async fn next_page(&mut self) -> Result<Option<MapVersionPage>> {
        if self.finished {
            return Ok(None);
        }
        let previous = self.cursor.clone();
        let page = self
            .table
            .client
            .core()
            .versions_page(&self.table.name, self.cursor.as_ref(), self.page_size)
            .await?;
        match &page.next_cursor {
            Some(next) => {
                if previous.as_ref() == Some(next) {
                    return Err(Error::Core(prolly_dynamodb_core::Error::CorruptData(
                        "versions paginator did not advance its cursor".into(),
                    )));
                }
                self.cursor = Some(next.clone());
            }
            None => self.finished = true,
        }
        Ok(Some(page))
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<MapVersionPage>> + Send + 'static {
        stream::try_unfold(self, |mut paginator| async move {
            match paginator.next_page().await? {
                Some(page) => Ok(Some((page, paginator))),
                None => Ok(None),
            }
        })
    }
}

/// Stateful structural-diff paginator. Its cursor contains the immutable roots
/// and traversal frontier, so it can be serialized as a durable checkpoint and
/// fails closed if resumed against another version pair.
#[derive(Clone)]
pub struct DiffPaginator {
    table: Table,
    base: MapVersionId,
    target: MapVersionId,
    cursor: Option<StructuralDiffCursor>,
    page_size: usize,
    finished: bool,
}

impl DiffPaginator {
    /// Bound each request to at most `page_size` changes.
    pub fn page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    /// Resume from a previously persisted structural checkpoint.
    pub fn cursor(mut self, cursor: StructuralDiffCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    pub fn set_cursor(mut self, cursor: Option<StructuralDiffCursor>) -> Self {
        self.cursor = cursor;
        self
    }

    pub async fn next_page(&mut self) -> Result<Option<StructuralDiffPage>> {
        if self.finished {
            return Ok(None);
        }
        let previous = self.cursor.clone();
        let page = self
            .table
            .client
            .core()
            .structural_diff_page(
                &self.table.name,
                &self.base,
                &self.target,
                self.cursor.as_ref(),
                self.page_size,
            )
            .await?;
        match &page.next_cursor {
            Some(next) => {
                if previous.as_ref() == Some(next) {
                    return Err(Error::Core(prolly_dynamodb_core::Error::CorruptData(
                        "diff paginator did not advance its structural cursor".into(),
                    )));
                }
                self.cursor = Some(next.clone());
            }
            None => self.finished = true,
        }
        Ok(Some(page))
    }

    /// Consume this paginator as a bounded fallible asynchronous page stream.
    pub fn into_stream(self) -> impl Stream<Item = Result<StructuralDiffPage>> + Send + 'static {
        stream::try_unfold(self, |mut paginator| async move {
            match paginator.next_page().await? {
                Some(page) => Ok(Some((page, paginator))),
                None => Ok(None),
            }
        })
    }
}

/// Explicit CAS restore builder. Restores never infer the expected head.
#[derive(Clone)]
pub struct Restore {
    client: Client,
    table_name: String,
    target: MapVersionId,
    expected_head: Option<MapVersionId>,
    request_token: Option<String>,
}

impl Restore {
    pub fn expected_head(mut self, version: MapVersionId) -> Self {
        self.expected_head = Some(version);
        self
    }

    pub fn request_token(mut self, token: impl Into<String>) -> Self {
        self.request_token = Some(token.into());
        self
    }

    pub fn set_request_token(mut self, token: Option<String>) -> Self {
        self.request_token = token;
        self
    }

    pub async fn send(self) -> Result<VersionedMapUpdate> {
        Ok(self.send_with_metadata().await?.output)
    }

    pub async fn send_with_metadata(self) -> Result<WithMetadata<VersionedMapUpdate>> {
        let expected = self
            .expected_head
            .ok_or_else(|| Error::InvalidRequest("restore.expected_head is required".into()))?;
        let result = match self.request_token {
            Some(token) => {
                self.client
                    .core()
                    .restore_idempotent_result(&self.table_name, &expected, &self.target, &token)
                    .await?
            }
            None => {
                self.client
                    .core()
                    .restore_result(&self.table_name, &expected, &self.target)
                    .await?
            }
        };
        let version_id = result.update.current().map(|version| version.id.clone());
        let Some(commit_id) = result.commit_id else {
            return Ok(WithMetadata::single(
                result.update,
                self.table_name,
                version_id,
            ));
        };
        let transition = TableTransitionMetadata::from_update(
            self.table_name.clone(),
            &result.update,
            Some(commit_id.clone()),
            Some(result.table_id),
        );
        Ok(WithMetadata::single_write(
            result.update,
            self.table_name,
            version_id,
            Some(commit_id),
            transition,
        ))
    }
}

fn hex_table_id(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;

    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String is infallible");
    }
    output
}

#[derive(Clone)]
pub struct ConditionalTable {
    client: Client,
    table_name: String,
    version: MapVersionId,
}

impl ConditionalTable {
    pub fn put_item(&self) -> PutItem {
        self.client
            .put_item()
            .table_name(&self.table_name)
            .expected_head(self.version.clone())
    }

    pub fn delete_item(&self) -> DeleteItem {
        self.client
            .delete_item()
            .table_name(&self.table_name)
            .expected_head(self.version.clone())
    }

    pub fn update_item(&self) -> UpdateItem {
        self.client
            .update_item()
            .table_name(&self.table_name)
            .expected_head(self.version.clone())
    }
}

#[derive(Clone)]
pub struct Snapshot {
    client: Client,
    table_name: String,
    version: MapVersionId,
}

impl Snapshot {
    pub fn version(&self) -> &MapVersionId {
        &self.version
    }

    pub fn get_item(&self) -> GetItem {
        GetItem::new(self.client.clone(), Some(self.version.clone())).table_name(&self.table_name)
    }

    pub fn query(&self) -> Query {
        Query::new(self.client.clone(), Some(self.version.clone())).table_name(&self.table_name)
    }

    pub fn scan(&self) -> Scan {
        Scan::new(self.client.clone(), Some(self.version.clone())).table_name(&self.table_name)
    }

    /// Export this exact immutable version rather than the current head.
    pub async fn export(&self, limits: TableArchiveLimits) -> Result<TableArchive> {
        Ok(self
            .client
            .core()
            .export_table(&self.table_name, Some(&self.version), limits)
            .await?)
    }
}
