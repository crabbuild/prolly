use std::error::Error as StdError;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prolly_dynamodb_core::{
    Item, MaintenanceContext, MaintenanceLease, MaintenanceLeaseRelease, TableCommit, TableId,
    TtlCandidate, WorkerCheckpoint, WorkerJobId, WorkerKind, WorkerLease, WorkerLeaseRelease,
    WorkerProgress, MAX_COMMIT_PAGE_ITEMS, MAX_WORKER_LEASE_MILLIS, MIN_WORKER_LEASE_MILLIS,
};
use serde::Serialize;
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;

use crate::{Client, Error, GcApplyOptions, GcApplyResult, GcCursor, GcPlan, GcPlanLimits, Result};

/// Bound both source reads and retained in-process work.
pub const MAX_WORKER_PAGE_ITEMS: usize = MAX_COMMIT_PAGE_ITEMS;
pub const MIN_WORKER_SLEEP: Duration = Duration::from_millis(1);
pub const MAX_WORKER_SLEEP: Duration = Duration::from_secs(60 * 60);

/// Common observable state for an explicitly constructed worker.
pub trait Worker {
    fn job_id(&self) -> &WorkerJobId;
    fn lease(&self) -> &WorkerLease;
    fn checkpoint(&self) -> Option<&WorkerCheckpoint>;
}

/// Namespace for explicit worker constructors. This value starts no task.
#[derive(Clone)]
pub struct Workers {
    client: Client,
}

impl Workers {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Resolve a table incarnation and acquire its stream subscription lease.
    #[tracing::instrument(
        name = "prolly_dynamodb.StreamWorkerOpen",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "StreamWorkerOpen"),
        err
    )]
    pub async fn stream(&self, options: StreamWorkerOptions) -> Result<StreamWorker> {
        options.validate()?;
        let description = self
            .client
            .core()
            .describe_table(&options.table_name)
            .await?;
        let configuration = StreamConfiguration {
            format_version: 1,
            table_id: &description.id,
            subscription_id: &options.subscription_id,
        };
        let canonical = encode_configuration(&configuration, "stream")?;
        let digest = WorkerJobId::configuration_digest(&canonical);
        let job_id = WorkerJobId::for_configuration(WorkerKind::Stream, &canonical);
        let checkpoint = self.client.core().worker_checkpoint(&job_id).await?;
        validate_checkpoint(
            checkpoint.as_ref(),
            &job_id,
            WorkerKind::Stream,
            digest,
            &description.id,
        )?;
        let lease = self
            .client
            .core()
            .acquire_worker_lease(
                job_id.clone(),
                WorkerKind::Stream,
                digest,
                options.owner_id.clone(),
                options.lease_duration_millis,
            )
            .await?;
        Ok(StreamWorker {
            client: self.client.clone(),
            options,
            table_id: description.id,
            job_id,
            lease,
            checkpoint,
        })
    }

    /// Resolve a table incarnation and acquire its TTL scanner lease.
    #[tracing::instrument(
        name = "prolly_dynamodb.TtlWorkerOpen",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "TtlWorkerOpen"),
        err
    )]
    pub async fn ttl(&self, options: TtlWorkerOptions) -> Result<TtlWorker> {
        options.validate()?;
        let description = self
            .client
            .core()
            .validate_ttl_configuration(&options.table_name, &options.ttl_attribute)
            .await?;
        let configuration = TtlConfiguration {
            format_version: 1,
            table_id: &description.id,
            ttl_attribute: &options.ttl_attribute,
        };
        let canonical = encode_configuration(&configuration, "TTL")?;
        let digest = WorkerJobId::configuration_digest(&canonical);
        let job_id = WorkerJobId::for_configuration(WorkerKind::Ttl, &canonical);
        let checkpoint = self.client.core().worker_checkpoint(&job_id).await?;
        validate_checkpoint(
            checkpoint.as_ref(),
            &job_id,
            WorkerKind::Ttl,
            digest,
            &description.id,
        )?;
        let lease = self
            .client
            .core()
            .acquire_worker_lease(
                job_id.clone(),
                WorkerKind::Ttl,
                digest,
                options.owner_id.clone(),
                options.lease_duration_millis,
            )
            .await?;
        Ok(TtlWorker {
            client: self.client.clone(),
            options,
            table_id: description.id,
            job_id,
            lease,
            checkpoint,
        })
    }

    /// Acquire the namespace-wide fail-closed fence for an explicit bounded
    /// maintenance session. No GC page is planned or applied automatically.
    #[tracing::instrument(
        name = "prolly_dynamodb.MaintenanceWorkerOpen",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "MaintenanceWorkerOpen"),
        err
    )]
    pub async fn maintenance(
        &self,
        context: MaintenanceContext,
        duration_millis: u64,
    ) -> Result<MaintenanceWorker> {
        let lease = self
            .client
            .acquire_maintenance_lease(context, duration_millis)
            .await?;
        Ok(MaintenanceWorker {
            client: self.client.clone(),
            lease: Some(lease),
            release: None,
        })
    }
}

/// Explicit operator-controlled physical-maintenance session. Planning remains
/// read-only; every apply still requires its own reviewed plan and attribution.
pub struct MaintenanceWorker {
    client: Client,
    lease: Option<MaintenanceLease>,
    release: Option<MaintenanceLeaseRelease>,
}

impl MaintenanceWorker {
    pub fn lease(&self) -> Option<&MaintenanceLease> {
        self.lease.as_ref()
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.MaintenancePlanGc",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "MaintenancePlanGc"),
        err
    )]
    pub async fn plan_gc(&self, cursor: Option<&GcCursor>, limits: GcPlanLimits) -> Result<GcPlan> {
        let lease = self.active_lease()?;
        self.client.plan_gc(&lease.id, cursor, limits).await
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.MaintenanceApplyGc",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "MaintenanceApplyGc"),
        err
    )]
    pub async fn apply_gc(
        &self,
        plan: &GcPlan,
        context: MaintenanceContext,
        options: GcApplyOptions,
    ) -> Result<GcApplyResult> {
        let lease = self.active_lease()?;
        if plan.lease_id != lease.id {
            return Err(Error::InvalidRequest(
                "GC plan belongs to another maintenance lease".into(),
            ));
        }
        self.client.apply_gc(plan, context, options).await
    }

    /// Durably release the global fence. Dropping this value never releases it;
    /// operators must reconcile or explicitly break an expired lease.
    #[tracing::instrument(
        name = "prolly_dynamodb.MaintenanceWorkerShutdown",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "MaintenanceWorkerShutdown"),
        err
    )]
    pub async fn shutdown(
        &mut self,
        context: MaintenanceContext,
    ) -> Result<MaintenanceLeaseRelease> {
        if let Some(mut release) = self.release.clone() {
            release.replayed = true;
            return Ok(release);
        }
        let lease = self.active_lease()?.clone();
        let release = self
            .client
            .release_maintenance_lease(&lease.id, context)
            .await?;
        self.lease = None;
        self.release = Some(release.clone());
        Ok(release)
    }

    fn active_lease(&self) -> Result<&MaintenanceLease> {
        self.lease.as_ref().ok_or_else(|| {
            Error::InvalidRequest("maintenance worker lease was already released".into())
        })
    }
}

#[derive(Serialize)]
struct StreamConfiguration<'a> {
    format_version: u8,
    table_id: &'a TableId,
    subscription_id: &'a str,
}

#[derive(Serialize)]
struct TtlConfiguration<'a> {
    format_version: u8,
    table_id: &'a TableId,
    ttl_attribute: &'a str,
}

fn encode_configuration(value: &impl Serialize, kind: &str) -> Result<Vec<u8>> {
    serde_cbor::to_vec(value).map_err(|error| {
        Error::InvalidRequest(format!("encode {kind} worker configuration: {error}"))
    })
}

#[derive(Clone, Debug)]
pub struct StreamWorkerOptions {
    table_name: String,
    subscription_id: String,
    owner_id: String,
    lease_duration_millis: u64,
    page_size: usize,
    idle_delay: Duration,
}

impl StreamWorkerOptions {
    pub fn new(
        table_name: impl Into<String>,
        subscription_id: impl Into<String>,
        owner_id: impl Into<String>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            subscription_id: subscription_id.into(),
            owner_id: owner_id.into(),
            lease_duration_millis: 30_000,
            page_size: 100,
            idle_delay: Duration::from_secs(1),
        }
    }

    pub fn lease_duration(mut self, value: Duration) -> Result<Self> {
        self.lease_duration_millis = duration_millis(value, "worker lease duration")?;
        Ok(self)
    }

    pub fn page_size(mut self, value: usize) -> Self {
        self.page_size = value;
        self
    }

    pub fn idle_delay(mut self, value: Duration) -> Self {
        self.idle_delay = value;
        self
    }

    fn validate(&self) -> Result<()> {
        validate_name(&self.subscription_id, "stream subscription ID")?;
        validate_name(&self.owner_id, "worker owner ID")?;
        validate_runtime_options(self.lease_duration_millis, self.page_size, self.idle_delay)
    }
}

#[derive(Clone, Debug)]
pub struct TtlWorkerOptions {
    table_name: String,
    ttl_attribute: String,
    owner_id: String,
    lease_duration_millis: u64,
    page_size: usize,
    idle_delay: Duration,
}

impl TtlWorkerOptions {
    pub fn new(
        table_name: impl Into<String>,
        ttl_attribute: impl Into<String>,
        owner_id: impl Into<String>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            ttl_attribute: ttl_attribute.into(),
            owner_id: owner_id.into(),
            lease_duration_millis: 30_000,
            page_size: 100,
            idle_delay: Duration::from_secs(1),
        }
    }

    pub fn lease_duration(mut self, value: Duration) -> Result<Self> {
        self.lease_duration_millis = duration_millis(value, "worker lease duration")?;
        Ok(self)
    }

    pub fn page_size(mut self, value: usize) -> Self {
        self.page_size = value;
        self
    }

    pub fn idle_delay(mut self, value: Duration) -> Self {
        self.idle_delay = value;
        self
    }

    fn validate(&self) -> Result<()> {
        validate_name(&self.owner_id, "worker owner ID")?;
        if self.ttl_attribute.is_empty() || self.ttl_attribute.len() > 255 {
            return Err(Error::InvalidRequest(
                "TTL attribute name must contain 1..=255 bytes".into(),
            ));
        }
        validate_runtime_options(self.lease_duration_millis, self.page_size, self.idle_delay)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRun {
    pub delivered: usize,
    pub delivered_through_sequence: Option<u64>,
    pub more_available: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtlRun {
    pub evaluated: usize,
    pub deleted: usize,
    pub completed_cycle: bool,
    pub cycle: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerExit {
    pub release: WorkerLeaseRelease,
}

pub struct StreamWorker {
    client: Client,
    options: StreamWorkerOptions,
    table_id: TableId,
    job_id: WorkerJobId,
    lease: WorkerLease,
    checkpoint: Option<WorkerCheckpoint>,
}

impl Worker for StreamWorker {
    fn job_id(&self) -> &WorkerJobId {
        &self.job_id
    }

    fn lease(&self) -> &WorkerLease {
        &self.lease
    }

    fn checkpoint(&self) -> Option<&WorkerCheckpoint> {
        self.checkpoint.as_ref()
    }
}

impl StreamWorker {
    /// Deliver one bounded page sequentially. A sink must deduplicate by the
    /// stable `commit_id` when it needs effectively-once external effects.
    #[tracing::instrument(
        name = "prolly_dynamodb.StreamWorkerRunOnce",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "StreamWorkerRunOnce"),
        err
    )]
    pub async fn run_once<F, Fut, E>(&mut self, sink: &mut F) -> Result<StreamRun>
    where
        F: FnMut(TableCommit) -> Fut + Send,
        Fut: Future<Output = std::result::Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.run_page(sink, None).await
    }

    async fn run_page<F, Fut, E>(
        &mut self,
        sink: &mut F,
        cancellation: Option<&CancellationToken>,
    ) -> Result<StreamRun>
    where
        F: FnMut(TableCommit) -> Fut + Send,
        Fut: Future<Output = std::result::Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.renew().await?;
        let after = stream_sequence(self.checkpoint.as_ref())?;
        let page = self
            .client
            .core()
            .commits_for_incarnation(
                &self.options.table_name,
                &self.table_id,
                after,
                self.options.page_size,
            )
            .await?;
        let mut more_available = page.last_sequence.is_some();
        let mut delivered = 0;
        let mut delivered_through_sequence = after;
        for commit in page.commits {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                more_available = true;
                break;
            }
            self.deliver_with_heartbeat(sink, commit.clone()).await?;
            self.renew().await?;
            let checkpoint = self
                .client
                .core()
                .update_worker_checkpoint(
                    &self.lease,
                    self.checkpoint.as_ref().map(|value| value.revision),
                    WorkerProgress::Stream {
                        table_id: self.table_id.clone(),
                        delivered_through_sequence: commit.sequence,
                    },
                )
                .await?;
            self.checkpoint = Some(checkpoint);
            delivered += 1;
            delivered_through_sequence = Some(commit.sequence);
        }
        Ok(StreamRun {
            delivered,
            delivered_through_sequence,
            more_available,
        })
    }

    /// Cancellation is observed between deliveries. An in-flight sink call is
    /// allowed to finish and be checkpointed before the lease is released.
    #[tracing::instrument(
        name = "prolly_dynamodb.StreamWorkerRun",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "StreamWorkerRun"),
        err
    )]
    pub async fn run<F, Fut, E>(
        &mut self,
        cancellation: &CancellationToken,
        mut sink: F,
    ) -> Result<WorkerExit>
    where
        F: FnMut(TableCommit) -> Fut + Send,
        Fut: Future<Output = std::result::Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        loop {
            if cancellation.is_cancelled() {
                return self.shutdown().await;
            }
            let run = match self.run_page(&mut sink, Some(cancellation)).await {
                Ok(run) => run,
                Err(error) => {
                    let _ = self.shutdown().await;
                    return Err(error);
                }
            };
            if run.delivered == 0 && self.wait_or_cancel(cancellation).await? {
                return self.shutdown().await;
            }
        }
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.StreamWorkerShutdown",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "StreamWorkerShutdown"),
        err
    )]
    pub async fn shutdown(&mut self) -> Result<WorkerExit> {
        let release = self.client.core().release_worker_lease(&self.lease).await?;
        Ok(WorkerExit { release })
    }

    async fn renew(&mut self) -> Result<()> {
        self.lease = self
            .client
            .core()
            .renew_worker_lease(&self.lease, self.options.lease_duration_millis)
            .await?;
        Ok(())
    }

    async fn wait_or_cancel(&mut self, cancellation: &CancellationToken) -> Result<bool> {
        let deadline = Instant::now() + self.options.idle_delay;
        loop {
            let heartbeat = Instant::now() + self.heartbeat_delay();
            let wake = deadline.min(heartbeat);
            tokio::select! {
                () = cancellation.cancelled() => return Ok(true),
                () = sleep_until(wake) => {}
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            self.renew().await?;
        }
    }

    fn heartbeat_delay(&self) -> Duration {
        Duration::from_millis((self.options.lease_duration_millis / 3).max(1))
    }

    async fn deliver_with_heartbeat<F, Fut, E>(
        &mut self,
        sink: &mut F,
        commit: TableCommit,
    ) -> Result<()>
    where
        F: FnMut(TableCommit) -> Fut + Send,
        Fut: Future<Output = std::result::Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        let delivery = sink(commit);
        tokio::pin!(delivery);
        loop {
            let heartbeat = sleep_until(Instant::now() + self.heartbeat_delay());
            tokio::pin!(heartbeat);
            tokio::select! {
                result = &mut delivery => {
                    return result.map_err(|source| Error::WorkerSink {
                        source: Box::new(source),
                    });
                }
                () = &mut heartbeat => self.renew().await?,
            }
        }
    }
}

pub struct TtlWorker {
    client: Client,
    options: TtlWorkerOptions,
    table_id: TableId,
    job_id: WorkerJobId,
    lease: WorkerLease,
    checkpoint: Option<WorkerCheckpoint>,
}

impl Worker for TtlWorker {
    fn job_id(&self) -> &WorkerJobId {
        &self.job_id
    }

    fn lease(&self) -> &WorkerLease {
        &self.lease
    }

    fn checkpoint(&self) -> Option<&WorkerCheckpoint> {
        self.checkpoint.as_ref()
    }
}

impl TtlWorker {
    #[tracing::instrument(
        name = "prolly_dynamodb.TtlWorkerRunOnce",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "TtlWorkerRunOnce"),
        err
    )]
    pub async fn run_once(&mut self) -> Result<TtlRun> {
        self.run_once_at(unix_epoch_seconds()?).await
    }

    /// Deterministic entry point for tests and controlled expiry sweeps.
    #[tracing::instrument(
        name = "prolly_dynamodb.TtlWorkerRunOnceAt",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "TtlWorkerRunOnceAt"),
        err
    )]
    pub async fn run_once_at(&mut self, now_epoch_seconds: u64) -> Result<TtlRun> {
        self.renew().await?;
        let state = ttl_state(self.checkpoint.as_ref())?;
        let page = self
            .client
            .core()
            .ttl_candidates(
                &self.options.table_name,
                &self.table_id,
                &self.options.ttl_attribute,
                state.last_evaluated_key.as_ref(),
                self.options.page_size,
                now_epoch_seconds,
            )
            .await?;
        let evaluated = page.evaluated;
        let mut deleted = 0usize;
        let heartbeat_delay =
            Duration::from_millis((self.options.lease_duration_millis / 3).max(1));
        let mut next_renewal = Instant::now() + heartbeat_delay;
        for candidate in &page.candidates {
            if Instant::now() >= next_renewal {
                self.renew().await?;
                next_renewal = Instant::now() + heartbeat_delay;
            }
            if self
                .delete_with_heartbeat(candidate, now_epoch_seconds)
                .await?
            {
                deleted += 1;
            }
        }
        self.renew().await?;
        let completed_cycle = page.last_evaluated_key.is_none();
        let cycle = if completed_cycle {
            state
                .cycle
                .checked_add(1)
                .ok_or_else(|| Error::InvalidRequest("TTL worker cycle exhausted".into()))?
        } else {
            state.cycle
        };
        let evaluated_total = checked_counter(state.evaluated_total, evaluated, "evaluated")?;
        let deleted_total = checked_counter(state.deleted_total, deleted, "deleted")?;
        let checkpoint = self
            .client
            .core()
            .update_worker_checkpoint(
                &self.lease,
                self.checkpoint.as_ref().map(|value| value.revision),
                WorkerProgress::Ttl {
                    table_id: self.table_id.clone(),
                    cycle,
                    last_evaluated_key: page.last_evaluated_key,
                    evaluated_total,
                    deleted_total,
                },
            )
            .await?;
        self.checkpoint = Some(checkpoint);
        Ok(TtlRun {
            evaluated,
            deleted,
            completed_cycle,
            cycle,
        })
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.TtlWorkerRun",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "TtlWorkerRun"),
        err
    )]
    pub async fn run(&mut self, cancellation: &CancellationToken) -> Result<WorkerExit> {
        loop {
            if cancellation.is_cancelled() {
                return self.shutdown().await;
            }
            let run = match self.run_once().await {
                Ok(run) => run,
                Err(error) => {
                    let _ = self.shutdown().await;
                    return Err(error);
                }
            };
            if run.completed_cycle && self.wait_or_cancel(cancellation).await? {
                return self.shutdown().await;
            }
        }
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.TtlWorkerShutdown",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "TtlWorkerShutdown"),
        err
    )]
    pub async fn shutdown(&mut self) -> Result<WorkerExit> {
        let release = self.client.core().release_worker_lease(&self.lease).await?;
        Ok(WorkerExit { release })
    }

    async fn renew(&mut self) -> Result<()> {
        self.lease = self
            .client
            .core()
            .renew_worker_lease(&self.lease, self.options.lease_duration_millis)
            .await?;
        Ok(())
    }

    async fn wait_or_cancel(&mut self, cancellation: &CancellationToken) -> Result<bool> {
        let deadline = Instant::now() + self.options.idle_delay;
        loop {
            let heartbeat = Instant::now() + self.heartbeat_delay();
            let wake = deadline.min(heartbeat);
            tokio::select! {
                () = cancellation.cancelled() => return Ok(true),
                () = sleep_until(wake) => {}
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            self.renew().await?;
        }
    }

    async fn delete_with_heartbeat(
        &mut self,
        candidate: &TtlCandidate,
        now_epoch_seconds: u64,
    ) -> Result<bool> {
        let client = self.client.clone();
        let table_name = self.options.table_name.clone();
        let ttl_attribute = self.options.ttl_attribute.clone();
        let table_id = self.table_id.clone();
        let deletion = async move {
            client
                .core()
                .expire_ttl_candidate(
                    &table_name,
                    &table_id,
                    &ttl_attribute,
                    candidate,
                    now_epoch_seconds,
                )
                .await
        };
        tokio::pin!(deletion);
        loop {
            let heartbeat = sleep_until(Instant::now() + self.heartbeat_delay());
            tokio::pin!(heartbeat);
            tokio::select! {
                result = &mut deletion => {
                    return match result {
                        Ok(deleted) => Ok(deleted),
                        Err(error) => Err(error.into()),
                    };
                }
                () = &mut heartbeat => self.renew().await?,
            }
        }
    }

    fn heartbeat_delay(&self) -> Duration {
        Duration::from_millis((self.options.lease_duration_millis / 3).max(1))
    }
}

#[derive(Default)]
struct TtlState {
    cycle: u64,
    last_evaluated_key: Option<Item>,
    evaluated_total: u64,
    deleted_total: u64,
}

fn stream_sequence(checkpoint: Option<&WorkerCheckpoint>) -> Result<Option<u64>> {
    match checkpoint.map(|value| &value.progress) {
        None => Ok(None),
        Some(WorkerProgress::Stream {
            delivered_through_sequence,
            ..
        }) => Ok(Some(*delivered_through_sequence)),
        Some(_) => Err(Error::InvalidRequest(
            "stream worker checkpoint contains TTL progress".into(),
        )),
    }
}

fn ttl_state(checkpoint: Option<&WorkerCheckpoint>) -> Result<TtlState> {
    match checkpoint.map(|value| &value.progress) {
        None => Ok(TtlState::default()),
        Some(WorkerProgress::Ttl {
            cycle,
            last_evaluated_key,
            evaluated_total,
            deleted_total,
            ..
        }) => Ok(TtlState {
            cycle: *cycle,
            last_evaluated_key: last_evaluated_key.clone(),
            evaluated_total: *evaluated_total,
            deleted_total: *deleted_total,
        }),
        Some(_) => Err(Error::InvalidRequest(
            "TTL worker checkpoint contains stream progress".into(),
        )),
    }
}

fn validate_checkpoint(
    checkpoint: Option<&WorkerCheckpoint>,
    job_id: &WorkerJobId,
    kind: WorkerKind,
    digest: [u8; 32],
    table_id: &TableId,
) -> Result<()> {
    let Some(checkpoint) = checkpoint else {
        return Ok(());
    };
    let progress_table = match &checkpoint.progress {
        WorkerProgress::Stream { table_id, .. } | WorkerProgress::Ttl { table_id, .. } => table_id,
    };
    if &checkpoint.job_id != job_id
        || checkpoint.kind != kind
        || checkpoint.configuration_digest != digest
        || progress_table != table_id
    {
        return Err(Error::InvalidRequest(
            "worker checkpoint does not match its job configuration/table incarnation".into(),
        ));
    }
    Ok(())
}

fn validate_name(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > 256 {
        return Err(Error::InvalidRequest(format!(
            "{label} must contain 1..=256 bytes"
        )));
    }
    Ok(())
}

fn validate_runtime_options(
    lease_duration_millis: u64,
    page_size: usize,
    idle_delay: Duration,
) -> Result<()> {
    if !(MIN_WORKER_LEASE_MILLIS..=MAX_WORKER_LEASE_MILLIS).contains(&lease_duration_millis) {
        return Err(Error::InvalidRequest(format!(
            "worker lease duration must be {MIN_WORKER_LEASE_MILLIS}..={MAX_WORKER_LEASE_MILLIS} milliseconds"
        )));
    }
    if !(1..=MAX_WORKER_PAGE_ITEMS).contains(&page_size) {
        return Err(Error::InvalidRequest(format!(
            "worker page size must be 1..={MAX_WORKER_PAGE_ITEMS}"
        )));
    }
    if !(MIN_WORKER_SLEEP..=MAX_WORKER_SLEEP).contains(&idle_delay) {
        return Err(Error::InvalidRequest(format!(
            "worker idle delay must be {:?}..={:?}",
            MIN_WORKER_SLEEP, MAX_WORKER_SLEEP
        )));
    }
    Ok(())
}

fn duration_millis(value: Duration, label: &str) -> Result<u64> {
    u64::try_from(value.as_millis())
        .map_err(|_| Error::InvalidRequest(format!("{label} exceeds u64 milliseconds")))
}

fn unix_epoch_seconds() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .map_err(|_| Error::InvalidRequest("system clock precedes Unix epoch".into()))
}

fn checked_counter(current: u64, increment: usize, label: &str) -> Result<u64> {
    let increment = u64::try_from(increment)
        .map_err(|_| Error::InvalidRequest(format!("TTL {label} counter overflow")))?;
    current
        .checked_add(increment)
        .ok_or_else(|| Error::InvalidRequest(format!("TTL {label} counter exhausted")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_options_are_strictly_bounded() {
        assert!(validate_runtime_options(MIN_WORKER_LEASE_MILLIS, 1, MIN_WORKER_SLEEP).is_ok());
        assert!(validate_runtime_options(0, 1, MIN_WORKER_SLEEP).is_err());
        assert!(validate_runtime_options(MIN_WORKER_LEASE_MILLIS, 0, MIN_WORKER_SLEEP).is_err());
        assert!(validate_runtime_options(MIN_WORKER_LEASE_MILLIS, 1, Duration::ZERO).is_err());
    }

    #[test]
    fn worker_configuration_identity_is_frozen_and_excludes_runtime_tuning() {
        let table_id = TableId([42; 32]);
        let stream = encode_configuration(
            &StreamConfiguration {
                format_version: 1,
                table_id: &table_id,
                subscription_id: "legal-ledger",
            },
            "stream",
        )
        .unwrap();
        let ttl = encode_configuration(
            &TtlConfiguration {
                format_version: 1,
                table_id: &table_id,
                ttl_attribute: "expiresAt",
            },
            "TTL",
        )
        .unwrap();

        assert_eq!(
            hex(&stream),
            "a36e666f726d61745f76657273696f6e01687461626c655f69649820182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a6f737562736372697074696f6e5f69646c6c6567616c2d6c6564676572"
        );
        assert_eq!(
            WorkerJobId::for_configuration(WorkerKind::Stream, &stream).to_string(),
            "881acdda5fe7249d3f4f9ca203c9433ab0f0abd1e1b8e16236c6958f7f5991eb"
        );
        assert_eq!(
            hex(&ttl),
            "a36e666f726d61745f76657273696f6e01687461626c655f69649820182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a182a6d74746c5f61747472696275746569657870697265734174"
        );
        assert_eq!(
            WorkerJobId::for_configuration(WorkerKind::Ttl, &ttl).to_string(),
            "47660c92d2f0dcb1c0e1f5bd5aeb0c800e559d87203ed8b666e69954ff7a8b2b"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write;
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").unwrap();
        }
        output
    }
}
