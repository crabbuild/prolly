use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use clap::{Args, Parser, Subcommand};
use prolly::MapVersionId;
use prolly_dynamodb_client::{
    Client, GcApplyOptions, GcCursor, GcPlan, GcPlanLimits, ImportPlan, IndexReconfigurationPlan,
    IndexReconfigurationPlanId, MaintenanceContext, MaintenanceLeaseId, RetentionPlan,
    RetentionPolicy, SecondaryIndexDefinition, TableArchive, TableArchiveLimits,
};
use prolly_store_dynamodb::DynamoDbBackend;
use serde_json::json;

#[derive(Parser)]
#[command(name = "prolly-dynamodb-admin", version, about)]
struct Cli {
    /// Physical DynamoDB node table. No logical operation uses a native item table.
    #[arg(long, env = "PROLLY_STORE_DYNAMODB_TABLE")]
    physical_table: Option<String>,

    /// Companion root-registry table; defaults to `<physical-table>-roots`.
    #[arg(long, env = "PROLLY_STORE_DYNAMODB_ROOT_TABLE")]
    root_table: Option<String>,

    /// Required tenant/database namespace prefix, interpreted as UTF-8 bytes.
    #[arg(long, env = "PROLLY_STORE_DYNAMODB_KEY_PREFIX")]
    key_prefix: Option<String>,

    /// Optional AWS SDK endpoint, for example DynamoDB Local.
    #[arg(long, env = "PROLLY_STORE_DYNAMODB_ENDPOINT")]
    endpoint: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Explicitly create/validate physical schema and initialize logical format.
    Bootstrap,
    /// Verify physical schema, logical format, and negotiated capabilities.
    Verify,
    /// Export one current or historical logical table version.
    Backup {
        #[arg(long)]
        table: String,
        /// Lowercase/uppercase 64-digit hexadecimal MapVersionId.
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        output: PathBuf,
        #[command(flatten)]
        limits: ArchiveLimitArgs,
    },
    /// Decode and completely verify a canonical archive without provider access.
    VerifyArchive {
        #[arg(long)]
        input: PathBuf,
        #[command(flatten)]
        limits: ArchiveLimitArgs,
    },
    /// Produce a read-only import plan; the target remains absent.
    ImportPlan {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        target_table: String,
        #[arg(long)]
        output: PathBuf,
        #[command(flatten)]
        limits: ArchiveLimitArgs,
    },
    /// Apply a reviewed import plan with durable operator attribution.
    ImportApply {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        plan: PathBuf,
        #[command(flatten)]
        authority: AuthorityArgs,
        #[command(flatten)]
        limits: ArchiveLimitArgs,
    },
    /// Produce a bounded, read-only retention plan.
    RetentionPlan {
        #[arg(long)]
        table: String,
        #[arg(long)]
        keep_last: usize,
        #[arg(long)]
        keep_since_millis: Option<u64>,
        #[arg(long = "protect-version")]
        protected_versions: Vec<String>,
        #[arg(long)]
        output: PathBuf,
    },
    /// Apply a reviewed retention plan with durable operator attribution.
    RetentionApply {
        #[arg(long)]
        plan: PathBuf,
        #[command(flatten)]
        authority: AuthorityArgs,
    },
    /// Produce a read-only clean shadow-build plan for an exact desired index set.
    IndexPlan {
        #[arg(long)]
        table: String,
        /// JSON array of `SecondaryIndexDefinition` values; an empty array removes all indexes.
        #[arg(long)]
        desired: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Build, verify, and atomically activate a reviewed index plan.
    IndexApply {
        #[arg(long)]
        plan: PathBuf,
        #[command(flatten)]
        authority: AuthorityArgs,
    },
    /// Resolve durable evidence for one index activation plan.
    IndexAudit {
        #[arg(long)]
        table: String,
        #[arg(long)]
        plan_id: String,
    },
    /// Inspect the fail-closed global writer fence.
    LeaseStatus,
    /// Acquire the writer fence before destructive physical maintenance.
    LeaseAcquire {
        #[arg(long)]
        duration_millis: u64,
        #[command(flatten)]
        authority: AuthorityArgs,
    },
    /// Release a held writer fence and record operator evidence.
    LeaseRelease {
        #[arg(long)]
        lease_id: String,
        #[command(flatten)]
        authority: AuthorityArgs,
    },
    /// Force-break a crashed holder's fence after its durable expiry.
    LeaseBreakExpired {
        #[arg(long)]
        lease_id: String,
        #[command(flatten)]
        authority: AuthorityArgs,
    },
    /// Produce one bounded global node/blob GC candidate page under a lease.
    GcPlan {
        #[arg(long)]
        lease_id: String,
        /// Optional cursor JSON copied from the preceding plan's `next_cursor`.
        #[arg(long)]
        cursor: Option<PathBuf>,
        #[arg(long)]
        output: PathBuf,
        #[command(flatten)]
        limits: GcLimitArgs,
    },
    /// Apply one reviewed canonical GC plan with durable progress evidence.
    GcApply {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long, default_value_t = 8)]
        blob_delete_parallelism: usize,
        #[command(flatten)]
        authority: AuthorityArgs,
    },
}

#[derive(Clone, Copy, Args)]
struct ArchiveLimitArgs {
    #[arg(long, default_value_t = 1_000_000)]
    max_nodes: usize,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    max_node_bytes: usize,
    #[arg(long, default_value_t = 100_000)]
    max_blobs: usize,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    max_blob_bytes: usize,
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    max_archive_bytes: usize,
}

impl From<ArchiveLimitArgs> for TableArchiveLimits {
    fn from(value: ArchiveLimitArgs) -> Self {
        Self::new(
            value.max_nodes,
            value.max_node_bytes,
            value.max_blobs,
            value.max_blob_bytes,
            value.max_archive_bytes,
        )
    }
}

#[derive(Clone, Copy, Args)]
struct GcLimitArgs {
    #[arg(long, default_value_t = 100_000)]
    max_roots: usize,
    #[arg(long, default_value_t = 10_000_000)]
    max_live_nodes: usize,
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    max_live_node_bytes: usize,
    #[arg(long, default_value_t = 100_000_000)]
    max_scanned_values: usize,
    #[arg(long, default_value_t = 1_000_000)]
    max_live_blobs: usize,
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    max_live_blob_bytes: u64,
    #[arg(long, default_value_t = 1_000)]
    candidate_page_evaluation_limit: usize,
}

impl From<GcLimitArgs> for GcPlanLimits {
    fn from(value: GcLimitArgs) -> Self {
        Self::new(
            value.max_roots,
            value.max_live_nodes,
            value.max_live_node_bytes,
            value.max_scanned_values,
            value.max_live_blobs,
            value.max_live_blob_bytes,
            value.candidate_page_evaluation_limit,
        )
    }
}

#[derive(Args)]
struct AuthorityArgs {
    #[arg(long)]
    actor: String,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    change_ticket: Option<String>,
}

impl AuthorityArgs {
    fn into_context(self) -> MaintenanceContext {
        let context = MaintenanceContext::new(self.actor, self.reason);
        match self.change_ticket {
            Some(ticket) => context.change_ticket(ticket),
            None => context,
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("error: {error:#}");
        std::process::exit(2);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    if let Command::VerifyArchive { input, limits } = &cli.command {
        let archive = read_archive(input, (*limits).into())?;
        print_archive_summary(&archive, (*limits).into())?;
        return Ok(());
    }

    let backend = build_backend(&cli).await?;
    if matches!(&cli.command, Command::Bootstrap) {
        backend
            .initialize_schema()
            .await
            .context("initialize physical DynamoDB schema")?;
    }
    let client = Client::open(backend)
        .await
        .context("open and negotiate versioned DynamoDB namespace")?;

    match cli.command {
        Command::Bootstrap | Command::Verify => {
            println!("{}", client.capabilities().to_json()?);
        }
        Command::Backup {
            table,
            version,
            output,
            limits,
        } => {
            let limits = limits.into();
            let archive = match version {
                Some(version) => {
                    let version = parse_version(&version)?;
                    client.table(table).at(version).export(limits).await?
                }
                None => client.table(table).export(limits).await?,
            };
            let bytes = archive.to_bytes(limits)?;
            write_new_file(&output, &bytes)?;
            print_archive_summary(&archive, limits)?;
        }
        Command::VerifyArchive { .. } => unreachable!("handled before provider construction"),
        Command::ImportPlan {
            archive,
            target_table,
            output,
            limits,
        } => {
            let limits = limits.into();
            let archive = read_archive(&archive, limits)?;
            let import = client.import(archive, target_table, limits);
            let plan = import.plan().await?;
            write_json_file(&output, &plan)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "import_plan",
                    "plan_id": plan.id.to_string(),
                    "target_table": plan.target_table_name,
                    "source_table": plan.source_table_name,
                    "source_version": plan.source_version.to_string(),
                    "mutated": false,
                    "plan_file": output,
                }))?
            );
        }
        Command::ImportApply {
            archive,
            plan,
            authority,
            limits,
        } => {
            let limits = limits.into();
            let archive = read_archive(&archive, limits)?;
            let plan: ImportPlan = read_json_file(&plan)?;
            let import = client.import(archive, plan.target_table_name.clone(), limits);
            let result = import.apply(&plan, authority.into_context()).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "import_apply",
                    "plan_id": result.plan_id.to_string(),
                    "table": result.description.name,
                    "version": result.version.to_string(),
                    "commit_id": result.commit_id.to_string(),
                    "completed_at_millis": result.completed_at_millis,
                    "replayed": result.replayed,
                }))?
            );
        }
        Command::RetentionPlan {
            table,
            keep_last,
            keep_since_millis,
            protected_versions,
            output,
        } => {
            let mut policy = RetentionPolicy::keep_last(keep_last);
            if let Some(cutoff) = keep_since_millis {
                policy = policy.keep_since_millis(cutoff);
            }
            for version in protected_versions {
                policy = policy.protect(parse_version(&version)?);
            }
            let plan = client.table(table).retention(policy).plan().await?;
            write_json_file(&output, &plan)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "retention_plan",
                    "plan_id": plan.id.to_string(),
                    "table": plan.table_name,
                    "remove_count": plan.remove.len(),
                    "examined_versions": plan.examined_versions,
                    "more_removable": plan.more_removable,
                    "mutated": false,
                    "plan_file": output,
                }))?
            );
        }
        Command::RetentionApply { plan, authority } => {
            let plan: RetentionPlan = read_json_file(&plan)?;
            let result = client
                .table(&plan.table_name)
                .apply_retention(&plan, authority.into_context())
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "retention_apply",
                    "plan_id": result.plan_id.to_string(),
                    "removed_count": result.removed.len(),
                    "completed_at_millis": result.completed_at_millis,
                    "replayed": result.replayed,
                }))?
            );
        }
        Command::IndexPlan {
            table,
            desired,
            output,
        } => {
            let desired: Vec<SecondaryIndexDefinition> = read_json_file(&desired)?;
            let plan = client.table(&table).indexes(desired).plan().await?;
            write_json_file(&output, &plan)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "index_plan",
                    "plan_id": plan.id.to_string(),
                    "table": plan.table_name,
                    "expected_head": plan.expected_head.to_string(),
                    "before_count": plan.before.secondary_indexes.len(),
                    "after_count": plan.after.secondary_indexes.len(),
                    "mutated": false,
                    "plan_file": output,
                }))?
            );
        }
        Command::IndexApply { plan, authority } => {
            let plan: IndexReconfigurationPlan = read_json_file(&plan)?;
            let result = client
                .table(&plan.table_name)
                .apply_indexes(&plan, authority.into_context())
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "index_apply",
                    "plan_id": result.plan_id.to_string(),
                    "table": result.description.name,
                    "version": result.version.to_string(),
                    "indexed_source_version": result.indexed_source_version.to_string(),
                    "indexed_snapshot_id": hex(result.indexed_snapshot_id.as_cid().as_bytes()),
                    "commit_id": result.commit_id.to_string(),
                    "completed_at_millis": result.completed_at_millis,
                    "replayed": result.replayed,
                }))?
            );
        }
        Command::IndexAudit { table, plan_id } => {
            let id = IndexReconfigurationPlanId(decode_hex_32(&plan_id)?);
            let audit = client.table(table).indexes_audit(&id).await?;
            println!("{}", serde_json::to_string_pretty(&audit)?);
        }
        Command::LeaseStatus => match client.maintenance_lease().await? {
            Some(lease) => println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "lease_status",
                    "active": true,
                    "lease_id": lease.id.to_string(),
                    "actor": lease.context.actor,
                    "reason": lease.context.reason,
                    "change_ticket": lease.context.change_ticket,
                    "acquired_at_millis": lease.acquired_at_millis,
                    "expires_at_millis": lease.expires_at_millis,
                    "expiry_does_not_auto_release": true,
                }))?
            ),
            None => println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "lease_status",
                    "active": false,
                }))?
            ),
        },
        Command::LeaseAcquire {
            duration_millis,
            authority,
        } => {
            let lease = client
                .acquire_maintenance_lease(authority.into_context(), duration_millis)
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "lease_acquire",
                    "lease_id": lease.id.to_string(),
                    "acquired_at_millis": lease.acquired_at_millis,
                    "expires_at_millis": lease.expires_at_millis,
                    "expiry_does_not_auto_release": true,
                }))?
            );
        }
        Command::LeaseRelease {
            lease_id,
            authority,
        } => {
            let id = MaintenanceLeaseId(decode_hex_32(&lease_id)?);
            let release = client
                .release_maintenance_lease(&id, authority.into_context())
                .await?;
            print_lease_release("lease_release", &release)?;
        }
        Command::LeaseBreakExpired {
            lease_id,
            authority,
        } => {
            let id = MaintenanceLeaseId(decode_hex_32(&lease_id)?);
            let release = client
                .break_expired_maintenance_lease(&id, authority.into_context())
                .await?;
            print_lease_release("lease_break_expired", &release)?;
        }
        Command::GcPlan {
            lease_id,
            cursor,
            output,
            limits,
        } => {
            let id = MaintenanceLeaseId(decode_hex_32(&lease_id)?);
            let cursor = cursor
                .as_deref()
                .map(read_json_file::<GcCursor>)
                .transpose()?;
            let plan = client.plan_gc(&id, cursor.as_ref(), limits.into()).await?;
            write_json_file(&output, &plan)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "gc_plan",
                    "plan_id": plan.id.to_string(),
                    "lease_id": plan.lease_id.to_string(),
                    "roots_digest": hex(plan.roots_digest.as_bytes()),
                    "retained_roots": plan.retained_roots,
                    "protected_trees": plan.protected_trees,
                    "live_nodes": plan.live_nodes,
                    "live_node_bytes": plan.live_node_bytes,
                    "scanned_blob_nodes": plan.scanned_blob_nodes,
                    "scanned_values": plan.scanned_values,
                    "live_blobs": plan.live_blobs,
                    "live_blob_bytes": plan.live_blob_bytes,
                    "examined_node_candidates": plan.examined_node_candidates,
                    "reclaimable_nodes": plan.reclaimable_nodes.len(),
                    "examined_blob_candidates": plan.examined_blob_candidates,
                    "reclaimable_blobs": plan.reclaimable_blobs.len(),
                    "next_cursor": plan.next_cursor,
                    "mutated": false,
                    "plan_file": output,
                }))?
            );
        }
        Command::GcApply {
            plan,
            blob_delete_parallelism,
            authority,
        } => {
            let plan = read_json_file::<GcPlan>(&plan)?;
            let result = client
                .apply_gc(
                    &plan,
                    authority.into_context(),
                    GcApplyOptions {
                        blob_delete_parallelism,
                    },
                )
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "operation": "gc_apply",
                    "plan_id": result.plan_id.to_string(),
                    "lease_id": result.lease_id.to_string(),
                    "node_deletes": result.node_deletes,
                    "blob_deletes": result.blob_deletes,
                    "completed_at_millis": result.completed_at_millis,
                    "replayed": result.replayed,
                    "lease_remains_held": true,
                }))?
            );
        }
    }
    Ok(())
}

async fn build_backend(cli: &Cli) -> anyhow::Result<DynamoDbBackend> {
    let physical_table = cli
        .physical_table
        .as_deref()
        .context("--physical-table is required for provider operations")?;
    let key_prefix = cli
        .key_prefix
        .as_deref()
        .context("--key-prefix is required for provider operations")?;
    if key_prefix.is_empty() {
        bail!("--key-prefix must be nonempty to prevent namespace collisions");
    }
    let shared = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let mut builder = aws_sdk_dynamodb::config::Builder::from(&shared);
    if let Some(endpoint) = &cli.endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    let mut backend = DynamoDbBackend::new(
        aws_sdk_dynamodb::Client::from_conf(builder.build()),
        physical_table,
    )
    .with_key_prefix(key_prefix.as_bytes().to_vec());
    if let Some(root_table) = &cli.root_table {
        backend = backend.with_root_table_name(root_table);
    }
    Ok(backend)
}

fn read_archive(path: &Path, limits: TableArchiveLimits) -> anyhow::Result<TableArchive> {
    let bytes = fs::read(path).with_context(|| format!("read archive {}", path.display()))?;
    TableArchive::from_bytes(&bytes, limits)
        .with_context(|| format!("verify archive {}", path.display()))
}

fn print_archive_summary(archive: &TableArchive, limits: TableArchiveLimits) -> anyhow::Result<()> {
    let summary = archive.verify(limits)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "operation": "archive_verify",
            "archive_digest": hex(summary.archive_digest.as_bytes()),
            "source_table": archive.source.name,
            "source_table_id": hex(&archive.source.id.0),
            "version": summary.version.to_string(),
            "node_count": summary.snapshot.node_count,
            "node_bytes": summary.snapshot.byte_count,
            "blob_count": summary.blob_count,
            "blob_bytes": summary.blob_bytes,
            "encoded_bytes": summary.encoded_bytes,
        }))?
    );
    Ok(())
}

fn write_json_file(path: &Path, value: &impl serde::Serialize) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_new_file(path, &bytes)
}

fn print_lease_release(
    operation: &str,
    release: &prolly_dynamodb_client::MaintenanceLeaseRelease,
) -> anyhow::Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "operation": operation,
            "lease_id": release.lease.id.to_string(),
            "released_at_millis": release.released_at_millis,
            "forced_after_expiry": release.forced_after_expiry,
            "replayed": release.replayed,
        }))?
    );
    Ok(())
}

fn read_json_file<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let bytes = fs::read(path).with_context(|| format!("read plan {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("decode plan {}", path.display()))
}

fn write_new_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create new output file {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write output file {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync output file {}", path.display()))?;
    Ok(())
}

fn parse_version(value: &str) -> anyhow::Result<MapVersionId> {
    let bytes = decode_hex_32(value).context("version must be exactly 64 hexadecimal digits")?;
    MapVersionId::from_bytes(&bytes).context("decode MapVersionId")
}

fn decode_hex_32(value: &str) -> anyhow::Result<[u8; 32]> {
    if value.len() != 64 {
        bail!("expected 64 hexadecimal digits");
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> anyhow::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hexadecimal digit"),
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_hex_parser_is_exact_and_case_insensitive() {
        let lower = "0123456789abcdef".repeat(4);
        let upper = lower.to_uppercase();
        assert_eq!(
            decode_hex_32(&lower).unwrap(),
            decode_hex_32(&upper).unwrap()
        );
        assert!(decode_hex_32("00").is_err());
        assert!(decode_hex_32(&"g0".repeat(32)).is_err());
    }

    #[test]
    fn output_creation_never_overwrites_existing_evidence() {
        let path = std::env::temp_dir().join(format!(
            "prolly-ddb-admin-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        write_new_file(&path, b"first").unwrap();
        assert!(write_new_file(&path, b"second").is_err());
        assert_eq!(fs::read(&path).unwrap(), b"first");
        fs::remove_file(path).unwrap();
    }
}
