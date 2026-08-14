//! Staged tiny-file benchmark for a bucket-level Git-like repository.
//!
//! The workload grows one repository through configurable target cardinalities.
//! Each publication contains at most 10,000 mutations, matching the canonical
//! repository-format limit. Payloads are unique and smaller than 100 bytes.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    types::{BucketVersioningStatus, VersioningConfiguration},
};
use futures_util::{stream, StreamExt};
use prolly_s3_client::{
    core::{
        GcPhase, MergePhase, MergePolicy, NodeCacheSnapshot, ObjectHeaders,
        ProviderPerKeyVersionLimit,
    },
    BulkWriteOptions, CheckoutRef, Client, HmacAttestationSigner, ProviderIdentity, PutObjectInput,
    S3OperationMetrics, S3WireAttemptInterceptor, S3WireAttemptMetrics,
};
#[cfg(feature = "foyer-cache")]
use prolly_s3_client::{FoyerNodeCache, FoyerNodeCacheConfig};
use std::collections::BTreeMap;
#[cfg(feature = "foyer-cache")]
use std::path::PathBuf;

type BenchResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_STAGES: &[usize] = &[10_000, 20_000, 50_000, 100_000, 500_000, 1_000_000];
const MUTATIONS_PER_COMMIT: usize = 10_000;
const LIST_PAGE_SIZE: usize = 1_000;

#[derive(Debug)]
struct LatencySummary {
    count: usize,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_stages() -> BenchResult<Vec<usize>> {
    let raw = std::env::var("PROLLY_RUSTFS_PERF_STAGES").ok();
    let mut stages = match raw {
        Some(raw) => raw
            .split(',')
            .map(|part| part.trim().parse::<usize>())
            .collect::<Result<Vec<_>, _>>()?,
        None => DEFAULT_STAGES.to_vec(),
    };
    if stages.is_empty() || stages.contains(&0) {
        return Err("benchmark stages must be non-empty positive integers".into());
    }
    stages.sort_unstable();
    stages.dedup();
    Ok(stages)
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    sorted[(sorted.len() * percentile).div_ceil(100) - 1]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn summarize(mut samples: Vec<Duration>) -> LatencySummary {
    samples.sort_unstable();
    let total = samples
        .iter()
        .map(|sample| sample.as_secs_f64())
        .sum::<f64>();
    LatencySummary {
        count: samples.len(),
        mean_ms: total * 1_000.0 / samples.len() as f64,
        p50_ms: millis(percentile(&samples, 50)),
        p95_ms: millis(percentile(&samples, 95)),
        p99_ms: millis(percentile(&samples, 99)),
        max_ms: millis(*samples.last().expect("non-empty latency samples")),
    }
}

fn payload(index: usize) -> Vec<u8> {
    format!("prolly-small-file-{index:016x}\n").into_bytes()
}

fn key(index: usize) -> String {
    format!("repo/files/{index:07}.txt")
}

fn print_provider_metrics(label: &str, metrics: S3OperationMetrics) {
    println!(
        "METRICS phase={label} s3_calls={} get={} head={} put={} list={} list_versions={} delete={} delete_batch={} uploaded_bytes={} downloaded_bytes={}",
        metrics.total_calls(),
        metrics.get_object,
        metrics.head_object,
        metrics.put_object,
        metrics.list_objects_v2,
        metrics.list_object_versions,
        metrics.delete_object,
        metrics.delete_objects,
        metrics.uploaded_body_bytes,
        metrics.downloaded_body_bytes,
    );
}

fn print_wire_metrics(label: &str, metrics: S3WireAttemptMetrics) {
    println!(
        "WIRE phase={label} executions={} transmissions={} retries={} success={} client_errors={} server_errors={} no_response={}",
        metrics.executions,
        metrics.transmissions,
        metrics.retry_transmissions(),
        metrics.successful_responses,
        metrics.client_error_responses,
        metrics.server_error_responses,
        metrics.attempts_without_response,
    );
}

fn print_cache_metrics(label: &str, before: NodeCacheSnapshot, after: NodeCacheSnapshot) {
    println!(
        "CACHE phase={label} hits={} misses={} insertions={} coalesced_waits={} ranged_fetches={} fetched_bytes={} avoided_bytes={} admission_rejections={} rss_kib={}",
        after.hits.saturating_sub(before.hits),
        after.misses.saturating_sub(before.misses),
        after.insertions.saturating_sub(before.insertions),
        after.coalesced_waits.saturating_sub(before.coalesced_waits),
        after.ranged_fetches.saturating_sub(before.ranged_fetches),
        after.fetched_bytes.saturating_sub(before.fetched_bytes),
        after.avoided_bytes.saturating_sub(before.avoided_bytes),
        after
            .admission_rejections
            .saturating_sub(before.admission_rejections),
        resident_kib().map_or_else(|| "unknown".to_string(), |rss| rss.to_string()),
    );
}

fn resident_kib() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse()
        .ok()
}

async fn ensure_versioned_bucket(aws: &aws_sdk_s3::Client, bucket: &str) -> BenchResult {
    if let Err(error) = aws.create_bucket().bucket(bucket).send().await {
        let text = format!("{error:?}");
        if !text.contains("BucketAlreadyOwnedByYou") && !text.contains("BucketAlreadyExists") {
            return Err(format!("create bucket {bucket}: {text}").into());
        }
    }
    aws.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 12)]
async fn main() -> BenchResult {
    let endpoint = env("PROLLY_RUSTFS_ENDPOINT", "http://127.0.0.1:9000");
    let access_key = env("PROLLY_RUSTFS_ACCESS_KEY", "prollyadmin");
    let secret_key = env("PROLLY_RUSTFS_SECRET_KEY", "prolly-local-secret-change-me");
    let bucket = env("PROLLY_RUSTFS_BUCKET", "prolly");
    let read_sample_size = env("PROLLY_RUSTFS_PERF_READ_SAMPLES", "1000").parse::<usize>()?;
    let read_concurrency = env("PROLLY_RUSTFS_PERF_READ_CONCURRENCY", "32").parse::<usize>()?;
    let write_concurrency = env("PROLLY_RUSTFS_PERF_WRITE_CONCURRENCY", "32").parse::<usize>()?;
    let existing_files = env("PROLLY_RUSTFS_PERF_EXISTING_FILES", "0").parse::<usize>()?;
    let branch_changes = env("PROLLY_RUSTFS_PERF_BRANCH_CHANGES", "100").parse::<usize>()?;
    let list_passes = env("PROLLY_RUSTFS_PERF_LIST_PASSES", "1").parse::<usize>()?;
    let read_passes = env("PROLLY_RUSTFS_PERF_READ_PASSES", "1").parse::<usize>()?;
    let node_cache_mib = env("PROLLY_RUSTFS_PERF_NODE_CACHE_MIB", "64").parse::<usize>()?;
    let rebuild_index = env("PROLLY_RUSTFS_PERF_REBUILD_INDEX", "false").parse::<bool>()?;
    let run_pack_stats = env("PROLLY_RUSTFS_PERF_PACK_STATS", "false").parse::<bool>()?;
    let run_gc = env("PROLLY_RUSTFS_PERF_GC", "false").parse::<bool>()?;
    let abandon_incomplete_gc =
        env("PROLLY_RUSTFS_PERF_GC_ABANDON_INCOMPLETE", "false").parse::<bool>()?;
    let gc_grace_millis = env("PROLLY_RUSTFS_PERF_GC_GRACE_MILLIS", "1").parse::<u64>()?;
    let run_foreground = env("PROLLY_RUSTFS_PERF_FOREGROUND", "true").parse::<bool>()?;
    let fsck_mode = env("PROLLY_RUSTFS_PERF_FSCK", "none");
    if !matches!(fsck_mode.as_str(), "none" | "shallow" | "deep") {
        return Err("fsck mode must be none, shallow, or deep".into());
    }
    #[cfg(feature = "foyer-cache")]
    let foyer_directory = std::env::var_os("PROLLY_RUSTFS_PERF_FOYER_DIR").map(PathBuf::from);
    #[cfg(not(feature = "foyer-cache"))]
    if std::env::var_os("PROLLY_RUSTFS_PERF_FOYER_DIR").is_some() {
        return Err("Foyer cache requested but the foyer-cache feature is disabled".into());
    }
    let stages = parse_stages()?;
    if read_sample_size == 0
        || read_concurrency == 0
        || write_concurrency == 0
        || branch_changes == 0
        || branch_changes > MUTATIONS_PER_COMMIT
        || list_passes == 0
        || read_passes == 0
        || node_cache_mib == 0
    {
        return Err("read sample size and read/write concurrency must be positive".into());
    }
    if existing_files > *stages.first().expect("non-empty stages") {
        return Err("existing file count cannot exceed the first benchmark stage".into());
    }

    let wire = S3WireAttemptInterceptor::new();
    let aws_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "rustfs-small-files-benchmark",
        ))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .interceptor(wire.clone())
        .build();
    let aws = aws_sdk_s3::Client::from_conf(aws_config);
    ensure_versioned_bucket(&aws, &bucket).await?;

    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let prefix = env(
        "PROLLY_RUSTFS_PERF_PREFIX",
        &format!("benchmarks/small-files/{run_id}"),
    );
    let writer = env(
        "PROLLY_RUSTFS_PERF_WRITER",
        &format!("small-files-benchmark-{run_id}"),
    );
    let branch_suffix = env("PROLLY_RUSTFS_PERF_BRANCH_SUFFIX", &run_id.to_string());
    #[cfg(feature = "foyer-cache")]
    let foyer_cache = match foyer_directory.as_ref() {
        Some(directory) => Some(
            FoyerNodeCache::open(FoyerNodeCacheConfig {
                directory: directory.clone(),
                memory_capacity_bytes: node_cache_mib * 1024 * 1024,
                disk_capacity_bytes: 2 * 1024 * 1024 * 1024,
                disk_block_size_bytes: 1024 * 1024,
                memory_shards: 64,
            })
            .await?,
        ),
        None => None,
    };
    #[allow(unused_mut)]
    let mut builder = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&prefix)
        .writer(&writer)
        // A sequential 10K-file session outlives the short interactive default.
        // Keep one authority permit valid for the full qualification run.
        .authority_lease_duration(Duration::from_secs(12 * 60 * 60))
        .provider_identity(ProviderIdentity::s3_compatible(&endpoint, "us-east-1"))
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "rustfs-small-files-benchmark",
            vec![0x62; 32],
        )?))
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .max_cached_node_bytes(node_cache_mib * 1024 * 1024);
    #[cfg(feature = "foyer-cache")]
    if let Some(cache) = &foyer_cache {
        builder = builder.node_cache(cache.clone());
    }
    let client = builder.initialize().await?;

    println!(
        "CONFIG endpoint={endpoint} bucket={bucket} prefix={prefix} writer={writer} stages={stages:?} existing_files={existing_files} branch_changes={branch_changes} list_passes={list_passes} read_passes={read_passes} node_cache_mib={node_cache_mib} foyer={} rebuild_index={rebuild_index} foreground={run_foreground} pack_stats={run_pack_stats} fsck={fsck_mode} gc={run_gc} gc_grace_millis={gc_grace_millis} object_bytes={} mutations_per_commit={MUTATIONS_PER_COMMIT} write_concurrency={write_concurrency} read_samples={read_sample_size} read_concurrency={read_concurrency}",
        if cfg!(feature = "foyer-cache")
            && std::env::var_os("PROLLY_RUSTFS_PERF_FOYER_DIR").is_some()
        {
            "enabled"
        } else {
            "disabled"
        },
        payload(0).len(),
    );

    if rebuild_index {
        let started = Instant::now();
        let mut cursor = client.start_branch_index_rebuild().await?;
        let mut indexed_publications = 0usize;
        let mut indexed_nodes = 0usize;
        loop {
            let step = client.advance_branch_index_rebuild(&cursor, 1_000).await?;
            indexed_publications += step.indexed_publications;
            indexed_nodes += step.indexed_nodes;
            cursor = step.cursor;
            if step.complete {
                break;
            }
        }
        println!(
            "INDEX_REBUILD wall_ms={:.3} indexed_publications={indexed_publications} indexed_nodes={indexed_nodes}",
            millis(started.elapsed()),
        );
    }

    let mut prior_target = existing_files;
    for target in stages {
        let added = target - prior_target;
        if added == 0 {
            println!("STAGE target={target} added=0 write=skipped_existing");
        } else {
            let cache_before = client.node_cache_snapshot();
            client.reset_s3_operation_metrics();
            wire.reset();
            let stage_started = Instant::now();
            let receipts = client
                .put_object_stream(
                    stream::iter((prior_target..target).map(|index| {
                        Ok(PutObjectInput {
                            key: key(index),
                            bytes: payload(index),
                            headers: ObjectHeaders::default(),
                            user_metadata: BTreeMap::new(),
                        })
                    })),
                    BulkWriteOptions {
                        batch_size: MUTATIONS_PER_COMMIT,
                        concurrency: write_concurrency,
                        checkpoint_every: 1_000,
                    },
                )
                .await?;

            let write_wall = stage_started.elapsed();
            let write_metrics = client.reset_s3_operation_metrics();
            let write_wire = wire.reset();
            println!(
                "STAGE target={target} added={added} write_wall_ms={:.3} files_per_second={:.2} commit_count={} mean_commit_ms={:.3}",
                millis(write_wall),
                added as f64 / write_wall.as_secs_f64(),
                receipts.len(),
                millis(write_wall) / receipts.len() as f64,
            );
            print_provider_metrics("write", write_metrics);
            print_wire_metrics("write", write_wire);
            print_cache_metrics("write", cache_before, client.node_cache_snapshot());
        }

        let cache_before = client.node_cache_snapshot();
        client.reset_s3_operation_metrics();
        wire.reset();
        let index_started = Instant::now();
        let index = client.advance_branch_indexes().await?;
        println!(
            "INDEX_CATCHUP target={target} wall_ms={:.3} journal_publications={} journal_commits={} journal_nodes={} operation_events={}",
            millis(index_started.elapsed()),
            index.journal.indexed_publications,
            index.journal.indexed_commits,
            index.journal.indexed_nodes,
            index.operations.indexed_events,
        );
        print_provider_metrics("index_catchup", client.reset_s3_operation_metrics());
        print_wire_metrics("index_catchup", wire.reset());
        print_cache_metrics("index_catchup", cache_before, client.node_cache_snapshot());
        if !run_foreground {
            prior_target = target;
            continue;
        }

        for pass in 1..=list_passes {
            let label = format!("list_pass_{pass}");
            let cache_before = client.node_cache_snapshot();
            client.reset_s3_operation_metrics();
            wire.reset();
            let list_started = Instant::now();
            let mut continuation = None;
            let mut listed = 0_usize;
            let mut list_page_latencies = Vec::new();
            loop {
                let page_started = Instant::now();
                let page = client
                    .list_objects_page("repo/files/", continuation.as_deref(), LIST_PAGE_SIZE)
                    .await?;
                list_page_latencies.push(page_started.elapsed());
                listed += page.objects.len();
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            let list_wall = list_started.elapsed();
            if listed != target {
                return Err(format!("expected {target} listed files, found {listed}").into());
            }
            let list_summary = summarize(list_page_latencies);
            println!(
                "LIST target={target} pass={pass} wall_ms={:.3} files_per_second={:.2} pages={} page_mean_ms={:.3} page_p50_ms={:.3} page_p95_ms={:.3} page_p99_ms={:.3} page_max_ms={:.3}",
                millis(list_wall),
                target as f64 / list_wall.as_secs_f64(),
                list_summary.count,
                list_summary.mean_ms,
                list_summary.p50_ms,
                list_summary.p95_ms,
                list_summary.p99_ms,
                list_summary.max_ms,
            );
            print_provider_metrics(&label, client.reset_s3_operation_metrics());
            print_wire_metrics(&label, wire.reset());
            print_cache_metrics(&label, cache_before, client.node_cache_snapshot());
        }

        let samples = read_sample_size.min(target);
        for pass in 1..=read_passes {
            let label = format!("read_pass_{pass}");
            let indexes = (0..samples)
                .map(|sample| sample * target / samples)
                .collect::<Vec<_>>();
            let cache_before = client.node_cache_snapshot();
            client.reset_s3_operation_metrics();
            wire.reset();
            let read_started = Instant::now();
            let results = stream::iter(indexes)
                .map(|index| {
                    let client = client.clone();
                    async move {
                        let started = Instant::now();
                        let object = client.get_object(key(index)).await?;
                        if object.as_ref().map(|value| value.bytes.as_slice())
                            != Some(payload(index).as_slice())
                        {
                            return Err(
                                format!("read verification failed for index {index}").into()
                            );
                        }
                        Ok::<Duration, Box<dyn std::error::Error + Send + Sync>>(started.elapsed())
                    }
                })
                .buffer_unordered(read_concurrency)
                .collect::<Vec<_>>()
                .await;
            let read_wall = read_started.elapsed();
            let read_latencies = results.into_iter().collect::<BenchResult<Vec<_>>>()?;
            let read_summary = summarize(read_latencies);
            println!(
                "READ target={target} pass={pass} samples={samples} concurrency={read_concurrency} wall_ms={:.3} reads_per_second={:.2} mean_ms={:.3} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3}",
                millis(read_wall),
                samples as f64 / read_wall.as_secs_f64(),
                read_summary.mean_ms,
                read_summary.p50_ms,
                read_summary.p95_ms,
                read_summary.p99_ms,
                read_summary.max_ms,
            );
            print_provider_metrics(&label, client.reset_s3_operation_metrics());
            print_wire_metrics(&label, wire.reset());
            print_cache_metrics(&label, cache_before, client.node_cache_snapshot());
        }

        let base = client.head().await?;
        let branch = format!("scale-{target}-{branch_suffix}");
        let cache_before = client.node_cache_snapshot();
        client.reset_s3_operation_metrics();
        wire.reset();
        let branch_started = Instant::now();
        client.create_branch(&branch, Some(base)).await?;
        let branch_elapsed = branch_started.elapsed();
        println!(
            "BRANCH target={target} wall_ms={:.3}",
            millis(branch_elapsed)
        );
        print_provider_metrics("branch", client.reset_s3_operation_metrics());
        print_wire_metrics("branch", wire.reset());
        print_cache_metrics("branch", cache_before, client.node_cache_snapshot());

        let feature = client.checkout(CheckoutRef::Branch(branch.clone())).await?;
        let sparse_changes = target.min(branch_changes);
        let cache_before = client.node_cache_snapshot();
        client.reset_s3_operation_metrics();
        wire.reset();
        let feature_write_started = Instant::now();
        feature
            .put_object_stream(
                stream::iter((0..sparse_changes).map(|index| {
                    Ok(PutObjectInput {
                        key: format!("repo/branch-probes/{target}/{index:03}.txt"),
                        bytes: format!("branch-{target}-{index}\n").into_bytes(),
                        headers: ObjectHeaders::default(),
                        user_metadata: BTreeMap::new(),
                    })
                })),
                BulkWriteOptions {
                    batch_size: sparse_changes,
                    concurrency: write_concurrency,
                    checkpoint_every: sparse_changes,
                },
            )
            .await?;
        let feature_head = feature.head().await?;
        println!(
            "BRANCH_WRITE target={target} changes={sparse_changes} wall_ms={:.3} changes_per_second={:.2}",
            millis(feature_write_started.elapsed()),
            sparse_changes as f64 / feature_write_started.elapsed().as_secs_f64(),
        );
        print_provider_metrics("branch_write", client.reset_s3_operation_metrics());
        print_wire_metrics("branch_write", wire.reset());
        print_cache_metrics("branch_write", cache_before, client.node_cache_snapshot());

        let cache_before = client.node_cache_snapshot();
        client.reset_s3_operation_metrics();
        wire.reset();
        let diff_started = Instant::now();
        let mut diff_cursor = None;
        let mut changed = 0usize;
        loop {
            let page = client
                .diff_bounded(base, feature_head, diff_cursor.as_ref(), 1_000)
                .await?;
            changed += page.changes.len();
            diff_cursor = page.continuation;
            if diff_cursor.is_none() {
                break;
            }
        }
        let diff_elapsed = diff_started.elapsed();
        println!(
            "DIFF target={target} changes={changed} wall_ms={:.3} changes_per_second={:.2}",
            millis(diff_elapsed),
            changed as f64 / diff_elapsed.as_secs_f64(),
        );
        print_provider_metrics("diff", client.reset_s3_operation_metrics());
        print_wire_metrics("diff", wire.reset());
        print_cache_metrics("diff", cache_before, client.node_cache_snapshot());

        let cache_before = client.node_cache_snapshot();
        client.reset_s3_operation_metrics();
        wire.reset();
        let merge_started = Instant::now();
        let mut merge = client
            .start_merge(
                &branch,
                Some(base),
                MergePolicy::Theirs,
                format!("merge scale branch at {target}"),
            )
            .await?;
        let mut merge_processed = 0usize;
        while merge.phase != MergePhase::ReadyToPublish {
            let page = client.advance_merge(&merge, 1_000).await?;
            merge_processed += page.processed;
            merge = page.cursor;
        }
        let merged = client.publish_merge(&merge).await?;
        let merge_elapsed = merge_started.elapsed();
        println!(
            "MERGE target={target} changes={} processed={} wall_ms={:.3}",
            merged.changed_keys,
            merge_processed,
            millis(merge_elapsed),
        );
        print_provider_metrics("merge", client.reset_s3_operation_metrics());
        print_wire_metrics("merge", wire.reset());
        print_cache_metrics("merge", cache_before, client.node_cache_snapshot());
        client.delete_branch(&branch, feature_head).await?;
        println!("STAGE_COMPLETE target={target}");
        prior_target = target;
    }
    if run_pack_stats {
        let cache_before = client.node_cache_snapshot();
        client.reset_s3_operation_metrics();
        wire.reset();
        let started = Instant::now();
        let mut cursor = client.start_payload_pack_stats().await?;
        let mut pages = 0usize;
        loop {
            let page = client.advance_payload_pack_stats(&cursor, 1_000).await?;
            pages += 1;
            cursor = page.cursor;
            if pages % 100 == 0 {
                println!(
                    "PACK_STATS_PROGRESS pages={pages} current_objects={}",
                    cursor.report.current_objects,
                );
            }
            if page.complete {
                break;
            }
        }
        println!(
            "PACK_STATS wall_ms={:.3} pages={pages} current_objects={} logical_bytes={} direct_objects={} packed_objects={} packed_logical_bytes={} unique_physical_objects={} unique_physical_bytes={} unique_pack_objects={} unique_pack_bytes={} unique_packed_extents={} unique_packed_extent_bytes={} utilization_basis_points={}",
            millis(started.elapsed()),
            cursor.report.current_objects,
            cursor.report.logical_bytes,
            cursor.report.direct_objects,
            cursor.report.packed_objects,
            cursor.report.packed_logical_bytes,
            cursor.report.unique_physical_objects,
            cursor.report.unique_physical_bytes,
            cursor.report.unique_pack_objects,
            cursor.report.unique_pack_bytes,
            cursor.report.unique_packed_extents,
            cursor.report.unique_packed_extent_bytes,
            cursor.report.pack_utilization_basis_points(),
        );
        print_provider_metrics("pack_stats", client.reset_s3_operation_metrics());
        print_wire_metrics("pack_stats", wire.reset());
        print_cache_metrics("pack_stats", cache_before, client.node_cache_snapshot());
    }
    if fsck_mode != "none" {
        let cache_before = client.node_cache_snapshot();
        client.reset_s3_operation_metrics();
        wire.reset();
        let started = Instant::now();
        let mut cursor = client.start_fsck(fsck_mode == "deep").await?;
        let mut pages = 0usize;
        loop {
            let page = client.advance_fsck(&cursor, 10_000).await?;
            pages += 1;
            cursor = page.cursor;
            if pages % 10 == 0 {
                println!(
                    "FSCK_PROGRESS mode={fsck_mode} pages={pages} phase={:?} commits={} current_objects={} logical_versions={}",
                    cursor.phase,
                    cursor.report.commits,
                    cursor.report.current_objects,
                    cursor.report.logical_versions,
                );
            }
            if page.complete {
                break;
            }
        }
        println!(
            "FSCK mode={fsck_mode} wall_ms={:.3} pages={pages} commits={} reachable_nodes={} current_objects={} logical_versions={} payloads_verified={} payload_bytes_verified={} deep_content_bytes_verified={} physical_payloads_verified={} physical_payload_bytes_verified={} deep_physical_bytes_read={} packed_payloads_verified={} packed_logical_bytes_verified={}",
            millis(started.elapsed()),
            cursor.report.commits,
            cursor.report.reachable_nodes,
            cursor.report.current_objects,
            cursor.report.logical_versions,
            cursor.report.payloads_verified,
            cursor.report.payload_bytes_verified,
            cursor.report.deep_content_bytes_verified,
            cursor.report.physical_payloads_verified,
            cursor.report.physical_payload_bytes_verified,
            cursor.report.deep_physical_bytes_read,
            cursor.report.packed_payloads_verified,
            cursor.report.packed_logical_bytes_verified,
        );
        print_provider_metrics("fsck", client.reset_s3_operation_metrics());
        print_wire_metrics("fsck", wire.reset());
        print_cache_metrics("fsck", cache_before, client.node_cache_snapshot());
    }
    if run_gc {
        let cache_before = client.node_cache_snapshot();
        client.reset_s3_operation_metrics();
        wire.reset();
        let started = Instant::now();
        if abandon_incomplete_gc {
            match client.abandon_incomplete_gc().await {
                Ok(epoch) => println!("GC_ABANDONED_INCOMPLETE epoch={epoch}"),
                Err(error)
                    if error.code == prolly_s3_client::core::ErrorCode::PreconditionFailed => {}
                Err(error) => return Err(error.into()),
            }
        }
        let mut cursor = match client.resume_gc().await? {
            Some(cursor) => cursor,
            None => client.start_gc(gc_grace_millis).await?,
        };
        let mut pages = 0usize;
        loop {
            let page = match cursor.phase {
                GcPhase::Ready | GcPhase::Sweeping => client.sweep_gc(&cursor, 1_000).await?,
                GcPhase::Complete => break,
                GcPhase::MarkNodes => client.advance_gc(&cursor, 100).await?,
                GcPhase::ScanCandidates => client.advance_gc(&cursor, 100).await?,
                _ => client.advance_gc(&cursor, 100).await?,
            };
            pages += 1;
            cursor = page.cursor;
            if pages % 100 == 0 {
                println!(
                    "GC_PROGRESS pages={pages} phase={:?} commits={} nodes={} logical_versions={} candidates={} deleted_versions={}",
                    cursor.phase,
                    cursor.report.commits,
                    cursor.report.nodes,
                    cursor.report.logical_versions,
                    cursor.report.candidates,
                    cursor.report.deleted_versions,
                );
            }
        }
        println!(
            "GC wall_ms={:.3} pages={pages} roots={} commits={} nodes={} logical_versions={} candidates={} candidate_bytes={} dirty_roots={} deleted_versions={} deleted_bytes={} already_missing={} skipped_reachable={}",
            millis(started.elapsed()),
            cursor.report.roots,
            cursor.report.commits,
            cursor.report.nodes,
            cursor.report.logical_versions,
            cursor.report.candidates,
            cursor.report.candidate_bytes,
            cursor.report.dirty_roots,
            cursor.report.deleted_versions,
            cursor.report.deleted_bytes,
            cursor.report.already_missing,
            cursor.report.skipped_reachable,
        );
        print_provider_metrics("gc", client.reset_s3_operation_metrics());
        print_wire_metrics("gc", wire.reset());
        print_cache_metrics("gc", cache_before, client.node_cache_snapshot());
    }
    drop(client);
    #[cfg(feature = "foyer-cache")]
    if let Some(cache) = foyer_cache {
        cache.close().await?;
    }
    Ok(())
}
