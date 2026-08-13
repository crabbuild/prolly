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
    core::ProviderPerKeyVersionLimit, Client, HmacAttestationSigner, ProviderIdentity,
    S3OperationMetrics, S3WireAttemptInterceptor, S3WireAttemptMetrics,
};

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
    let access_key = env("PROLLY_RUSTFS_ACCESS_KEY", "prolly");
    let secret_key = env("PROLLY_RUSTFS_SECRET_KEY", "prolly");
    let bucket = env("PROLLY_RUSTFS_BUCKET", "prolly");
    let read_sample_size = env("PROLLY_RUSTFS_PERF_READ_SAMPLES", "1000").parse::<usize>()?;
    let read_concurrency = env("PROLLY_RUSTFS_PERF_READ_CONCURRENCY", "32").parse::<usize>()?;
    let stages = parse_stages()?;
    if read_sample_size == 0 || read_concurrency == 0 {
        return Err("read sample size and concurrency must be positive".into());
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
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&prefix)
        .writer(format!("small-files-benchmark-{run_id}"))
        // A sequential 10K-file session outlives the short interactive default.
        // Keep one authority permit valid for the full qualification run.
        .authority_lease_duration(Duration::from_secs(12 * 60 * 60))
        .provider_identity(ProviderIdentity::s3_compatible(&endpoint, "us-east-1"))
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "rustfs-small-files-benchmark",
            vec![0x62; 32],
        )?))
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await?;

    println!(
        "CONFIG endpoint={endpoint} bucket={bucket} prefix={prefix} stages={stages:?} object_bytes={} mutations_per_commit={MUTATIONS_PER_COMMIT} read_samples={read_sample_size} read_concurrency={read_concurrency}",
        payload(0).len(),
    );

    let mut prior_target = 0_usize;
    for target in stages {
        let added = target - prior_target;
        client.reset_s3_operation_metrics();
        wire.reset();
        let stage_started = Instant::now();
        let mut write_latencies = Vec::with_capacity(added);
        let mut publish_latencies = Vec::with_capacity(added.div_ceil(MUTATIONS_PER_COMMIT));

        for batch_start in (prior_target..target).step_by(MUTATIONS_PER_COMMIT) {
            let batch_end = (batch_start + MUTATIONS_PER_COMMIT).min(target);
            let mut session = client
                .begin_commit()
                .message(format!("small-file benchmark through {batch_end}"))
                .checkpoint_every(1_000)
                .start()
                .await?;
            for index in batch_start..batch_end {
                let started = Instant::now();
                session.put_object(key(index), payload(index)).await?;
                write_latencies.push(started.elapsed());
            }
            let publish_started = Instant::now();
            session.publish().await?;
            publish_latencies.push(publish_started.elapsed());
            println!(
                "PROGRESS target={target} committed={batch_end} added={}",
                batch_end - prior_target
            );
        }

        let write_wall = stage_started.elapsed();
        let write_summary = summarize(write_latencies);
        let publish_summary = summarize(publish_latencies);
        let write_metrics = client.reset_s3_operation_metrics();
        let write_wire = wire.reset();
        println!(
            "STAGE target={target} added={added} write_wall_ms={:.3} files_per_second={:.2} write_count={} write_mean_ms={:.3} write_p50_ms={:.3} write_p95_ms={:.3} write_p99_ms={:.3} write_max_ms={:.3} publish_count={} publish_mean_ms={:.3} publish_p50_ms={:.3} publish_p95_ms={:.3} publish_p99_ms={:.3} publish_max_ms={:.3}",
            millis(write_wall),
            added as f64 / write_wall.as_secs_f64(),
            write_summary.count,
            write_summary.mean_ms,
            write_summary.p50_ms,
            write_summary.p95_ms,
            write_summary.p99_ms,
            write_summary.max_ms,
            publish_summary.count,
            publish_summary.mean_ms,
            publish_summary.p50_ms,
            publish_summary.p95_ms,
            publish_summary.p99_ms,
            publish_summary.max_ms,
        );
        print_provider_metrics("write", write_metrics);
        print_wire_metrics("write", write_wire);

        client.reset_s3_operation_metrics();
        wire.reset();
        let list_started = Instant::now();
        let mut after = None;
        let mut listed = 0_usize;
        let mut list_page_latencies = Vec::new();
        loop {
            let page_started = Instant::now();
            let (_, page, truncated) = client
                .list_objects("repo/files/", after.as_deref(), LIST_PAGE_SIZE)
                .await?;
            list_page_latencies.push(page_started.elapsed());
            listed += page.len();
            after = page
                .last()
                .map(|item| String::from_utf8_lossy(&item.key).into_owned());
            if !truncated {
                break;
            }
        }
        let list_wall = list_started.elapsed();
        if listed != target {
            return Err(format!("expected {target} listed files, found {listed}").into());
        }
        let list_summary = summarize(list_page_latencies);
        println!(
            "LIST target={target} wall_ms={:.3} files_per_second={:.2} pages={} page_mean_ms={:.3} page_p50_ms={:.3} page_p95_ms={:.3} page_p99_ms={:.3} page_max_ms={:.3}",
            millis(list_wall),
            target as f64 / list_wall.as_secs_f64(),
            list_summary.count,
            list_summary.mean_ms,
            list_summary.p50_ms,
            list_summary.p95_ms,
            list_summary.p99_ms,
            list_summary.max_ms,
        );
        print_provider_metrics("list", client.reset_s3_operation_metrics());
        print_wire_metrics("list", wire.reset());

        let samples = read_sample_size.min(target);
        let indexes = (0..samples)
            .map(|sample| sample * target / samples)
            .collect::<Vec<_>>();
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
                        return Err(format!("read verification failed for index {index}").into());
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
            "READ target={target} samples={samples} concurrency={read_concurrency} wall_ms={:.3} reads_per_second={:.2} mean_ms={:.3} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3}",
            millis(read_wall),
            samples as f64 / read_wall.as_secs_f64(),
            read_summary.mean_ms,
            read_summary.p50_ms,
            read_summary.p95_ms,
            read_summary.p99_ms,
            read_summary.max_ms,
        );
        print_provider_metrics("read", client.reset_s3_operation_metrics());
        print_wire_metrics("read", wire.reset());
        println!("STAGE_COMPLETE target={target}");
        prior_target = target;
    }
    Ok(())
}
