use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, types::BucketVersioningStatus};
use futures_util::{stream, StreamExt};
use prolly_s3_client::{
    core::{ObjectHeaders, Repository, RepositoryOptions},
    AwsS3ObjectPlane,
};

fn enabled() -> bool {
    std::env::var("PROLLY_S3_AWS_PERF").as_deref() == Ok("1")
}

fn required<T: std::str::FromStr>(name: &str) -> T
where
    T::Err: std::fmt::Display,
{
    let value = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    value
        .parse()
        .unwrap_or_else(|error| panic!("invalid {name}={value:?}: {error}"))
}

fn percentile(sorted: &[std::time::Duration], percentile: usize) -> std::time::Duration {
    sorted[(sorted.len() * percentile).div_ceil(100) - 1]
}

/// Explicit, fail-closed AWS release gate for a hot branch. This test has no
/// built-in latency or throughput promises: operators must supply thresholds
/// derived from their target region, traffic model, and SLO.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "requires an operator-owned versioned AWS bucket and explicit SLOs"]
async fn aws_hot_branch_performance_release_gate() {
    assert!(
        enabled(),
        "set PROLLY_S3_AWS_PERF=1 plus the documented AWS performance variables to run"
    );

    let region_name = std::env::var("PROLLY_AWS_REGION").expect("PROLLY_AWS_REGION is required");
    let bucket = std::env::var("PROLLY_AWS_BUCKET_VERSIONED")
        .expect("PROLLY_AWS_BUCKET_VERSIONED is required");
    let writes_per_tier: usize = required("PROLLY_AWS_PERF_WRITES_PER_TIER");
    let max_p99_millis: u128 = required("PROLLY_AWS_PERF_MAX_P99_MS");
    let min_writes_per_second: f64 = required("PROLLY_AWS_PERF_MIN_WRITES_PER_SECOND");
    assert!(
        writes_per_tier >= 32,
        "performance tiers require at least 32 writes"
    );
    assert!(max_p99_millis > 0);
    assert!(min_writes_per_second > 0.0);

    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region_name.clone()))
        .load()
        .await;
    let aws = aws_sdk_s3::Client::new(&shared);
    let versioning = aws
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await
        .expect("read AWS bucket versioning");
    assert_eq!(
        versioning.status,
        Some(BucketVersioningStatus::Enabled),
        "AWS performance bucket must have versioning enabled"
    );

    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: format!("prolly-s3-performance/{run_id}"),
            writer: format!("aws-performance-{run_id}"),
            writer_lease_millis: 10 * 60 * 1_000,
            max_parallel_payload_writes: 64,
            ..RepositoryOptions::default()
        },
    )
    .await
    .expect("initialize AWS performance repository");
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            vec![0; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .expect("warm AWS request paths");

    for concurrency in [1_usize, 8, 32] {
        plane.reset_metrics();
        let started = Instant::now();
        let results = stream::iter(0..writes_per_tier)
            .map(|index| {
                let repository = &repository;
                async move {
                    let operation_started = Instant::now();
                    repository
                        .put_bytes(
                            "main",
                            format!("tier-{concurrency}/{index}.bin").into_bytes(),
                            vec![index as u8; 64 * 1024],
                            ObjectHeaders::default(),
                            BTreeMap::new(),
                            None,
                        )
                        .await
                        .map(|_| operation_started.elapsed())
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let wall = started.elapsed();
        let mut latencies = results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("AWS performance write failed");
        latencies.sort_unstable();
        let p50 = percentile(&latencies, 50);
        let p95 = percentile(&latencies, 95);
        let p99 = percentile(&latencies, 99);
        let writes_per_second = writes_per_tier as f64 / wall.as_secs_f64();
        let calls = plane.reset_metrics();

        assert_eq!(
            calls.total_calls(),
            (writes_per_tier * 4) as u64,
            "tier {concurrency} exceeded the four-call logical-write budget: {calls:?}"
        );
        assert_eq!(
            calls.put_object,
            (writes_per_tier * 3) as u64,
            "tier {concurrency} used an unexpected AWS operation mix: {calls:?}"
        );
        assert_eq!(calls.get_object, writes_per_tier as u64);
        assert!(
            p99.as_millis() <= max_p99_millis,
            "tier {concurrency} p99 {}ms exceeds {}ms",
            p99.as_millis(),
            max_p99_millis
        );
        assert!(
            writes_per_second >= min_writes_per_second,
            "tier {concurrency} throughput {writes_per_second:.2}/s is below {min_writes_per_second:.2}/s"
        );
        eprintln!(
            "aws_hot_branch run={run_id} region={region_name} tier={concurrency} writes={writes_per_tier} object_bytes=65536 s3_calls={} calls_per_write=4 wall_ms={} p50_ms={} p95_ms={} p99_ms={} writes_per_second={writes_per_second:.2}",
            calls.total_calls(),
            wall.as_millis(),
            p50.as_millis(),
            p95.as_millis(),
            p99.as_millis(),
        );
    }

    let performance = repository.performance_snapshot();
    assert_eq!(performance.publication_queue_depth, 0);
    eprintln!(
        "aws_publication run={run_id} acquisitions={} wait_ms={} max_queue_depth={}",
        performance.publication_acquisitions,
        performance.publication_wait_nanos / 1_000_000,
        performance.publication_max_queue_depth,
    );
}
