use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, types::BucketVersioningStatus};
use futures_util::{stream, StreamExt};
use prolly_s3_client::{
    core::ProviderPerKeyVersionLimit, Client, HmacAttestationSigner, ProviderIdentity,
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
    let max_calls_per_write: u64 = required("PROLLY_AWS_PERF_MAX_CALLS_PER_WRITE");

    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region_name.clone()))
        .load()
        .await;
    let aws = aws_sdk_s3::Client::new(&shared);
    assert_eq!(
        aws.get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .expect("read AWS bucket versioning")
            .status,
        Some(BucketVersioningStatus::Enabled)
    );

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(format!("prolly-s3-performance/{run_id}"))
        .writer(format!("aws-performance-{run_id}"))
        .provider_identity(ProviderIdentity::aws_region(region_name.clone()))
        .attestation_signer(Arc::new(
            HmacAttestationSigner::single("aws-performance", vec![0x61; 32]).unwrap(),
        ))
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .expect("initialize AWS performance repository");
    client
        .put_object("warmup.bin", vec![0; 64 * 1024])
        .await
        .expect("warm AWS request paths");

    for concurrency in [1_usize, 8, 32] {
        client.reset_s3_operation_metrics();
        let started = Instant::now();
        let results = stream::iter(0..writes_per_tier)
            .map(|index| {
                let client = client.clone();
                async move {
                    let operation_started = Instant::now();
                    client
                        .put_object(
                            format!("tier-{concurrency}/{index}.bin"),
                            vec![index as u8; 64 * 1024],
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
        let calls = client.reset_s3_operation_metrics();
        let calls_per_write = calls.total_calls().div_ceil(writes_per_tier as u64);

        assert!(
            calls_per_write <= max_calls_per_write,
            "tier {concurrency} used {calls_per_write} calls/write; limit is {max_calls_per_write}: {calls:?}"
        );
        assert!(p99.as_millis() <= max_p99_millis);
        assert!(writes_per_second >= min_writes_per_second);
        eprintln!(
            "aws_hot_branch run={run_id} region={region_name} tier={concurrency} writes={writes_per_tier} object_bytes=65536 s3_calls={} calls_per_write={} wall_ms={} p50_ms={} p95_ms={} p99_ms={} writes_per_second={writes_per_second:.2}",
            calls.total_calls(),
            calls_per_write,
            wall.as_millis(),
            p50.as_millis(),
            p95.as_millis(),
            p99.as_millis(),
        );
    }
}
