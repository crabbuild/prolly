//! Shared RustFS bootstrap used by the runnable examples.
//!
//! Every example is a standalone Cargo example. The only external dependency
//! is the local RustFS service documented in `extensions/s3/README.md`.

use std::{sync::Arc, time::Duration};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_types::region::Region;
use prolly_s3_client::{
    core::ProviderPerKeyVersionLimit, Client, HmacAttestationSigner, ProviderIdentity,
};

pub type ExampleResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// A ready client plus the physical names selected for this example run.
#[allow(dead_code)]
pub struct ExampleRepository {
    pub client: Client,
    pub bucket: String,
    pub prefix: String,
}

/// Create a versioned RustFS bucket and initialize an isolated repository.
///
/// Set `PROLLY_S3_EXAMPLE_PREFIX` to reuse a stable base prefix. Without it,
/// each run uses a unique prefix so examples can run in parallel.
pub async fn initialize(scenario: &str) -> ExampleResult<ExampleRepository> {
    let endpoint = environment("PROLLY_RUSTFS_ENDPOINT", "http://127.0.0.1:9000");
    let access_key = environment("PROLLY_RUSTFS_ACCESS_KEY", "prollyadmin");
    let secret_key = environment("PROLLY_RUSTFS_SECRET_KEY", "prolly-local-secret-change-me");
    let bucket = environment("PROLLY_RUSTFS_BUCKET", "prolly-versioned-s3-examples");
    let prefix = repository_prefix(scenario);

    let aws_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "prolly-s3-rustfs-example",
        ))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .build();
    let aws = aws_sdk_s3::Client::from_conf(aws_config);
    ensure_versioned_bucket(&aws, &bucket).await?;

    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&prefix)
        .writer(format!("prolly-s3-{scenario}-example"))
        .provider_identity(ProviderIdentity::s3_compatible(&endpoint, "us-east-1"))
        .provider_attestation_validity(Duration::from_secs(24 * 60 * 60))
        // This deterministic key is suitable only for a local example. Use a
        // protected, rotated signing key in an enterprise deployment.
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "local-example-provider-key",
            vec![0x41; 32],
        )?))
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await?;

    Ok(ExampleRepository {
        client,
        bucket,
        prefix,
    })
}

fn repository_prefix(scenario: &str) -> String {
    let scenario = scenario.replace('_', "-");
    if let Ok(base) = std::env::var("PROLLY_S3_EXAMPLE_PREFIX") {
        return format!("{}/{scenario}", base.trim_end_matches('/'));
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    format!(".prolly-examples/{scenario}/{}-{nanos}", std::process::id())
}

fn environment(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

async fn ensure_versioned_bucket(aws: &aws_sdk_s3::Client, bucket: &str) -> ExampleResult {
    match aws.create_bucket().bucket(bucket).send().await {
        Ok(_) => {}
        Err(error) => {
            let description = format!("{error:?}");
            if !description.contains("BucketAlreadyOwnedByYou")
                && !description.contains("BucketAlreadyExists")
            {
                return Err(error.into());
            }
        }
    }

    // Versioning is a hard repository requirement, not an optional feature.
    aws.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    Ok(())
}
