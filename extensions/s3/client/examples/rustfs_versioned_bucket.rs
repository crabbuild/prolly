use std::{sync::Arc, time::Duration};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_types::region::Region;
use prolly_s3_client::{
    core::{MergePhase, MergePolicy, ProviderPerKeyVersionLimit},
    Client, HmacAttestationSigner, ProviderIdentity,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = environment("PROLLY_RUSTFS_ENDPOINT", "http://127.0.0.1:9000");
    let access_key = environment("PROLLY_RUSTFS_ACCESS_KEY", "prollyadmin");
    let secret_key = environment("PROLLY_RUSTFS_SECRET_KEY", "prolly-local-secret-change-me");
    let bucket = environment("PROLLY_RUSTFS_BUCKET", "prolly-versioned-s3-demo");
    let repository_prefix = environment("PROLLY_S3_DEMO_PREFIX", ".prolly-demo");

    let aws_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "rustfs-versioned-bucket-example",
        ))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .build();
    let aws = aws_sdk_s3::Client::from_conf(aws_config);
    ensure_bucket(&aws, &bucket).await?;

    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-versioned-bucket-example")
        .provider_identity(ProviderIdentity::s3_compatible(&endpoint, "us-east-1"))
        .provider_attestation_validity(Duration::from_secs(24 * 60 * 60))
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "demo-provider-key",
            vec![0x41; 32],
        )?))
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await?;

    let first = client
        .put_object("demo/greeting.txt", b"hello from main\n".to_vec())
        .await?;
    client.create_branch("feature", Some(first.id)).await?;

    let feature = client.checkout("feature").await?;
    feature
        .put_object("demo/feature.txt", b"created on feature\n".to_vec())
        .await?;
    feature
        .put_object("demo/greeting.txt", b"hello from feature\n".to_vec())
        .await?;
    client
        .put_object("demo/main.txt", b"created on main\n".to_vec())
        .await?;

    let mut merge = client
        .start_merge(
            "feature",
            None,
            MergePolicy::Theirs,
            "merge feature into main",
        )
        .await?;
    while merge.phase != MergePhase::ReadyToPublish {
        merge = client.advance_merge(&merge, 100).await?.cursor;
    }

    let changes = client.merge_changes_page(&merge, None, 100).await?.changes;
    let receipt = client.publish_merge(&merge).await?;
    let historical = client
        .get_object_at(first.id, "demo/greeting.txt")
        .await?
        .ok_or("historical object is missing")?;
    let (_, current, truncated) = client.list_objects("demo/", None, 100).await?;

    println!("bucket={bucket}");
    println!("repository_prefix={repository_prefix}");
    println!("first_commit={}", first.id);
    println!("feature_changes={}", changes.len());
    println!("merge_commit={}", receipt.id);
    println!("merge_changed_keys={}", receipt.changed_keys);
    println!(
        "historical_greeting={}",
        String::from_utf8_lossy(&historical.bytes).trim()
    );
    println!("current_objects={}", current.len());
    println!("listing_truncated={truncated}");
    Ok(())
}

fn environment(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

async fn ensure_bucket(
    aws: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<(), Box<dyn std::error::Error>> {
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
