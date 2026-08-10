use std::{sync::Arc, time::Duration};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use prolly_s3_client::{Client, HmacAttestationSigner, HmacTokenSigner, ProviderIdentity};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = environment("PROLLY_RUSTFS_ENDPOINT", "http://127.0.0.1:9000");
    let access_key = environment("PROLLY_RUSTFS_ACCESS_KEY", "prollyadmin");
    let secret_key = environment("PROLLY_RUSTFS_SECRET_KEY", "prolly-local-secret-change-me");
    let bucket = environment("PROLLY_RUSTFS_BUCKET", "prolly-versioned-s3-demo");
    let repository_prefix = environment("PROLLY_S3_DEMO_PREFIX", ".prolly-demo/v1");

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

    // These fixed keys are for the loopback demonstration only. Production
    // deployments load independent provider-attestation and cursor key rings.
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .default_branch("main")
        .writer("rustfs-versioned-bucket-example")
        .provider_identity(ProviderIdentity::s3_compatible(
            endpoint.as_str(),
            "us-east-1",
        ))
        .provider_attestation_validity(Duration::from_secs(24 * 60 * 60))
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "demo-provider-key",
            vec![0x41; 32],
        )?))
        .token_signer(Arc::new(HmacTokenSigner::single(
            "demo-cursor-key",
            vec![0x42; 32],
        )?))
        .initialize()
        .await?;

    let before = client.head_commit().await?;
    let first = client
        .put_object()
        .bucket(&bucket)
        .key("demo/greeting.txt")
        .body(ByteStream::from_static(b"hello from version one\n"))
        .content_type("text/plain")
        .logical_retry_limit(3)
        .send()
        .await?;
    let second = client
        .put_object()
        .bucket(&bucket)
        .key("demo/greeting.txt")
        .body(ByteStream::from_static(b"hello from version two\n"))
        .content_type("text/plain")
        .logical_retry_limit(3)
        .send()
        .await?;

    // Reconstruct the adapter as a separate process would. Ordinary open
    // verifies the persisted format and provider attestation without probes.
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .default_branch("main")
        .writer("rustfs-versioned-bucket-example-reopened")
        .provider_identity(ProviderIdentity::s3_compatible(
            endpoint.as_str(),
            "us-east-1",
        ))
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "demo-provider-key",
            vec![0x41; 32],
        )?))
        .token_signer(Arc::new(HmacTokenSigner::single(
            "demo-cursor-key",
            vec![0x42; 32],
        )?))
        .open()
        .await?;
    if client.head_commit().await? != second.snapshot {
        return Err("reopened repository did not retain the published head".into());
    }

    let historical = client
        .at(first.snapshot)
        .await?
        .get_object()
        .bucket(&bucket)
        .key("demo/greeting.txt")
        .send()
        .await?
        .output
        .body
        .collect()
        .await?
        .into_bytes();
    if historical.as_ref() != b"hello from version one\n" {
        return Err("historical snapshot returned unexpected content".into());
    }

    let listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("demo/")
        .send()
        .await?;
    let (changes, truncated) = client.diff_page(before, second.snapshot, None, 100).await?;
    if truncated || changes.len() != 1 {
        return Err("expected one complete logical-key diff".into());
    }
    let fsck = client.fsck().await?;

    println!("bucket={bucket}");
    println!("repository_prefix={repository_prefix}");
    println!("first_commit={}", first.snapshot);
    println!("second_commit={}", second.snapshot);
    println!("listed_objects={}", listing.output.contents().len());
    println!("diff_entries={}", changes.len());
    println!("fsck_commits={}", fsck.commits);
    println!("fsck_logical_versions={}", fsck.logical_versions);
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
        Ok(_) => Ok(()),
        Err(error) => {
            let description = format!("{error:?}");
            if description.contains("BucketAlreadyOwnedByYou")
                || description.contains("BucketAlreadyExists")
            {
                Ok(())
            } else {
                Err(error.into())
            }
        }
    }
}
