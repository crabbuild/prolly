use std::{env, error::Error, sync::Arc};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::Region,
    primitives::ByteStream,
    types::{BucketVersioningStatus, VersioningConfiguration},
};
use prolly_s3_client::{
    core::{
        decode_canonical, encode_canonical, CompareExchange, CompareExchangeOutcome, ErrorCode,
        ListRequest, ObjectPath, ObjectPlane, RepositoryFormatV1,
    },
    AwsS3ObjectPlane, Client, HmacAttestationSigner, ProviderIdentity,
};
use sha2::{Digest, Sha256};

struct Environment {
    endpoint: String,
    bucket: String,
    prefix: String,
    aws: aws_sdk_s3::Client,
}

impl Environment {
    async fn load() -> Result<Self, Box<dyn Error>> {
        let endpoint = env::var("PROLLY_RUSTFS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        let access_key =
            env::var("PROLLY_RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "prollyadmin".to_string());
        let secret_key = env::var("PROLLY_RUSTFS_SECRET_KEY")
            .unwrap_or_else(|_| "prolly-local-secret-change-me".to_string());
        let bucket = env::var("PROLLY_S3_ROLLING_BUCKET")?;
        let prefix = env::var("PROLLY_S3_ROLLING_PREFIX")?;
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                access_key,
                secret_key,
                None,
                None,
                "prolly-s3-rolling-fixture",
            ))
            .endpoint_url(&endpoint)
            .force_path_style(true)
            .build();
        Ok(Self {
            endpoint,
            bucket,
            prefix,
            aws: aws_sdk_s3::Client::from_conf(config),
        })
    }

    fn builder(&self, writer: &str) -> prolly_s3_client::ClientBuilder {
        Client::builder()
            .aws_client(self.aws.clone())
            .bucket(&self.bucket)
            .repository_prefix(&self.prefix)
            .writer(writer)
            .provider_identity(ProviderIdentity::s3_compatible(
                self.endpoint.clone(),
                "us-east-1",
            ))
            .attestation_signer(Arc::new(
                HmacAttestationSigner::single("rolling-fixture-attestation-v1", vec![0x5a; 32])
                    .expect("fixed rolling-fixture attestation key is valid"),
            ))
    }

    async fn ensure_versioned_bucket(&self) -> Result<(), Box<dyn Error>> {
        match self.aws.create_bucket().bucket(&self.bucket).send().await {
            Ok(_) => {}
            Err(error) => {
                let text = format!("{error:?}");
                if !text.contains("BucketAlreadyOwnedByYou")
                    && !text.contains("BucketAlreadyExists")
                {
                    return Err(format!("create rolling bucket failed: {text}").into());
                }
            }
        }
        self.aws
            .put_bucket_versioning()
            .bucket(&self.bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;
        Ok(())
    }

    async fn open(&self, writer: &str) -> prolly_s3_client::Result<Client> {
        self.builder(writer).open().await
    }

    fn plane(&self) -> AwsS3ObjectPlane {
        AwsS3ObjectPlane::new(self.aws.clone(), &self.bucket)
    }
}

async fn assert_payload(
    client: &Client,
    bucket: &str,
    key: &str,
    expected: &[u8],
) -> Result<(), Box<dyn Error>> {
    let body = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .output
        .body
        .collect()
        .await?
        .into_bytes();
    if body.as_ref() != expected {
        return Err(format!("unexpected payload for {key}").into());
    }
    Ok(())
}

async fn put(
    client: &Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> Result<(), Box<dyn Error>> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    Ok(())
}

async fn snapshot(environment: &Environment) -> Result<String, Box<dyn Error>> {
    let plane = environment.plane();
    let mut continuation = None;
    let mut entries = Vec::new();
    loop {
        let page = plane
            .list(ListRequest {
                prefix: format!("{}/", environment.prefix),
                continuation,
                limit: 1_000,
                include_versions: true,
            })
            .await?;
        entries.extend(page.entries.into_iter().map(|entry| {
            (
                entry.path.as_str().to_string(),
                entry.metadata.token.version_id.unwrap_or_default(),
                entry.metadata.token.etag,
                entry.metadata.delete_marker,
                entry.metadata.len,
            )
        }));
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    entries.sort();
    let encoded = encode_canonical(&entries)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

async fn set_requirement(
    environment: &Environment,
    requirement: &str,
    value: u32,
) -> Result<(), Box<dyn Error>> {
    let plane = environment.plane();
    let path = ObjectPath::new(format!("{}/format/v1.cbor", environment.prefix))?;
    let current = plane
        .load_mutable(&path)
        .await?
        .ok_or("format marker is missing")?;
    let mut format: RepositoryFormatV1 = decode_canonical(&current.bytes)?;
    match requirement {
        "reader" => format.min_reader_version = value,
        "writer" => format.min_writer_version = value,
        "profile" => set_profile(&mut format, value)?,
        _ => return Err(format!("unknown requirement {requirement}").into()),
    }
    let outcome = plane
        .compare_exchange(CompareExchange {
            path,
            expected: Some(current.metadata.token),
            bytes: encode_canonical(&format)?,
        })
        .await?;
    if !matches!(outcome, CompareExchangeOutcome::Applied(_)) {
        return Err("format requirement CAS was not applied".into());
    }
    Ok(())
}

#[cfg(not(prolly_s3_legacy_v1_codec))]
fn set_profile(format: &mut RepositoryFormatV1, value: u32) -> Result<(), Box<dyn Error>> {
    format.required_capability_profile = u16::try_from(value)?;
    Ok(())
}

#[cfg(prolly_s3_legacy_v1_codec)]
fn set_profile(_format: &mut RepositoryFormatV1, _value: u32) -> Result<(), Box<dyn Error>> {
    Err("legacy fixture cannot author a capability-profile field".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mode = env::args().nth(1).ok_or("usage: rolling-client <mode>")?;
    let environment = Environment::load().await?;
    match mode.as_str() {
        "init" => {
            environment.ensure_versioned_bucket().await?;
            let client = environment
                .builder("rolling-new-initializer")
                .initialize()
                .await?;
            put(
                &client,
                &environment.bucket,
                "rolling/new-before.txt",
                b"new-before",
            )
            .await?;
        }
        "legacy-write" => {
            let client = environment.open("rolling-legacy-writer").await?;
            assert_payload(
                &client,
                &environment.bucket,
                "rolling/new-before.txt",
                b"new-before",
            )
            .await?;
            put(
                &client,
                &environment.bucket,
                "rolling/legacy-middle.txt",
                b"legacy-middle",
            )
            .await?;
        }
        "new-write" => {
            let client = environment.open("rolling-new-writer").await?;
            assert_payload(
                &client,
                &environment.bucket,
                "rolling/legacy-middle.txt",
                b"legacy-middle",
            )
            .await?;
            put(
                &client,
                &environment.bucket,
                "rolling/new-after.txt",
                b"new-after",
            )
            .await?;
        }
        "verify" => {
            let client = environment.open("rolling-verifier").await?;
            for (key, expected) in [
                ("rolling/new-before.txt", b"new-before".as_slice()),
                ("rolling/legacy-middle.txt", b"legacy-middle".as_slice()),
                ("rolling/new-after.txt", b"new-after".as_slice()),
            ] {
                assert_payload(&client, &environment.bucket, key, expected).await?;
            }
            client.fsck().await?;
        }
        "expect-incompatible" => match environment.open("rolling-rejected").await {
            Ok(_) => return Err("incompatible repository unexpectedly opened".into()),
            Err(error) if error.code == ErrorCode::UnsupportedRepositoryFormat => {
                eprintln!("EXPECTED_INCOMPATIBLE code={:?}", error.code);
            }
            Err(error) => return Err(format!("unexpected open error: {error}").into()),
        },
        "snapshot" => println!("{}", snapshot(&environment).await?),
        "set-requirement" => {
            let requirement = env::args().nth(2).ok_or("missing requirement")?;
            let value = env::args()
                .nth(3)
                .ok_or("missing requirement value")?
                .parse()?;
            set_requirement(&environment, &requirement, value).await?;
        }
        _ => return Err(format!("unknown mode {mode}").into()),
    }
    eprintln!("ROLLING_CLIENT_OK mode={mode}");
    Ok(())
}
