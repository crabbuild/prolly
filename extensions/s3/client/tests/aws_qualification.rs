use std::{sync::Arc, time::SystemTime};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, primitives::ByteStream, types::BucketVersioningStatus};
use prolly_s3_client::{
    core::{CompareExchange, CompareExchangeOutcome, ObjectPath, ObjectPlane},
    AwsS3ObjectPlane, Client, ErrorCode, HmacAttestationSigner, HmacTokenSigner, ProviderIdentity,
};

fn enabled() -> bool {
    std::env::var("PROLLY_S3_AWS").as_deref() == Ok("1")
}

fn unique_prefix(profile: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("prolly-s3-qualification/{profile}/{nanos}")
}

fn signer() -> Arc<HmacAttestationSigner> {
    Arc::new(HmacAttestationSigner::single("aws-qualification-v1", vec![0x51; 32]).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aws_general_purpose_bucket_qualification_matrix() {
    if !enabled() {
        eprintln!("set PROLLY_S3_AWS=1 and the documented AWS bucket variables to run");
        return;
    }
    let region_name = std::env::var("PROLLY_AWS_REGION")
        .expect("PROLLY_AWS_REGION is required when PROLLY_S3_AWS=1");
    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region_name.clone()))
        .load()
        .await;
    let aws = aws_sdk_s3::Client::new(&shared);
    let mut profiles = Vec::new();
    if let Ok(bucket) = std::env::var("PROLLY_AWS_BUCKET_UNVERSIONED") {
        profiles.push(("unversioned", bucket, false));
    }
    if let Ok(bucket) = std::env::var("PROLLY_AWS_BUCKET_VERSIONED") {
        profiles.push(("versioned", bucket, true));
    }
    assert!(
        !profiles.is_empty(),
        "set PROLLY_AWS_BUCKET_UNVERSIONED and/or PROLLY_AWS_BUCKET_VERSIONED"
    );

    for (profile, bucket, expect_versioned) in profiles {
        let status = aws
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .status;
        assert_eq!(
            status == Some(BucketVersioningStatus::Enabled),
            expect_versioned,
            "AWS qualification bucket has the wrong native versioning profile"
        );
        let prefix = unique_prefix(profile);
        let client = Client::builder()
            .aws_client(aws.clone())
            .bucket(&bucket)
            .repository_prefix(&prefix)
            .writer(format!("aws-{profile}-qualification"))
            .provider_identity(ProviderIdentity::aws_region(region_name.clone()))
            .attestation_signer(signer())
            .token_signer(Arc::new(
                HmacTokenSigner::single("aws-cursor-v1", vec![0x71; 32]).unwrap(),
            ))
            .initialize()
            .await
            .unwrap();

        let first = client
            .put_object()
            .bucket(&bucket)
            .key("matrix/range.bin")
            .if_none_match("*")
            .body(ByteStream::from_static(b"abcdef"))
            .send()
            .await
            .unwrap();
        let etag = first.output.e_tag().unwrap();
        assert_eq!(
            client
                .get_object()
                .bucket(&bucket)
                .key("matrix/range.bin")
                .if_match(etag)
                .range("bytes=1-3")
                .send()
                .await
                .unwrap()
                .output
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            b"bcd"
        );
        assert_eq!(
            client
                .put_object()
                .bucket(&bucket)
                .key("matrix/range.bin")
                .if_none_match("*")
                .body(ByteStream::from_static(b"must-not-publish"))
                .send()
                .await
                .unwrap_err()
                .code,
            ErrorCode::PreconditionFailed
        );

        let plane = Arc::new(AwsS3ObjectPlane::new(aws.clone(), &bucket));
        let ref_path = ObjectPath::new(format!("{prefix}/qualification/32-writer-ref")).unwrap();
        let created = match plane
            .compare_exchange(CompareExchange {
                path: ref_path.clone(),
                expected: None,
                bytes: b"base".to_vec(),
            })
            .await
            .unwrap()
        {
            CompareExchangeOutcome::Applied(metadata) => metadata,
            CompareExchangeOutcome::Conflict(_) => panic!("isolated AWS CAS path already existed"),
        };
        let writers = (0..32)
            .map(|ordinal| {
                let plane = plane.clone();
                let path = ref_path.clone();
                let expected = created.token.clone();
                tokio::spawn(async move {
                    plane
                        .compare_exchange(CompareExchange {
                            path,
                            expected: Some(expected),
                            bytes: format!("writer-{ordinal}").into_bytes(),
                        })
                        .await
                })
            })
            .collect::<Vec<_>>();
        let mut applied = 0;
        let mut conflicts = 0;
        for writer in writers {
            match writer.await.unwrap().unwrap() {
                CompareExchangeOutcome::Applied(_) => applied += 1,
                CompareExchangeOutcome::Conflict(_) => conflicts += 1,
            }
        }
        assert_eq!((applied, conflicts), (1, 31));
        client.fsck().await.unwrap();
    }

    if let Ok(identifiers) = std::env::var("PROLLY_AWS_REJECT_IDENTIFIERS") {
        for identifier in identifiers
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let result = Client::builder()
                .aws_client(aws.clone())
                .bucket(identifier)
                .repository_prefix(unique_prefix("rejection"))
                .writer("aws-rejection-qualification")
                .provider_identity(ProviderIdentity::aws_region(region_name.clone()))
                .attestation_signer(signer())
                .initialize()
                .await;
            assert_eq!(
                result
                    .err()
                    .expect("unsupported AWS identifier was accepted")
                    .code,
                ErrorCode::ProviderNotQualified
            );
        }
    }
}
