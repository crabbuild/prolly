use std::{sync::Arc, time::SystemTime};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, types::BucketVersioningStatus};
use prolly_s3_client::{
    core::{
        CompareExchange, CompareExchangeOutcome, ObjectPath, ObjectPlane,
        ProviderPerKeyVersionLimit,
    },
    AwsS3ObjectPlane, Client, ErrorCode, HmacAttestationSigner, ProviderIdentity,
};

fn enabled() -> bool {
    std::env::var("PROLLY_S3_AWS").as_deref() == Ok("1")
}

fn unique_prefix(profile: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("prolly-s3-qualification/{profile}/{nanos}")
}

fn signer() -> Arc<HmacAttestationSigner> {
    Arc::new(HmacAttestationSigner::single("aws-qualification", vec![0x51; 32]).unwrap())
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

    if let Ok(bucket) = std::env::var("PROLLY_AWS_BUCKET_UNVERSIONED") {
        let result = Client::builder()
            .aws_client(aws.clone())
            .bucket(&bucket)
            .repository_prefix(unique_prefix("unversioned"))
            .writer("aws-unversioned-qualification")
            .provider_identity(ProviderIdentity::aws_region(region_name.clone()))
            .attestation_signer(signer())
            .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
            .initialize()
            .await;
        let error = match result {
            Ok(_) => panic!("unversioned bucket was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.code, ErrorCode::ProviderNotQualified);
    }

    let bucket = std::env::var("PROLLY_AWS_BUCKET_VERSIONED")
        .expect("PROLLY_AWS_BUCKET_VERSIONED is required");
    let status = aws
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await
        .unwrap()
        .status;
    assert_eq!(status, Some(BucketVersioningStatus::Enabled));

    let prefix = unique_prefix("versioned");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&prefix)
        .writer("aws-versioned-qualification")
        .provider_identity(ProviderIdentity::aws_region(region_name.clone()))
        .attestation_signer(signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();

    let first = client
        .put_object("matrix/history.bin", b"first".to_vec())
        .await
        .unwrap();
    client
        .put_object("matrix/history.bin", b"second".to_vec())
        .await
        .unwrap();
    assert_eq!(
        client
            .get_object_at(first.id, "matrix/history.bin")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"first"
    );

    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
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
}
