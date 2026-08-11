use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_smithy_types::retry::RetryConfig;
use prolly_s3_client::S3WireAttemptInterceptor;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn interceptor_counts_one_execution_and_two_transmissions_for_a_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for ordinal in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 16 * 1024];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            let request_line = request.lines().next().unwrap_or_default();
            assert!(request_line.starts_with("GET /fixture"), "{request_line}");
            assert!(request_line.contains("list-type=2"), "{request_line}");
            let (status, body) = if ordinal == 0 {
                (
                    "503 Service Unavailable",
                    "<Error><Code>SlowDown</Code><Message>retry fixture</Message></Error>",
                )
            } else {
                (
                    "200 OK",
                    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>fixture</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>",
                )
            };
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });

    let interceptor = S3WireAttemptInterceptor::new();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            "fixture-access-key",
            "fixture-secret-key",
            None,
            None,
            "wire-attempt-fixture",
        ))
        .endpoint_url(format!("http://{address}"))
        .force_path_style(true)
        .retry_config(RetryConfig::standard().with_max_attempts(2))
        .interceptor(interceptor.clone())
        .build();
    let client = aws_sdk_s3::Client::from_conf(config);
    client
        .list_objects_v2()
        .bucket("fixture")
        .send()
        .await
        .unwrap();
    server.await.unwrap();

    let metrics = interceptor.metrics();
    assert_eq!(metrics.executions, 1);
    assert_eq!(metrics.transmissions, 2);
    assert_eq!(metrics.completed_attempts, 2);
    assert_eq!(metrics.retry_transmissions(), 1);
    assert_eq!(metrics.server_error_responses, 1);
    assert_eq!(metrics.successful_responses, 1);
    assert_eq!(metrics.informational_responses, 0);
    assert_eq!(metrics.redirection_responses, 0);
    assert_eq!(metrics.client_error_responses, 0);
    assert_eq!(metrics.unclassified_responses, 0);
    assert_eq!(metrics.attempts_without_response, 0);

    let reset = interceptor.reset();
    assert_eq!(reset, metrics);
    assert_eq!(interceptor.metrics(), Default::default());
}
