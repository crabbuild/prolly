use aws_sdk_s3::{
    operation::put_object::PutObjectInput,
    primitives::ByteStream,
};
use prolly_s3_client::{
    Client, QualifiedClone, Result, S3OperationMetrics, S3WireAttemptInterceptor, Versioned,
    WriteOptions,
};

pub fn compile_wire_metrics_surface(
    builder: aws_sdk_s3::config::Builder,
) -> (aws_sdk_s3::Config, S3WireAttemptInterceptor) {
    let metrics = S3WireAttemptInterceptor::new();
    (builder.interceptor(metrics.clone()).build(), metrics)
}

pub fn compile_fluent_surface(client: &Client) {
    let _layout = client.physical_layout();
    let _metrics = client.s3_operation_metrics();
    let _put = client
        .put_object()
        .bucket(client.bucket())
        .key("downstream/compile.txt")
        .body(ByteStream::from_static(b"compile"));
    let _get = client
        .get_object()
        .bucket(client.bucket())
        .key("downstream/compile.txt");
    let _list = client.list_objects_v2().bucket(client.bucket()).prefix("downstream/");
}

pub fn compile_clone_metrics_surface(clone: &QualifiedClone) -> S3OperationMetrics {
    clone.target_s3_metrics
}

pub async fn compile_official_input_surface(
    client: &Client,
    input: PutObjectInput,
) -> Result<Versioned<aws_sdk_s3::operation::put_object::PutObjectOutput>> {
    client.execute_put_object(input, WriteOptions::default()).await
}

#[cfg(feature = "slatedb-index")]
pub fn compile_slatedb_surface(index: &prolly_s3_client::SlateDbAdvisoryIndex) -> &str {
    index.path()
}
