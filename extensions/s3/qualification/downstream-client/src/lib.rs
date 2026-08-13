use prolly_s3_client::{
    core::{CommitId, CommitReceipt, ObjectData},
    Client, Result, S3WireAttemptInterceptor,
};

pub fn compile_wire_metrics_surface(
    builder: aws_sdk_s3::config::Builder,
) -> (aws_sdk_s3::Config, S3WireAttemptInterceptor) {
    let metrics = S3WireAttemptInterceptor::new();
    (builder.interceptor(metrics.clone()).build(), metrics)
}

pub fn compile_fluent_surface(client: &Client) {
    let _metrics = client.s3_operation_metrics();
    let _put = client.put_object("downstream/compile.txt", b"compile".to_vec());
    let _get = client.get_object("downstream/compile.txt");
    let _list = client.list_objects("downstream/", None, 100);
}

pub async fn compile_surface(
    client: &Client,
    snapshot: CommitId,
) -> Result<(CommitReceipt, Option<ObjectData>)> {
    let receipt = client
        .put_object("downstream/compile.txt", b"compile".to_vec())
        .await?;
    let historical = client
        .get_object_at(snapshot, "downstream/compile.txt")
        .await?;
    Ok((receipt, historical))
}
