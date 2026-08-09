#![recursion_limit = "256"]

use std::collections::HashMap;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, Get, GlobalSecondaryIndex, KeySchemaElement, KeyType,
    KeysAndAttributes, Projection, ProjectionType, Put as TransactPut, PutRequest, ReturnValue,
    ReturnValuesOnConditionCheckFailure, ScalarAttributeType, Select, TransactGetItem,
    TransactWriteItem, WriteRequest,
};
use prolly::MapVersionId;
use prolly_dynamodb_client::{
    Client, IndexReconfigurationPlan, KeyAttribute, KeyKind, MaintenanceContext, RetentionPolicy,
    SecondaryIndexDefinition, SecondaryIndexKind, SecondaryIndexProjection, StreamWorkerOptions,
    TableArchive, TableArchiveLimits, TtlWorkerOptions,
};

#[allow(dead_code)]
fn advertised_fluent_chains_compile(
    client: &Client,
    version: MapVersionId,
    archive: TableArchive,
    index_plan: IndexReconfigurationPlan,
) {
    fn assert_stream<S: futures_util::Stream>(_: S) {}
    fn assert_send<T: Send>(_: T) {}
    let _ = Client::builder()
        .logical_retry_limit(3)
        .node_cache_max_nodes(1_000)
        .node_cache_max_bytes(64 * 1024 * 1024)
        .set_logical_retry_limit(Some(4))
        .set_node_cache_max_nodes(Some(2_000))
        .set_node_cache_max_bytes(Some(128 * 1024 * 1024));
    let _ = client
        .create_table()
        .table_name("Orders")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("accountId")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("status")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("accountId")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("ByStatus")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("status")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .request_token("create-orders");
    let _ = client
        .put_item()
        .table_name("Orders")
        .item("accountId", AttributeValue::S("acct-1".into()))
        .item("status", AttributeValue::S("OPEN".into()))
        .return_values(ReturnValue::AllOld)
        .request_token("put-order");
    let _ = client
        .get_item()
        .table_name("Orders")
        .key("accountId", AttributeValue::S("acct-1".into()))
        .projection_expression("#status")
        .expression_attribute_names("#status", "status");
    let _ = client
        .table("Orders")
        .at(version.clone())
        .get_item()
        .key("accountId", AttributeValue::S("acct-1".into()));
    let _ = client
        .table("Orders")
        .at(version.clone())
        .query()
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", "accountId")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":pk", AttributeValue::S("acct-1".into()))
        .expression_attribute_values(":open", AttributeValue::S("OPEN".into()))
        .filter_expression("#status = :open")
        .projection_expression("#status")
        .select(Select::SpecificAttributes);
    let _ = client.table("Orders").at(version.clone()).scan().limit(25);
    let _ = client
        .table("Orders")
        .if_head(version.clone())
        .put_item()
        .item("accountId", AttributeValue::S("acct-1".into()))
        .condition_expression("attribute_not_exists(#pk)")
        .expression_attribute_names("#pk", "accountId");
    let _ = client
        .table("Orders")
        .if_head(version.clone())
        .update_item()
        .key("accountId", AttributeValue::S("acct-1".into()))
        .update_expression("ADD #count :one")
        .expression_attribute_names("#count", "count")
        .expression_attribute_values(":one", AttributeValue::N("1".into()));
    let _ = client
        .delete_item()
        .table_name("Orders")
        .key("accountId", AttributeValue::S("acct-1".into()))
        .return_values(ReturnValue::AllOld)
        .request_token("delete-order");
    let _ = client
        .update_item()
        .table_name("Orders")
        .key("accountId", AttributeValue::S("acct-1".into()))
        .update_expression("SET #status = :closed")
        .condition_expression("#status = :open")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":closed", AttributeValue::S("CLOSED".into()))
        .expression_attribute_values(":open", AttributeValue::S("OPEN".into()))
        .return_values(ReturnValue::AllNew)
        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        .request_token("update-order");
    let _ = client.describe_table().table_name("Orders");
    let _ = client.list_tables().limit(25);
    let _ = client
        .delete_table()
        .table_name("Orders")
        .request_token("delete-orders-table");
    let _ = client
        .table("Orders")
        .restore(version.clone())
        .expected_head(version.clone())
        .request_token("restore-orders");
    assert_stream(
        client
            .table("Orders")
            .diff(version.clone(), version.clone())
            .page_size(25)
            .into_stream(),
    );
    let retention = client
        .table("Orders")
        .retention(RetentionPolicy::keep_last(100));
    assert_send(retention.plan());
    let context = MaintenanceContext::new("records-officer", "scheduled retention");
    let indexes = client
        .table("Orders")
        .indexes(vec![SecondaryIndexDefinition {
            name: "ByStatus".into(),
            kind: SecondaryIndexKind::Global,
            partition_key: KeyAttribute {
                name: "status".into(),
                kind: KeyKind::String,
            },
            sort_key: None,
            projection: SecondaryIndexProjection::All,
        }]);
    assert_send(indexes.plan());
    assert_send(
        client
            .table("Orders")
            .apply_indexes(&index_plan, context.clone()),
    );
    assert_send(client.table("Orders").indexes_audit(&index_plan.id));
    let archive_limits = TableArchiveLimits::new(100, 1024, 100, 1024, 4096);
    assert_send(client.table("Orders").export(archive_limits));
    assert_send(
        client
            .table("Orders")
            .at(version.clone())
            .export(archive_limits),
    );
    let import = client.import(archive, "OrdersRecovered", archive_limits);
    assert_send(import.plan());
    assert_send(client.workers().stream(StreamWorkerOptions::new(
        "Orders",
        "audit-journal",
        "worker-a",
    )));
    assert_send(
        client
            .workers()
            .ttl(TtlWorkerOptions::new("Orders", "expiresAt", "worker-a")),
    );
    assert_send(client.workers().maintenance(context, 60_000));
    assert_stream(
        client
            .table("Orders")
            .versions()
            .page_size(25)
            .into_stream(),
    );
    assert_stream(
        client
            .query()
            .table_name("Orders")
            .index_name("ByStatus")
            .key_condition_expression("#pk = :pk")
            .expression_attribute_names("#pk", "status")
            .expression_attribute_values(":pk", AttributeValue::S("OPEN".into()))
            .limit(25)
            .scan_index_forward(false)
            .consistent_read(false)
            .into_paginator()
            .into_stream(),
    );
    assert_stream(
        client
            .scan()
            .table_name("Orders")
            .index_name("ByStatus")
            .filter_expression("#status = :open")
            .projection_expression("#status")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":open", AttributeValue::S("OPEN".into()))
            .limit(25)
            .select(Select::SpecificAttributes)
            .consistent_read(false)
            .into_paginator()
            .into_stream(),
    );
    let batch_keys = KeysAndAttributes::builder()
        .keys(HashMap::from([(
            "accountId".into(),
            AttributeValue::S("acct-1".into()),
        )]))
        .projection_expression("#status")
        .expression_attribute_names("#status", "status")
        .build()
        .unwrap();
    let _ = client
        .batch_get_item()
        .request_items("Orders", batch_keys)
        .at([("Orders".into(), version)]);
    let write = WriteRequest::builder()
        .put_request(
            PutRequest::builder()
                .item("accountId", AttributeValue::S("acct-2".into()))
                .build()
                .unwrap(),
        )
        .build();
    let _ = client
        .batch_write_item()
        .request_items("Orders", vec![write]);
    let transactional_get = Get::builder()
        .table_name("Orders")
        .key("accountId", AttributeValue::S("acct-1".into()))
        .projection_expression("#status")
        .expression_attribute_names("#status", "status")
        .build()
        .unwrap();
    let _ = client
        .transact_get_items()
        .transact_items(TransactGetItem::builder().get(transactional_get).build());
    let transactional_put = TransactPut::builder()
        .table_name("Orders")
        .item("accountId", AttributeValue::S("acct-3".into()))
        .condition_expression("attribute_not_exists(#pk)")
        .expression_attribute_names("#pk", "accountId")
        .build()
        .unwrap();
    let _ = client
        .transact_write_items()
        .transact_items(TransactWriteItem::builder().put(transactional_put).build())
        .client_request_token("transactional-put");
}

#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
fn advertised_input_first_paths_compile(
    client: &Client,
    create: aws_sdk_dynamodb::operation::create_table::CreateTableInput,
    describe: aws_sdk_dynamodb::operation::describe_table::DescribeTableInput,
    list: aws_sdk_dynamodb::operation::list_tables::ListTablesInput,
    delete_table: aws_sdk_dynamodb::operation::delete_table::DeleteTableInput,
    get: aws_sdk_dynamodb::operation::get_item::GetItemInput,
    put: aws_sdk_dynamodb::operation::put_item::PutItemInput,
    delete: aws_sdk_dynamodb::operation::delete_item::DeleteItemInput,
    update: aws_sdk_dynamodb::operation::update_item::UpdateItemInput,
    query: aws_sdk_dynamodb::operation::query::QueryInput,
    scan: aws_sdk_dynamodb::operation::scan::ScanInput,
    batch_get: aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput,
    batch_write: aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemInput,
    transact_get: aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsInput,
    transact_write: aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsInput,
) {
    fn assert_send<T: Send>(_: T) {}
    assert_send(client.execute_create_table(create));
    assert_send(client.execute_describe_table(describe));
    assert_send(client.execute_list_tables(list));
    assert_send(client.execute_delete_table(delete_table));
    assert_send(client.execute_get_item(get));
    assert_send(client.execute_put_item(put));
    assert_send(client.execute_delete_item(delete));
    assert_send(client.execute_update_item(update));
    assert_send(client.execute_query(query));
    assert_send(client.execute_scan(scan));
    assert_send(client.execute_batch_get_item(batch_get));
    assert_send(client.execute_batch_write_item(batch_write));
    assert_send(client.execute_transact_get_items(transact_get));
    assert_send(client.execute_transact_write_items(transact_write));
}

#[test]
fn public_builders_are_send() {
    fn assert_send<T: Send>() {}
    assert_send::<prolly_dynamodb_client::operation::GetItem>();
    assert_send::<prolly_dynamodb_client::operation::PutItem>();
    assert_send::<prolly_dynamodb_client::operation::DeleteItem>();
    assert_send::<prolly_dynamodb_client::operation::UpdateItem>();
    assert_send::<prolly_dynamodb_client::operation::CreateTable>();
    assert_send::<prolly_dynamodb_client::operation::BatchGetItem>();
    assert_send::<prolly_dynamodb_client::operation::BatchWriteItem>();
    assert_send::<prolly_dynamodb_client::operation::TransactGetItems>();
    assert_send::<prolly_dynamodb_client::operation::TransactWriteItems>();
    assert_send::<prolly_dynamodb_client::Restore>();
    assert_send::<prolly_dynamodb_client::Import>();
    assert_send::<prolly_dynamodb_client::operation::QueryPaginator>();
    assert_send::<prolly_dynamodb_client::operation::ScanPaginator>();
    assert_send::<prolly_dynamodb_client::StreamWorker>();
    assert_send::<prolly_dynamodb_client::TtlWorker>();
    assert_send::<prolly_dynamodb_client::MaintenanceWorker>();
}
