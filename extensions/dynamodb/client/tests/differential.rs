use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use prolly_dynamodb_client::{Client, Error};
use prolly_store_dynamodb::DynamoDbBackend;

#[test]
fn supported_expression_and_pagination_subset_matches_dynamodb_local() {
    if std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT").is_err() {
        eprintln!("skipping: PROLLY_STORE_DYNAMODB_ENDPOINT is not set");
        return;
    }
    std::thread::Builder::new()
        .name("dynamodb-differential".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(run_differential());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_differential() {
    let endpoint = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT").unwrap();
    let physical_table = std::env::var("PROLLY_DYNAMODB_CLIENT_TEST_TABLE")
        .unwrap_or_else(|_| "prolly-versioned-client-test".into());
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let native_table = format!("NativeDiff{}{}", std::process::id(), nonce);
    let logical_table = "DifferentialOrders";
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let native = aws_sdk_dynamodb::Client::from_conf(config);
    let backend = DynamoDbBackend::new(native.clone(), &physical_table)
        .with_root_table_name(format!("{physical_table}-custom-roots"))
        .with_key_prefix(format!("differential-{}-{nonce}:", std::process::id()).into_bytes());
    backend.initialize_schema().await.unwrap();
    let cleanup = backend.clone();
    let client = Client::open(backend).await.unwrap();

    create_native_table(&native, &native_table).await;
    client
        .create_table()
        .table_name(logical_table)
        .attribute_definitions(string_attribute("account"))
        .attribute_definitions(number_attribute("sequence"))
        .key_schema(hash_key("account"))
        .key_schema(range_key("sequence"))
        .send()
        .await
        .unwrap();

    for (sequence, status) in [("1", "OPEN"), ("2", "OPEN"), ("10", "MATCH")] {
        let item = order_item(sequence, status);
        native
            .put_item()
            .table_name(&native_table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        client
            .put_item()
            .table_name(logical_table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    assert_decimal_semantics(&native, &native_table, &client, logical_table).await;

    // One expression covers nested paths, immutable-old-item path copies,
    // arithmetic, set deletion, condition functions, and ALL_NEW conversion.
    let native_update = update_request(&native_table);
    let logical_update = update_request(logical_table);
    let native_updated = native
        .update_item()
        .set_table_name(native_update.table_name)
        .set_key(native_update.key)
        .set_update_expression(native_update.update_expression)
        .set_condition_expression(native_update.condition_expression)
        .set_expression_attribute_names(native_update.expression_attribute_names)
        .set_expression_attribute_values(native_update.expression_attribute_values)
        .set_return_values(native_update.return_values)
        .send()
        .await
        .unwrap();
    let logical_updated = client.execute_update_item(logical_update).await.unwrap();
    assert_eq!(logical_updated.attributes, native_updated.attributes);

    let native_projection = native
        .get_item()
        .table_name(&native_table)
        .set_key(Some(key("1")))
        .projection_expression("#profile.#name, #count, #copied")
        .expression_attribute_names("#profile", "profile")
        .expression_attribute_names("#name", "name")
        .expression_attribute_names("#count", "count")
        .expression_attribute_names("#copied", "copiedStatus")
        .send()
        .await
        .unwrap();
    let logical_projection = client
        .get_item()
        .table_name(logical_table)
        .set_key(Some(key("1")))
        .projection_expression("#profile.#name, #count, #copied")
        .expression_attribute_names("#profile", "profile")
        .expression_attribute_names("#name", "name")
        .expression_attribute_names("#count", "count")
        .expression_attribute_names("#copied", "copiedStatus")
        .send()
        .await
        .unwrap();
    assert_eq!(logical_projection.item, native_projection.item);

    let native_failure = native
        .update_item()
        .table_name(&native_table)
        .set_key(Some(key("1")))
        .update_expression("SET #status = :closed")
        .condition_expression("#status = :missing")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":closed", AttributeValue::S("CLOSED".into()))
        .expression_attribute_values(":missing", AttributeValue::S("MISSING".into()))
        .return_values_on_condition_check_failure(
            aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure::AllOld,
        )
        .send()
        .await
        .unwrap_err();
    assert!(native_failure
        .as_service_error()
        .is_some_and(|error| error.is_conditional_check_failed_exception()));
    let logical_failure = client
        .update_item()
        .table_name(logical_table)
        .set_key(Some(key("1")))
        .update_expression("SET #status = :closed")
        .condition_expression("#status = :missing")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":closed", AttributeValue::S("CLOSED".into()))
        .expression_attribute_values(":missing", AttributeValue::S("MISSING".into()))
        .return_values_on_condition_check_failure(
            aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure::AllOld,
        )
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        &logical_failure,
        Error::ConditionalCheckFailed { .. }
    ));
    let native_failure_item = native_failure
        .as_service_error()
        .and_then(|error| match error {
            aws_sdk_dynamodb::operation::update_item::UpdateItemError::ConditionalCheckFailedException(error) => error.item(),
            _ => None,
        });
    assert_eq!(
        logical_failure.conditional_failure_item(),
        native_failure_item
    );

    for (expression, names, values, expected) in filter_cases() {
        assert_filter_matches(
            &native,
            &native_table,
            &client,
            logical_table,
            expression,
            names,
            values,
            expected,
        )
        .await;
    }

    // A second mutation covers the remaining advertised update clauses and
    // UPDATED_NEW projection. Both engines must evaluate nested function and
    // arithmetic operands from the same immutable pre-update item.
    let native_followup = followup_update_request(&native_table);
    let logical_followup = followup_update_request(logical_table);
    let native_followup_output = native
        .update_item()
        .set_table_name(native_followup.table_name)
        .set_key(native_followup.key)
        .set_update_expression(native_followup.update_expression)
        .set_condition_expression(native_followup.condition_expression)
        .set_expression_attribute_names(native_followup.expression_attribute_names)
        .set_expression_attribute_values(native_followup.expression_attribute_values)
        .set_return_values(native_followup.return_values)
        .send()
        .await
        .unwrap();
    let logical_followup_output = client.execute_update_item(logical_followup).await.unwrap();
    assert_eq!(
        logical_followup_output.attributes,
        native_followup_output.attributes
    );
    let native_followup_item = native
        .get_item()
        .table_name(&native_table)
        .set_key(Some(key("1")))
        .send()
        .await
        .unwrap();
    let logical_followup_item = client
        .get_item()
        .table_name(logical_table)
        .set_key(Some(key("1")))
        .send()
        .await
        .unwrap();
    assert_eq!(logical_followup_item.item, native_followup_item.item);

    // Limit applies before filtering: the first page is empty but must retain
    // a continuation because later evaluated items can match.
    let native_first = query_page(&native, &native_table, None, true, 1, true).await;
    let logical_first = client
        .query()
        .table_name(logical_table)
        .key_condition_expression("#account = :account")
        .filter_expression("#status = :match")
        .expression_attribute_names("#account", "account")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":account", AttributeValue::S("acct-1".into()))
        .expression_attribute_values(":match", AttributeValue::S("MATCH".into()))
        .limit(1)
        .scan_index_forward(true)
        .send()
        .await
        .unwrap();
    assert_eq!(logical_first.items, native_first.items);
    assert_eq!(logical_first.count, native_first.count);
    assert_eq!(logical_first.scanned_count, native_first.scanned_count);
    assert!(logical_first.items().is_empty());
    assert!(logical_first.last_evaluated_key.is_some());
    assert!(native_first.last_evaluated_key.is_some());

    let native_forward = collect_query(&native, &native_table, true).await;
    let logical_forward = collect_logical_query(&client, logical_table, true).await;
    assert_eq!(logical_forward, native_forward);
    assert_eq!(logical_forward, vec!["1", "2", "10"]);
    let native_reverse = collect_query(&native, &native_table, false).await;
    let logical_reverse = collect_logical_query(&client, logical_table, false).await;
    assert_eq!(logical_reverse, native_reverse);
    assert_eq!(logical_reverse, vec!["10", "2", "1"]);

    // Scan order is not a DynamoDB contract, so compare logical sets, counts,
    // and multi-page continuation completeness rather than physical order.
    let native_scan = collect_native_scan(&native, &native_table).await;
    let logical_scan = collect_logical_scan(&client, logical_table).await;
    assert_eq!(logical_scan, native_scan);

    native
        .delete_table()
        .table_name(&native_table)
        .send()
        .await
        .unwrap();
    cleanup.clear_namespace().await.unwrap();
}

async fn create_native_table(client: &aws_sdk_dynamodb::Client, table: &str) {
    client
        .create_table()
        .table_name(table)
        .attribute_definitions(string_attribute("account"))
        .attribute_definitions(number_attribute("sequence"))
        .key_schema(hash_key("account"))
        .key_schema(range_key("sequence"))
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
}

fn string_attribute(name: &str) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(ScalarAttributeType::S)
        .build()
        .unwrap()
}

fn number_attribute(name: &str) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(ScalarAttributeType::N)
        .build()
        .unwrap()
}

fn hash_key(name: &str) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(KeyType::Hash)
        .build()
        .unwrap()
}

fn range_key(name: &str) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(KeyType::Range)
        .build()
        .unwrap()
}

fn key(sequence: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("account".into(), AttributeValue::S("acct-1".into())),
        ("sequence".into(), AttributeValue::N(sequence.into())),
    ])
}

fn order_item(sequence: &str, status: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("account".into(), AttributeValue::S("acct-1".into())),
        ("sequence".into(), AttributeValue::N(sequence.into())),
        ("status".into(), AttributeValue::S(status.into())),
        ("count".into(), AttributeValue::N("1".into())),
        (
            "profile".into(),
            AttributeValue::M(HashMap::from([(
                "name".into(),
                AttributeValue::S("before".into()),
            )])),
        ),
        (
            "flags".into(),
            AttributeValue::Ss(vec!["keep".into(), "remove".into()]),
        ),
    ])
}

async fn assert_decimal_semantics(
    native: &aws_sdk_dynamodb::Client,
    native_table: &str,
    logical: &Client,
    logical_table: &str,
) {
    let invalid = HashMap::from([
        (
            "account".into(),
            AttributeValue::S("acct-invalid-number".into()),
        ),
        ("sequence".into(), AttributeValue::N("1".into())),
        (
            "tooPrecise".into(),
            AttributeValue::N("111111111111111111111111111111111111111".into()),
        ),
    ]);
    let native_invalid = native
        .put_item()
        .table_name(native_table)
        .set_item(Some(invalid.clone()))
        .send()
        .await
        .unwrap_err();
    assert!(native_invalid
        .as_service_error()
        .is_some_and(|error| error.code() == Some("ValidationException")));
    let logical_invalid = logical
        .put_item()
        .table_name(logical_table)
        .set_item(Some(invalid))
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        logical_invalid,
        Error::Core(prolly_dynamodb_core::Error::Validation(_))
    ));

    let exact = HashMap::from([
        ("account".into(), AttributeValue::S("acct-number".into())),
        ("sequence".into(), AttributeValue::N("1.2300".into())),
        (
            "maximumPrecision".into(),
            AttributeValue::N("99999999999999999999999999999999999999".into()),
        ),
        ("minimumExponent".into(), AttributeValue::N("1e-130".into())),
        ("signedZero".into(), AttributeValue::N("-0".into())),
        (
            "balance".into(),
            AttributeValue::N("99999999999999999999.99".into()),
        ),
    ]);
    native
        .put_item()
        .table_name(native_table)
        .set_item(Some(exact.clone()))
        .send()
        .await
        .unwrap();
    logical
        .put_item()
        .table_name(logical_table)
        .set_item(Some(exact))
        .send()
        .await
        .unwrap();
    let exact_key = HashMap::from([
        ("account".into(), AttributeValue::S("acct-number".into())),
        ("sequence".into(), AttributeValue::N("1.2300".into())),
    ]);
    let native_exact = native
        .get_item()
        .table_name(native_table)
        .set_key(Some(exact_key.clone()))
        .send()
        .await
        .unwrap();
    let logical_exact = logical
        .get_item()
        .table_name(logical_table)
        .set_key(Some(exact_key))
        .send()
        .await
        .unwrap();
    assert_eq!(logical_exact.item, native_exact.item);

    let native_decimal_update = decimal_update_request(native_table);
    let logical_decimal_update = decimal_update_request(logical_table);
    let native_decimal_output = native
        .update_item()
        .set_table_name(native_decimal_update.table_name)
        .set_key(native_decimal_update.key)
        .set_update_expression(native_decimal_update.update_expression)
        .set_expression_attribute_names(native_decimal_update.expression_attribute_names)
        .set_expression_attribute_values(native_decimal_update.expression_attribute_values)
        .set_return_values(native_decimal_update.return_values)
        .send()
        .await
        .unwrap();
    let logical_decimal_output = logical
        .execute_update_item(logical_decimal_update)
        .await
        .unwrap();
    let native_decimal_item = native_decimal_output.attributes.unwrap();
    let logical_decimal_item = logical_decimal_output.attributes.unwrap();
    assert_eq!(logical_decimal_item.len(), native_decimal_item.len());
    for (name, expected) in [
        ("balance", "100000000000000000000"),
        (
            "maximumPrecision",
            "100000000000000000000000000000000000000",
        ),
        ("minimumExponent", "0"),
        ("sequence", "1.23"),
        ("signedZero", "0"),
    ] {
        assert_eq!(
            canonical_number(&native_decimal_item[name]),
            expected,
            "{name}"
        );
        assert_eq!(
            logical_decimal_item[name].as_n().unwrap(),
            expected,
            "{name}"
        );
    }
    assert_eq!(
        logical_decimal_item["account"],
        native_decimal_item["account"]
    );

    let ordered = ["-100", "-1.5", "-0.0001", "0", "0.0001", "1.5", "100"];
    for number in ordered {
        let item = HashMap::from([
            ("account".into(), AttributeValue::S("acct-sort".into())),
            ("sequence".into(), AttributeValue::N(number.into())),
        ]);
        native
            .put_item()
            .table_name(native_table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        logical
            .put_item()
            .table_name(logical_table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
    let native_ordered = native
        .query()
        .table_name(native_table)
        .key_condition_expression("#account = :account")
        .expression_attribute_names("#account", "account")
        .expression_attribute_values(":account", AttributeValue::S("acct-sort".into()))
        .send()
        .await
        .unwrap();
    let logical_ordered = logical
        .query()
        .table_name(logical_table)
        .key_condition_expression("#account = :account")
        .expression_attribute_names("#account", "account")
        .expression_attribute_values(":account", AttributeValue::S("acct-sort".into()))
        .send()
        .await
        .unwrap();
    let native_numbers = numeric_values(native_ordered.items());
    let logical_numbers = numeric_values(logical_ordered.items());
    assert_eq!(logical_numbers, native_numbers);
    assert_eq!(logical_numbers, ordered);
}

fn numeric_values(items: &[HashMap<String, AttributeValue>]) -> Vec<String> {
    items
        .iter()
        .map(|item| item["sequence"].as_n().unwrap().to_owned())
        .collect()
}

fn canonical_number(value: &AttributeValue) -> String {
    prolly_dynamodb_core::DynamoNumber::parse(value.as_n().unwrap())
        .unwrap()
        .as_str()
        .to_owned()
}

fn decimal_update_request(
    table: &str,
) -> aws_sdk_dynamodb::operation::update_item::UpdateItemInput {
    aws_sdk_dynamodb::operation::update_item::UpdateItemInput::builder()
        .table_name(table)
        .key("account", AttributeValue::S("acct-number".into()))
        .key("sequence", AttributeValue::N("1.2300".into()))
        .update_expression(
            "SET #balance = #balance + :cent, #maximum = #maximum + :one, #minimum = #minimum - :quantum",
        )
        .expression_attribute_names("#balance", "balance")
        .expression_attribute_names("#maximum", "maximumPrecision")
        .expression_attribute_names("#minimum", "minimumExponent")
        .expression_attribute_values(":cent", AttributeValue::N("0.01".into()))
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .expression_attribute_values(":quantum", AttributeValue::N("1e-130".into()))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
        .build()
        .unwrap()
}

fn update_request(table: &str) -> aws_sdk_dynamodb::operation::update_item::UpdateItemInput {
    aws_sdk_dynamodb::operation::update_item::UpdateItemInput::builder()
        .table_name(table)
        .set_key(Some(key("1")))
        .update_expression(
            "SET #profile.#name = :after, #copied = #status ADD #count :one DELETE #flags :remove",
        )
        .condition_expression("#status = :open AND attribute_exists(#account)")
        .expression_attribute_names("#profile", "profile")
        .expression_attribute_names("#name", "name")
        .expression_attribute_names("#copied", "copiedStatus")
        .expression_attribute_names("#status", "status")
        .expression_attribute_names("#count", "count")
        .expression_attribute_names("#flags", "flags")
        .expression_attribute_names("#account", "account")
        .expression_attribute_values(":after", AttributeValue::S("after".into()))
        .expression_attribute_values(":open", AttributeValue::S("OPEN".into()))
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .expression_attribute_values(":remove", AttributeValue::Ss(vec!["remove".into()]))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
        .build()
        .unwrap()
}

fn followup_update_request(
    table: &str,
) -> aws_sdk_dynamodb::operation::update_item::UpdateItemInput {
    aws_sdk_dynamodb::operation::update_item::UpdateItemInput::builder()
        .table_name(table)
        .set_key(Some(key("1")))
        .update_expression(
            "SET #history = list_append(if_not_exists(#history, :empty), :event), #count = #count - :one REMOVE #copied ADD #flags :added",
        )
        .condition_expression("size(#flags) = :size AND attribute_type(#profile, :map)")
        .expression_attribute_names("#history", "history")
        .expression_attribute_names("#count", "count")
        .expression_attribute_names("#copied", "copiedStatus")
        .expression_attribute_names("#flags", "flags")
        .expression_attribute_names("#profile", "profile")
        .expression_attribute_values(":empty", AttributeValue::L(Vec::new()))
        .expression_attribute_values(
            ":event",
            AttributeValue::L(vec![AttributeValue::S("audit".into())]),
        )
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .expression_attribute_values(":added", AttributeValue::Ss(vec!["added".into()]))
        .expression_attribute_values(":size", AttributeValue::N("1".into()))
        .expression_attribute_values(":map", AttributeValue::S("M".into()))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
        .build()
        .unwrap()
}

type FilterCase = (
    &'static str,
    Vec<(&'static str, &'static str)>,
    Vec<(&'static str, AttributeValue)>,
    &'static [&'static str],
);

fn filter_cases() -> Vec<FilterCase> {
    vec![
        (
            "begins_with(#status, :prefix) AND contains(#flags, :flag)",
            vec![("#status", "status"), ("#flags", "flags")],
            vec![
                (":prefix", AttributeValue::S("OP".into())),
                (":flag", AttributeValue::S("keep".into())),
            ],
            &["1", "2"],
        ),
        (
            "attribute_exists(#profile.#name) AND attribute_not_exists(#missing) AND attribute_type(#count, :kind)",
            vec![
                ("#profile", "profile"),
                ("#name", "name"),
                ("#missing", "missing"),
                ("#count", "count"),
            ],
            vec![(":kind", AttributeValue::S("N".into()))],
            &["1", "2", "10"],
        ),
        (
            "size(#flags) = :two",
            vec![("#flags", "flags")],
            vec![(":two", AttributeValue::N("2".into()))],
            &["2", "10"],
        ),
        (
            "contains(#profile.#name, :fragment)",
            vec![("#profile", "profile"), ("#name", "name")],
            vec![(":fragment", AttributeValue::S("aft".into()))],
            &["1"],
        ),
        (
            "#status IN (:open, :match) AND #count BETWEEN :one AND :two",
            vec![("#status", "status"), ("#count", "count")],
            vec![
                (":open", AttributeValue::S("OPEN".into())),
                (":match", AttributeValue::S("MATCH".into())),
                (":one", AttributeValue::N("1".into())),
                (":two", AttributeValue::N("2".into())),
            ],
            &["1", "2", "10"],
        ),
        (
            "NOT (#status = :open) OR (#sequence >= :ten AND #sequence <> :eleven)",
            vec![("#status", "status"), ("#sequence", "sequence")],
            vec![
                (":open", AttributeValue::S("OPEN".into())),
                (":ten", AttributeValue::N("10".into())),
                (":eleven", AttributeValue::N("11".into())),
            ],
            &["10"],
        ),
    ]
}

#[allow(clippy::too_many_arguments)]
async fn assert_filter_matches(
    native: &aws_sdk_dynamodb::Client,
    native_table: &str,
    logical: &Client,
    logical_table: &str,
    expression: &str,
    names: Vec<(&str, &str)>,
    values: Vec<(&str, AttributeValue)>,
    expected: &[&str],
) {
    let mut names = names
        .into_iter()
        .map(|(alias, name)| (alias.to_owned(), name.to_owned()))
        .collect::<HashMap<_, _>>();
    let mut values = values
        .into_iter()
        .map(|(alias, value)| (alias.to_owned(), value))
        .collect::<HashMap<_, _>>();
    names.insert("#differential_account".into(), "account".into());
    values.insert(
        ":differential_account".into(),
        AttributeValue::S("acct-1".into()),
    );
    let scoped_expression =
        format!("#differential_account = :differential_account AND ({expression})");
    let native_page = native
        .scan()
        .table_name(native_table)
        .filter_expression(&scoped_expression)
        .set_expression_attribute_names(Some(names.clone()))
        .set_expression_attribute_values(Some(values.clone()))
        .send()
        .await
        .unwrap();
    let logical_page = logical
        .scan()
        .table_name(logical_table)
        .filter_expression(scoped_expression)
        .set_expression_attribute_names(Some(names))
        .set_expression_attribute_values(Some(values))
        .send()
        .await
        .unwrap();
    let native_sequences = sorted_sequences(native_page.items());
    let logical_sequences = sorted_sequences(logical_page.items());
    assert_eq!(logical_sequences, native_sequences, "{expression}");
    assert_eq!(
        logical_sequences,
        expected.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "{expression}"
    );
    assert_eq!(logical_page.count, native_page.count, "{expression}");
    assert_eq!(
        logical_page.scanned_count, native_page.scanned_count,
        "{expression}"
    );
}

fn sorted_sequences(items: &[HashMap<String, AttributeValue>]) -> Vec<String> {
    let mut sequences = items
        .iter()
        .map(|item| item["sequence"].as_n().unwrap().to_owned())
        .collect::<Vec<_>>();
    sequences.sort_by_key(|value| value.parse::<i64>().unwrap());
    sequences
}

async fn query_page(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    start: Option<HashMap<String, AttributeValue>>,
    forward: bool,
    limit: i32,
    filter: bool,
) -> aws_sdk_dynamodb::operation::query::QueryOutput {
    let mut request = client
        .query()
        .table_name(table)
        .key_condition_expression("#account = :account")
        .expression_attribute_names("#account", "account")
        .expression_attribute_values(":account", AttributeValue::S("acct-1".into()))
        .set_exclusive_start_key(start)
        .limit(limit)
        .scan_index_forward(forward);
    if filter {
        request = request
            .filter_expression("#status = :match")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":match", AttributeValue::S("MATCH".into()));
    }
    request.send().await.unwrap()
}

async fn collect_query(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    forward: bool,
) -> Vec<String> {
    let mut start = None;
    let mut sequences = Vec::new();
    loop {
        let page = query_page(client, table, start, forward, 1, false).await;
        sequences.extend(
            page.items()
                .iter()
                .map(|item| item["sequence"].as_n().unwrap().to_owned()),
        );
        start = page.last_evaluated_key;
        if start.is_none() {
            return sequences;
        }
    }
}

async fn collect_logical_query(client: &Client, table: &str, forward: bool) -> Vec<String> {
    let mut start = None;
    let mut sequences = Vec::new();
    loop {
        let page = client
            .query()
            .table_name(table)
            .key_condition_expression("#account = :account")
            .expression_attribute_names("#account", "account")
            .expression_attribute_values(":account", AttributeValue::S("acct-1".into()))
            .set_exclusive_start_key(start)
            .limit(1)
            .scan_index_forward(forward)
            .send()
            .await
            .unwrap();
        sequences.extend(
            page.items()
                .iter()
                .map(|item| item["sequence"].as_n().unwrap().to_owned()),
        );
        start = page.last_evaluated_key;
        if start.is_none() {
            return sequences;
        }
    }
}

async fn collect_native_scan(client: &aws_sdk_dynamodb::Client, table: &str) -> Vec<String> {
    let mut start = None;
    let mut sequences = Vec::new();
    loop {
        let page = client
            .scan()
            .table_name(table)
            .set_exclusive_start_key(start)
            .limit(1)
            .send()
            .await
            .unwrap();
        sequences.extend(
            page.items()
                .iter()
                .map(|item| item["sequence"].as_n().unwrap().to_owned()),
        );
        start = page.last_evaluated_key;
        if start.is_none() {
            sequences.sort();
            return sequences;
        }
    }
}

async fn collect_logical_scan(client: &Client, table: &str) -> Vec<String> {
    let mut start = None;
    let mut sequences = Vec::new();
    loop {
        let page = client
            .scan()
            .table_name(table)
            .set_exclusive_start_key(start)
            .limit(1)
            .send()
            .await
            .unwrap();
        sequences.extend(
            page.items()
                .iter()
                .map(|item| item["sequence"].as_n().unwrap().to_owned()),
        );
        start = page.last_evaluated_key;
        if start.is_none() {
            sequences.sort();
            return sequences;
        }
    }
}
