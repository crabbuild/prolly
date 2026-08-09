//! Stable cross-binary qualification probe for rolling client releases.
//!
//! This example is packaged with the crate so an independently archived
//! release can be built and exercised against a candidate binary. It is an
//! operator/CI probe, not an application API.

use std::collections::{BTreeSet, HashMap};
use std::error::Error as StdError;
use std::time::Duration;

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use prolly::MapVersionId;
use prolly_dynamodb_client::{Client, Error};
use prolly_dynamodb_core::TransactionCancellationCode;
use prolly_store_dynamodb::DynamoDbBackend;
use serde_json::{json, Value};

const PROTOCOL: &str = "prolly-dynamodb-rolling-probe-v1";
const LOGICAL_TABLE: &str = "RollingCompatibility";
const MAX_WRITES_PER_INVOCATION: usize = 10_000;
const MAX_RETRIES: usize = 256;

type AnyError = Box<dyn StdError + Send + Sync>;

fn main() -> Result<(), AnyError> {
    std::thread::Builder::new()
        .name("rolling-compatibility-probe".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?
                .block_on(run())
        })?
        .join()
        .map_err(|_| "rolling compatibility probe thread panicked")?
}

async fn run() -> Result<(), AnyError> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next().ok_or("missing probe command")?;
    let output = match command.as_str() {
        "identity" => {
            require_end(&mut arguments)?;
            identity().await?
        }
        "init" => {
            require_end(&mut arguments)?;
            initialize().await?
        }
        "write" => {
            let writer = parse_writer(arguments.next())?;
            let start = parse_usize("start", arguments.next(), 0, usize::MAX)?;
            let count = parse_usize("count", arguments.next(), 1, MAX_WRITES_PER_INVOCATION)?;
            require_end(&mut arguments)?;
            write_range(&writer, start, count).await?
        }
        "verify" => {
            let old_count =
                parse_usize("old_count", arguments.next(), 1, MAX_WRITES_PER_INVOCATION)?;
            let new_count =
                parse_usize("new_count", arguments.next(), 1, MAX_WRITES_PER_INVOCATION)?;
            require_end(&mut arguments)?;
            verify(old_count, new_count).await?
        }
        "verify-at" => {
            let version = parse_version(arguments.next())?;
            let id = arguments.next().ok_or("missing item id")?;
            let writer = parse_writer(arguments.next())?;
            let counter = parse_usize("counter", arguments.next(), 0, usize::MAX)?;
            require_end(&mut arguments)?;
            verify_at(version, &id, &writer, counter).await?
        }
        "cleanup" => {
            require_end(&mut arguments)?;
            cleanup().await?
        }
        _ => return Err(format!("unknown probe command {command:?}").into()),
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

fn envelope(command: &str) -> serde_json::Map<String, Value> {
    serde_json::Map::from_iter([
        ("protocol".into(), Value::String(PROTOCOL.into())),
        ("command".into(), Value::String(command.into())),
        (
            "package_version".into(),
            Value::String(env!("CARGO_PKG_VERSION").into()),
        ),
    ])
}

async fn identity() -> Result<Value, AnyError> {
    let (_, client) = open(false).await?;
    let mut output = envelope("identity");
    output.insert(
        "capabilities".into(),
        serde_json::from_str(&client.capabilities().to_json()?)?,
    );
    Ok(Value::Object(output))
}

async fn initialize() -> Result<Value, AnyError> {
    let (_, client) = open(true).await?;
    client
        .create_table()
        .table_name(LOGICAL_TABLE)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()?,
        )
        .request_token("rolling-compatibility-create-v1")
        .send()
        .await?;
    let head = client.table(LOGICAL_TABLE).head().await?;
    let mut output = envelope("init");
    output.insert("head".into(), Value::String(head.id.to_string()));
    Ok(Value::Object(output))
}

async fn write_range(writer: &str, start: usize, count: usize) -> Result<Value, AnyError> {
    let (_, client) = open(false).await?;
    let end = start.checked_add(count).ok_or("write range overflow")?;
    let mut first = None;
    let mut last = None;

    for counter in start..end {
        let id = item_id(writer, counter);
        let token = format!("rolling-v1-{writer}-{counter:010}");
        let version = execute_put(&client, &id, writer, counter, &token).await?;
        let item = json!({ "id": id, "version": version.to_string() });
        first.get_or_insert_with(|| item.clone());
        last = Some(item);
    }

    let mut output = envelope("write");
    output.insert("writer".into(), Value::String(writer.into()));
    output.insert("start".into(), json!(start));
    output.insert("count".into(), json!(count));
    output.insert("first".into(), first.expect("count is nonzero"));
    output.insert("last".into(), last.expect("count is nonzero"));
    Ok(Value::Object(output))
}

async fn execute_put(
    client: &Client,
    id: &str,
    writer: &str,
    counter: usize,
    token: &str,
) -> Result<MapVersionId, AnyError> {
    for attempt in 0..MAX_RETRIES {
        match client
            .put_item()
            .table_name(LOGICAL_TABLE)
            .item("id", AttributeValue::S(id.into()))
            .item("writer", AttributeValue::S(writer.into()))
            .item("counter", AttributeValue::N(counter.to_string()))
            .request_token(token)
            .send_with_metadata()
            .await
        {
            Ok(result) => {
                if result.commit_id.is_none() || result.transitions.len() != 1 {
                    return Err("write returned incomplete commit metadata".into());
                }
                return result
                    .version_id
                    .ok_or_else(|| "write returned no table version".into());
            }
            Err(error) if retryable_conflict(&error) => {
                let jitter = u64::try_from((counter + attempt) % 11)?;
                tokio::time::sleep(Duration::from_millis(1 + jitter)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(format!("write {id:?} exhausted {MAX_RETRIES} conflict retries").into())
}

async fn verify(old_count: usize, new_count: usize) -> Result<Value, AnyError> {
    let expected_count = old_count
        .checked_add(new_count)
        .ok_or("expected item count overflow")?;
    if expected_count + 1 > prolly_dynamodb_core::MAX_COLLECTED_VERSIONS {
        return Err(format!(
            "verification requires at most {} writes",
            prolly_dynamodb_core::MAX_COLLECTED_VERSIONS - 1
        )
        .into());
    }

    let (_, client) = open(false).await?;
    let current_items = collect_scan(&client, None).await?;
    validate_items(&current_items, old_count, new_count)?;

    let table = client.table(LOGICAL_TABLE);
    let versions = table.collect_versions().await?;
    if versions.len() != expected_count + 1 {
        return Err(format!(
            "expected {} versions including table creation, found {}",
            expected_count + 1,
            versions.len()
        )
        .into());
    }

    let mut after = None;
    let mut commits = Vec::new();
    loop {
        let page = table.commits(after, 1_000).await?;
        commits.extend(page.commits);
        if commits.len() > expected_count + 1 {
            return Err("commit log exceeds expected cardinality".into());
        }
        match page.last_sequence {
            Some(sequence) => after = Some(sequence),
            None => break,
        }
    }
    if commits.len() != expected_count + 1 {
        return Err(format!(
            "expected {} commits including table creation, found {}",
            expected_count + 1,
            commits.len()
        )
        .into());
    }
    let sequences = commits
        .iter()
        .map(|commit| commit.sequence)
        .collect::<Vec<_>>();
    let expected_sequences = (1..=u64::try_from(expected_count + 1)?).collect::<Vec<_>>();
    if sequences != expected_sequences {
        return Err("commit sequences are not contiguous from one".into());
    }
    let unique_commits = commits
        .iter()
        .map(|commit| commit.commit_id.clone())
        .collect::<BTreeSet<_>>();
    if unique_commits.len() != commits.len() {
        return Err("commit IDs are not unique".into());
    }

    let head = table.head().await?;
    let pinned_items = collect_scan(&client, Some(head.id.clone())).await?;
    validate_items(&pinned_items, old_count, new_count)?;

    let mut output = envelope("verify");
    output.insert("items".into(), json!(current_items.len()));
    output.insert("versions".into(), json!(versions.len()));
    output.insert("commits".into(), json!(commits.len()));
    output.insert("head".into(), Value::String(head.id.to_string()));
    Ok(Value::Object(output))
}

async fn verify_at(
    version: MapVersionId,
    id: &str,
    writer: &str,
    counter: usize,
) -> Result<Value, AnyError> {
    if id != item_id(writer, counter) {
        return Err("item id does not match writer/counter".into());
    }
    let (_, client) = open(false).await?;
    let result = client
        .table(LOGICAL_TABLE)
        .at(version.clone())
        .get_item()
        .key("id", AttributeValue::S(id.into()))
        .send()
        .await?;
    let item = result
        .item
        .ok_or("immutable version did not contain item")?;
    validate_item(&item, id, writer, counter)?;

    let mut output = envelope("verify-at");
    output.insert("id".into(), Value::String(id.into()));
    output.insert("version".into(), Value::String(version.to_string()));
    Ok(Value::Object(output))
}

async fn cleanup() -> Result<Value, AnyError> {
    let (backend, _) = open(false).await?;
    backend.clear_namespace().await?;
    Ok(Value::Object(envelope("cleanup")))
}

async fn collect_scan(
    client: &Client,
    version: Option<MapVersionId>,
) -> Result<Vec<HashMap<String, AttributeValue>>, AnyError> {
    let mut cursor = None;
    let mut items = Vec::new();
    loop {
        let request = match &version {
            Some(version) => client.table(LOGICAL_TABLE).at(version.clone()).scan(),
            None => client.scan().table_name(LOGICAL_TABLE),
        }
        .limit(1_000)
        .set_exclusive_start_key(cursor.take());
        let page = request.send().await?;
        items.extend(page.items.unwrap_or_default());
        cursor = page.last_evaluated_key;
        if cursor.is_none() {
            return Ok(items);
        }
    }
}

fn validate_items(
    items: &[HashMap<String, AttributeValue>],
    old_count: usize,
    new_count: usize,
) -> Result<(), AnyError> {
    let expected = [("old", old_count), ("new", new_count)]
        .into_iter()
        .flat_map(|(writer, count)| (0..count).map(move |counter| item_id(writer, counter)))
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for item in items {
        let id = string_attribute(item, "id")?;
        let writer = string_attribute(item, "writer")?;
        let counter = number_attribute(item, "counter")?;
        validate_item(item, id, writer, counter)?;
        if !actual.insert(id.to_owned()) {
            return Err(format!("duplicate item {id:?}").into());
        }
    }
    if actual != expected {
        return Err("current item identity set differs from the exact expected set".into());
    }
    Ok(())
}

fn validate_item(
    item: &HashMap<String, AttributeValue>,
    id: &str,
    writer: &str,
    counter: usize,
) -> Result<(), AnyError> {
    if item.len() != 3
        || string_attribute(item, "id")? != id
        || string_attribute(item, "writer")? != writer
        || number_attribute(item, "counter")? != counter
        || id != item_id(writer, counter)
    {
        return Err(format!("item {id:?} has invalid canonical contents").into());
    }
    Ok(())
}

fn string_attribute<'a>(
    item: &'a HashMap<String, AttributeValue>,
    name: &str,
) -> Result<&'a str, AnyError> {
    item.get(name)
        .and_then(|value| value.as_s().ok())
        .map(String::as_str)
        .ok_or_else(|| format!("attribute {name:?} is not a string").into())
}

fn number_attribute(item: &HashMap<String, AttributeValue>, name: &str) -> Result<usize, AnyError> {
    item.get(name)
        .and_then(|value| value.as_n().ok())
        .ok_or_else(|| format!("attribute {name:?} is not a number"))?
        .parse::<usize>()
        .map_err(Into::into)
}

fn retryable_conflict(error: &Error) -> bool {
    match error {
        Error::Core(prolly_dynamodb_core::Error::ConflictExhausted) => true,
        Error::Core(prolly_dynamodb_core::Error::TransactionCanceled { reasons }) => reasons
            .iter()
            .all(|reason| reason.code == Some(TransactionCancellationCode::TransactionConflict)),
        _ => false,
    }
}

async fn open(initialize_schema: bool) -> Result<(DynamoDbBackend, Client), AnyError> {
    let physical_table = required_env("PROLLY_DYNAMODB_COMPAT_TABLE")?;
    let root_table = required_env("PROLLY_DYNAMODB_COMPAT_ROOT_TABLE")?;
    let prefix = required_env("PROLLY_DYNAMODB_COMPAT_PREFIX")?.into_bytes();
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());

    let sdk = if let Ok(endpoint) = std::env::var("PROLLY_DYNAMODB_COMPAT_ENDPOINT") {
        let config = aws_sdk_dynamodb::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new("test", "test", None, None, "local"))
            .build();
        aws_sdk_dynamodb::Client::from_conf(config)
    } else {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        aws_sdk_dynamodb::Client::new(&config)
    };
    let backend = DynamoDbBackend::new(sdk, physical_table)
        .with_root_table_name(root_table)
        .with_key_prefix(prefix);
    if initialize_schema {
        backend.initialize_schema().await?;
    }
    let client = Client::open(backend.clone()).await?;
    Ok((backend, client))
}

fn required_env(name: &str) -> Result<String, AnyError> {
    let value = std::env::var(name).map_err(|_| format!("missing {name}"))?;
    if value.is_empty() {
        return Err(format!("{name} must not be empty").into());
    }
    Ok(value)
}

fn item_id(writer: &str, counter: usize) -> String {
    format!("{writer}-{counter:010}")
}

fn parse_writer(value: Option<String>) -> Result<String, AnyError> {
    let value = value.ok_or("missing writer")?;
    if value.is_empty()
        || value.len() > 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err("writer must be 1..=32 ASCII alphanumeric, '-' or '_' bytes".into());
    }
    Ok(value)
}

fn parse_usize(
    name: &str,
    value: Option<String>,
    minimum: usize,
    maximum: usize,
) -> Result<usize, AnyError> {
    let value = value.ok_or_else(|| format!("missing {name}"))?;
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{name} must be an integer"))?;
    if !(minimum..=maximum).contains(&parsed) {
        return Err(format!("{name} must be in {minimum}..={maximum}").into());
    }
    Ok(parsed)
}

fn parse_version(value: Option<String>) -> Result<MapVersionId, AnyError> {
    let value = value.ok_or("missing version")?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("version must contain exactly 64 lowercase hex characters".into());
    }
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair)?;
            Ok(u8::from_str_radix(pair, 16)?)
        })
        .collect::<Result<Vec<_>, AnyError>>()?;
    Ok(MapVersionId::from_bytes(&bytes)?)
}

fn require_end(arguments: &mut impl Iterator<Item = String>) -> Result<(), AnyError> {
    if let Some(argument) = arguments.next() {
        return Err(format!("unexpected argument {argument:?}").into());
    }
    Ok(())
}
