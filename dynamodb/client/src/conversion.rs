use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use prolly_dynamodb_core::{AttributeValue, DynamoNumber, Item};

use crate::{Error, Result};

pub(crate) fn table_to_aws(
    table: &prolly_dynamodb_core::TableDescription,
) -> aws_sdk_dynamodb::types::TableDescription {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, GlobalSecondaryIndexDescription, IndexStatus, KeySchemaElement,
        KeyType, LocalSecondaryIndexDescription, Projection, ProjectionType, ScalarAttributeType,
        TableStatus,
    };
    let scalar = |kind| match kind {
        prolly_dynamodb_core::KeyKind::String => ScalarAttributeType::S,
        prolly_dynamodb_core::KeyKind::Number => ScalarAttributeType::N,
        prolly_dynamodb_core::KeyKind::Binary => ScalarAttributeType::B,
    };
    let mut builder = aws_sdk_dynamodb::types::TableDescription::builder()
        .table_name(&table.name)
        .table_status(TableStatus::Active)
        .table_id(hex(&table.id.0))
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(&table.partition_key.name)
                .key_type(KeyType::Hash)
                .build()
                .expect("validated core partition key builds an AWS key schema"),
        );
    for (name, kind) in &table.attribute_definitions {
        builder = builder.attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(name)
                .attribute_type(scalar(*kind))
                .build()
                .expect("validated core key attribute builds an AWS definition"),
        );
    }
    if let Some(sort) = &table.sort_key {
        builder = builder.key_schema(
            KeySchemaElement::builder()
                .attribute_name(&sort.name)
                .key_type(KeyType::Range)
                .build()
                .expect("validated core sort key builds an AWS key schema"),
        );
    }
    let projection = |value: &prolly_dynamodb_core::SecondaryIndexProjection| {
        let mut builder = Projection::builder();
        match value {
            prolly_dynamodb_core::SecondaryIndexProjection::KeysOnly => {
                builder = builder.projection_type(ProjectionType::KeysOnly);
            }
            prolly_dynamodb_core::SecondaryIndexProjection::Include(names) => {
                builder = builder
                    .projection_type(ProjectionType::Include)
                    .set_non_key_attributes(Some(names.iter().cloned().collect()));
            }
            prolly_dynamodb_core::SecondaryIndexProjection::All => {
                builder = builder.projection_type(ProjectionType::All);
            }
        }
        builder.build()
    };
    let key_schema = |index: &prolly_dynamodb_core::SecondaryIndexDescription| {
        let mut keys = vec![KeySchemaElement::builder()
            .attribute_name(&index.partition_key.name)
            .key_type(KeyType::Hash)
            .build()
            .expect("validated index partition key builds an AWS key schema")];
        if let Some(sort) = &index.sort_key {
            keys.push(
                KeySchemaElement::builder()
                    .attribute_name(&sort.name)
                    .key_type(KeyType::Range)
                    .build()
                    .expect("validated index sort key builds an AWS key schema"),
            );
        }
        keys
    };
    for index in &table.secondary_indexes {
        match index.kind {
            prolly_dynamodb_core::SecondaryIndexKind::Local => {
                builder = builder.local_secondary_indexes(
                    LocalSecondaryIndexDescription::builder()
                        .index_name(&index.name)
                        .set_key_schema(Some(key_schema(index)))
                        .projection(projection(&index.projection))
                        .build(),
                );
            }
            prolly_dynamodb_core::SecondaryIndexKind::Global => {
                let status = match index.status {
                    prolly_dynamodb_core::SecondaryIndexStatus::Building => IndexStatus::Creating,
                    prolly_dynamodb_core::SecondaryIndexStatus::Active => IndexStatus::Active,
                    prolly_dynamodb_core::SecondaryIndexStatus::Retiring => IndexStatus::Deleting,
                };
                builder = builder.global_secondary_indexes(
                    GlobalSecondaryIndexDescription::builder()
                        .index_name(&index.name)
                        .set_key_schema(Some(key_schema(index)))
                        .projection(projection(&index.projection))
                        .index_status(status)
                        .build(),
                );
            }
        }
    }
    builder.build()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

pub(crate) fn item_from_aws(item: HashMap<String, AwsAttributeValue>) -> Result<Item> {
    item.into_iter()
        .map(|(name, value)| Ok((name, attribute_from_aws(value)?)))
        .collect()
}

pub(crate) fn item_to_aws(item: Item) -> HashMap<String, AwsAttributeValue> {
    item.into_iter()
        .map(|(name, value)| (name, attribute_to_aws(value)))
        .collect()
}

fn attribute_from_aws(value: AwsAttributeValue) -> Result<AttributeValue> {
    Ok(match value {
        AwsAttributeValue::B(value) => AttributeValue::B(value.into_inner()),
        AwsAttributeValue::Bool(value) => AttributeValue::Bool(value),
        AwsAttributeValue::Bs(values) => {
            AttributeValue::Bs(values.into_iter().map(|value| value.into_inner()).collect())
        }
        AwsAttributeValue::L(values) => AttributeValue::L(
            values
                .into_iter()
                .map(attribute_from_aws)
                .collect::<Result<Vec<_>>>()?,
        ),
        AwsAttributeValue::M(values) => AttributeValue::M(
            values
                .into_iter()
                .map(|(name, value)| Ok((name, attribute_from_aws(value)?)))
                .collect::<Result<BTreeMap<_, _>>>()?,
        ),
        AwsAttributeValue::N(value) => AttributeValue::N(DynamoNumber::parse(&value)?),
        AwsAttributeValue::Ns(values) => AttributeValue::Ns(
            values
                .into_iter()
                .map(|value| DynamoNumber::parse(&value).map_err(Error::from))
                .collect::<Result<Vec<_>>>()?,
        ),
        AwsAttributeValue::Null(value) => AttributeValue::Null(value),
        AwsAttributeValue::S(value) => AttributeValue::S(value),
        AwsAttributeValue::Ss(values) => AttributeValue::Ss(values),
        _ => {
            return Err(Error::Unsupported(
                "unknown AWS AttributeValue union member".into(),
            ))
        }
    })
}

pub(crate) fn condition_from_aws(
    expression: &str,
    names: &HashMap<String, String>,
    values: &HashMap<String, AwsAttributeValue>,
) -> Result<prolly_dynamodb_core::Condition> {
    let names = names
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let values = values
        .iter()
        .map(|(name, value)| Ok((name.clone(), attribute_from_aws(value.clone())?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(prolly_dynamodb_core::parse_condition(
        expression, &names, &values,
    )?)
}

pub(crate) fn update_from_aws(
    update_expression: &str,
    condition_expression: Option<&str>,
    names: &HashMap<String, String>,
    values: &HashMap<String, AwsAttributeValue>,
) -> Result<prolly_dynamodb_core::ParsedUpdate> {
    let names = names
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let values = values
        .iter()
        .map(|(name, value)| Ok((name.clone(), attribute_from_aws(value.clone())?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(prolly_dynamodb_core::parse_update(
        update_expression,
        condition_expression,
        &names,
        &values,
    )?)
}

pub(crate) fn projection_from_aws(
    expression: &str,
    names: &HashMap<String, String>,
) -> Result<prolly_dynamodb_core::Projection> {
    let names = names
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    Ok(prolly_dynamodb_core::parse_projection(expression, &names)?)
}

pub(crate) fn read_expressions_from_aws(
    key_condition_expression: Option<&str>,
    filter_expression: Option<&str>,
    projection_expression: Option<&str>,
    names: &HashMap<String, String>,
    values: &HashMap<String, AwsAttributeValue>,
) -> Result<prolly_dynamodb_core::ParsedReadExpressions> {
    let names = names
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let values = values
        .iter()
        .map(|(name, value)| Ok((name.clone(), attribute_from_aws(value.clone())?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(prolly_dynamodb_core::parse_read_expressions(
        key_condition_expression,
        filter_expression,
        projection_expression,
        &names,
        &values,
    )?)
}

fn attribute_to_aws(value: AttributeValue) -> AwsAttributeValue {
    match value {
        AttributeValue::B(value) => AwsAttributeValue::B(value.into()),
        AttributeValue::Bool(value) => AwsAttributeValue::Bool(value),
        AttributeValue::Bs(values) => {
            AwsAttributeValue::Bs(values.into_iter().map(Into::into).collect())
        }
        AttributeValue::L(values) => {
            AwsAttributeValue::L(values.into_iter().map(attribute_to_aws).collect())
        }
        AttributeValue::M(values) => AwsAttributeValue::M(
            values
                .into_iter()
                .map(|(name, value)| (name, attribute_to_aws(value)))
                .collect(),
        ),
        AttributeValue::N(value) => AwsAttributeValue::N(value.to_string()),
        AttributeValue::Ns(values) => {
            AwsAttributeValue::Ns(values.into_iter().map(|value| value.to_string()).collect())
        }
        AttributeValue::Null(value) => AwsAttributeValue::Null(value),
        AttributeValue::S(value) => AwsAttributeValue::S(value),
        AttributeValue::Ss(values) => AwsAttributeValue::Ss(values),
    }
}

pub(crate) fn conditional_error_from_core(
    error: prolly_dynamodb_core::Error,
    return_old: bool,
) -> Error {
    match error {
        prolly_dynamodb_core::Error::ConditionalCheckFailed { old_item } => {
            Error::ConditionalCheckFailed {
                item: if return_old {
                    old_item.map(item_to_aws)
                } else {
                    None
                },
            }
        }
        prolly_dynamodb_core::Error::ExpectedHeadMismatch {
            expected, current, ..
        } => Error::HeadConflict {
            expected,
            current: Some(current),
        },
        error => Error::Core(error),
    }
}

pub(crate) fn return_failure_old(
    value: Option<&aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure>,
) -> Result<bool> {
    match value
        .map(aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure::as_str)
        .unwrap_or("NONE")
    {
        "NONE" => Ok(false),
        "ALL_OLD" => Ok(true),
        value => Err(Error::Unsupported(format!(
            "unsupported ReturnValuesOnConditionCheckFailure value {value:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct CanonicalCase {
        name: String,
        item: Option<Item>,
        encoded_hex: String,
    }

    #[derive(Deserialize)]
    struct CanonicalFixtures {
        fixture_version: u32,
        item_codec: String,
        items: Vec<CanonicalCase>,
    }

    #[derive(Deserialize)]
    struct ValidationFixtures {
        fixture_version: u32,
        cases: Vec<ValidationCase>,
    }

    #[derive(Deserialize)]
    struct ValidationCase {
        name: String,
        kind: String,
        expression: String,
        #[serde(default)]
        condition: Option<String>,
        names: BTreeMap<String, String>,
        values: BTreeMap<String, AttributeValue>,
        category: String,
        message: String,
    }

    #[test]
    fn aws_conversion_normalizes_numbers_recursively() {
        let aws = HashMap::from([(
            "document".to_string(),
            AwsAttributeValue::M(HashMap::from([(
                "numbers".to_string(),
                AwsAttributeValue::Ns(vec!["1.20".into(), "3e0".into()]),
            )])),
        )]);
        let core = item_from_aws(aws).unwrap();
        let round_trip = item_to_aws(core);
        let numbers = round_trip["document"].as_m().unwrap()["numbers"]
            .as_ns()
            .unwrap();
        assert_eq!(numbers, &vec!["1.2".to_string(), "3".to_string()]);
    }

    #[test]
    fn conditional_failure_mapping_returns_old_item_only_when_requested() {
        let core_error = prolly_dynamodb_core::Error::ConditionalCheckFailed {
            old_item: Some(Item::from([(
                "state".into(),
                AttributeValue::S("OPEN".into()),
            )])),
        };
        let error = conditional_error_from_core(core_error, true);
        assert_eq!(
            error.conditional_failure_item().unwrap()["state"],
            AwsAttributeValue::S("OPEN".into())
        );
        let error = conditional_error_from_core(
            prolly_dynamodb_core::Error::ConditionalCheckFailed {
                old_item: Some(Item::new()),
            },
            false,
        );
        assert!(error.conditional_failure_item().is_none());
    }

    #[test]
    fn aws_facade_consumes_the_frozen_core_item_fixtures() {
        let fixtures: CanonicalFixtures =
            serde_json::from_str(include_str!("fixtures/canonical-v1.json")).unwrap();
        assert_eq!(fixtures.fixture_version, 1);
        assert_eq!(fixtures.item_codec, "DDBI-v1-canonical-cbor");

        for case in fixtures.items {
            let expected = case.item.expect("item fixture must contain an item");
            let converted = item_from_aws(item_to_aws(expected.clone())).unwrap();
            assert_eq!(converted, expected, "fixture {}", case.name);
            assert_eq!(
                hex(&prolly_dynamodb_core::encode_item(&converted).unwrap()),
                case.encoded_hex,
                "fixture {}",
                case.name
            );
        }
    }

    #[test]
    fn aws_facade_preserves_frozen_validation_precedence() {
        let fixtures: ValidationFixtures =
            serde_json::from_str(include_str!("fixtures/validation-v1.json")).unwrap();
        assert_eq!(fixtures.fixture_version, 1);

        for case in fixtures.cases {
            let names = case.names.into_iter().collect::<HashMap<_, _>>();
            let values = item_to_aws(case.values);
            let result = match case.kind.as_str() {
                "projection" => projection_from_aws(&case.expression, &names).map(|_| ()),
                "condition" => condition_from_aws(&case.expression, &names, &values).map(|_| ()),
                "key_condition" => {
                    read_expressions_from_aws(Some(&case.expression), None, None, &names, &values)
                        .map(|_| ())
                }
                "read_none" => {
                    read_expressions_from_aws(None, None, None, &names, &values).map(|_| ())
                }
                "update" => {
                    update_from_aws(&case.expression, case.condition.as_deref(), &names, &values)
                        .map(|_| ())
                }
                other => panic!("unknown validation fixture kind {other:?}"),
            };
            let error = result.expect_err(&case.name);
            let Error::Core(core) = error else {
                panic!("unexpected facade error for {}: {error:?}", case.name)
            };
            let (category, message) = match core {
                prolly_dynamodb_core::Error::Validation(message) => ("validation", message),
                prolly_dynamodb_core::Error::Unsupported(message) => ("unsupported", message),
                other => panic!("unexpected core error for {}: {other:?}", case.name),
            };
            assert_eq!(category, case.category, "fixture {}", case.name);
            assert_eq!(message, case.message, "fixture {}", case.name);
        }
    }
}
