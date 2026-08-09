use std::collections::BTreeMap;

use prolly_dynamodb_core::{
    encode_item, encode_primary_key, AttributeValue, DynamoNumber, Item, KeyAttribute, KeyKind,
    TableDescription, TableId, TableStatus,
};
use serde::Serialize;

#[derive(Serialize)]
struct ItemCase {
    name: &'static str,
    item: Item,
    encoded_hex: String,
}

#[derive(Serialize)]
struct KeyCase {
    name: &'static str,
    key: Item,
    encoded_hex: String,
}

#[derive(Serialize)]
struct Fixtures {
    fixture_version: u32,
    item_codec: &'static str,
    items: Vec<ItemCase>,
    keys: Vec<KeyCase>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let items = [
        (
            "scalar",
            Item::from([
                ("active".into(), AttributeValue::Bool(true)),
                ("id".into(), AttributeValue::S("case-1".into())),
                (
                    "ratio".into(),
                    AttributeValue::N(DynamoNumber::parse("01.200e0")?),
                ),
            ]),
        ),
        (
            "sets_and_document",
            Item::from([
                (
                    "document".into(),
                    AttributeValue::M(BTreeMap::from([(
                        "tags".into(),
                        AttributeValue::L(vec![
                            AttributeValue::S("legal".into()),
                            AttributeValue::Null(true),
                        ]),
                    )])),
                ),
                (
                    "numbers".into(),
                    AttributeValue::Ns(vec![
                        DynamoNumber::parse("10")?,
                        DynamoNumber::parse("-2.50")?,
                    ]),
                ),
                (
                    "strings".into(),
                    AttributeValue::Ss(vec!["z".into(), "a".into()]),
                ),
            ]),
        ),
    ]
    .into_iter()
    .map(|(name, item)| {
        Ok(ItemCase {
            name,
            encoded_hex: hex(&encode_item(&item)?),
            item,
        })
    })
    .collect::<prolly_dynamodb_core::Result<Vec<_>>>()?;

    let schema = TableDescription {
        name: "Orders".into(),
        id: TableId([7; 32]),
        partition_key: KeyAttribute {
            name: "account".into(),
            kind: KeyKind::String,
        },
        sort_key: Some(KeyAttribute {
            name: "sequence".into(),
            kind: KeyKind::Number,
        }),
        attribute_definitions: std::collections::BTreeMap::from([
            ("account".into(), KeyKind::String),
            ("sequence".into(), KeyKind::Number),
        ]),
        secondary_indexes: Vec::new(),
        status: TableStatus::Active,
        created_at_millis: 0,
    };
    let keys = ["-2.50", "0", "01.200e0", "10"]
        .into_iter()
        .map(|number| {
            let key = Item::from([
                ("account".into(), AttributeValue::S("acct-1".into())),
                (
                    "sequence".into(),
                    AttributeValue::N(DynamoNumber::parse(number)?),
                ),
            ]);
            Ok(KeyCase {
                name: number,
                encoded_hex: hex(&encode_primary_key(&schema, &key)?),
                key,
            })
        })
        .collect::<prolly_dynamodb_core::Result<Vec<_>>>()?;

    println!(
        "{}",
        serde_json::to_string_pretty(&Fixtures {
            fixture_version: 1,
            item_codec: "DDBI-v1-canonical-cbor",
            items,
            keys,
        })?
    );
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}
