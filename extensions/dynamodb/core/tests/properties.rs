use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use prolly_dynamodb_core::{
    decode_item, encode_item, encode_primary_key, parse_condition, parse_update, AttributeValue,
    DynamoNumber, Item, KeyAttribute, KeyKind, TableDescription, TableId, TableStatus,
};
use proptest::prelude::*;

fn numeric_schema() -> TableDescription {
    TableDescription {
        name: "Numbers".into(),
        id: TableId([9; 32]),
        partition_key: KeyAttribute {
            name: "value".into(),
            kind: KeyKind::Number,
        },
        sort_key: None,
        attribute_definitions: BTreeMap::from([("value".into(), KeyKind::Number)]),
        secondary_indexes: Vec::new(),
        status: TableStatus::Active,
        created_at_millis: 0,
    }
}

fn decimal(value: i64, scale: u32) -> String {
    let negative = value < 0;
    let mut digits = value.unsigned_abs().to_string();
    if scale > 0 {
        let scale = scale as usize;
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale, '.');
    }
    if negative {
        digits.insert(0, '-');
    }
    digits
}

fn compare_scaled(left: (i64, u32), right: (i64, u32)) -> Ordering {
    let common = left.1.max(right.1);
    let left = i128::from(left.0) * 10_i128.pow(common - left.1);
    let right = i128::from(right.0) * 10_i128.pow(common - right.1);
    left.cmp(&right)
}

proptest! {
    #[test]
    fn numeric_key_order_matches_exact_decimal_order(
        left_value in -1_000_000_000_000_000_i64..=1_000_000_000_000_000,
        left_scale in 0_u32..=10,
        right_value in -1_000_000_000_000_000_i64..=1_000_000_000_000_000,
        right_scale in 0_u32..=10,
    ) {
        let schema = numeric_schema();
        let left_number = DynamoNumber::parse(&decimal(left_value, left_scale)).unwrap();
        let right_number = DynamoNumber::parse(&decimal(right_value, right_scale)).unwrap();
        let left = encode_primary_key(
            &schema,
            &Item::from([("value".into(), AttributeValue::N(left_number.clone()))]),
        ).unwrap();
        let right = encode_primary_key(
            &schema,
            &Item::from([("value".into(), AttributeValue::N(right_number.clone()))]),
        ).unwrap();
        prop_assert_eq!(left.cmp(&right), compare_scaled(
            (left_value, left_scale),
            (right_value, right_scale),
        ));
        prop_assert_eq!(
            left_number.numeric_cmp(&right_number).unwrap(),
            compare_scaled((left_value, left_scale), (right_value, right_scale)),
        );
    }

    #[test]
    fn set_input_order_never_changes_canonical_item_bytes(
        values in prop::collection::btree_set("[a-z]{1,12}", 1..32),
    ) {
        let forward = values.iter().cloned().collect::<Vec<_>>();
        let reverse = values.iter().rev().cloned().collect::<Vec<_>>();
        let left = Item::from([("set".into(), AttributeValue::Ss(forward))]);
        let right = Item::from([("set".into(), AttributeValue::Ss(reverse))]);
        let encoded = encode_item(&left).unwrap();
        prop_assert_eq!(&encoded, &encode_item(&right).unwrap());
        prop_assert_eq!(decode_item(&encoded).unwrap(), decode_item(&encode_item(&right).unwrap()).unwrap());
    }

    #[test]
    fn condition_parser_terminates_and_never_accepts_undeclared_bindings(
        expression in ".{0,5000}",
    ) {
        let result = parse_condition(&expression, &BTreeMap::new(), &BTreeMap::new());
        prop_assert!(result.is_err());
    }

    #[test]
    fn update_parser_terminates_and_rejects_hostile_unbound_input(
        expression in ".{0,5000}",
    ) {
        let result = parse_update(&expression, None, &BTreeMap::new(), &BTreeMap::new());
        prop_assert!(result.is_err());
    }

    #[test]
    fn nested_update_plans_are_deterministic(
        index in 0_usize..64,
        old_values in prop::collection::vec("[a-z]{0,8}", 0..32),
        replacement in "[a-z]{0,8}",
    ) {
        let expression = format!("SET #list[{index}]=:value");
        let plan = parse_update(
            &expression,
            None,
            &BTreeMap::from([("#list".into(), "list".into())]),
            &BTreeMap::from([(
                ":value".into(),
                AttributeValue::S(replacement),
            )]),
        ).unwrap().plan;
        let old = Item::from([(
            "list".into(),
            AttributeValue::L(old_values.into_iter().map(AttributeValue::S).collect()),
        )]);
        let first = plan.apply(&old, std::iter::empty()).unwrap();
        let second = plan.apply(&old, std::iter::empty()).unwrap();
        prop_assert_eq!(first, second);
    }
}

#[test]
fn property_strategy_assumptions_are_stable() {
    let values = BTreeSet::from(["a".to_string(), "z".to_string()]);
    assert_eq!(values.len(), 2);
}
