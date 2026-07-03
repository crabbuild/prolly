use prolly::{
    resolver, Config, Error, MemStore, MergePolicyRegistry, MergePolicyRuleLabel, Prolly,
    Resolution,
};

#[test]
fn registry_resolves_conflicts_by_key_prefix() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let base = prolly.create();
    let base = prolly
        .put(&base, b"settings/theme".to_vec(), b"light".to_vec())
        .unwrap();
    let base = prolly
        .put(&base, b"permissions/admin".to_vec(), b"allow".to_vec())
        .unwrap();

    let left = prolly.delete(&base, b"settings/theme").unwrap();
    let left = prolly.delete(&left, b"permissions/admin").unwrap();
    let right = prolly
        .put(&base, b"settings/theme".to_vec(), b"dark".to_vec())
        .unwrap();
    let right = prolly
        .put(&right, b"permissions/admin".to_vec(), b"deny".to_vec())
        .unwrap();

    let policies = MergePolicyRegistry::new()
        .add_prefix(b"settings/".to_vec(), resolver::update_wins)
        .add_prefix(b"permissions/".to_vec(), resolver::delete_wins);

    let merged = prolly
        .merge(&base, &left, &right, Some(policies.as_resolver()))
        .unwrap();

    assert_eq!(
        prolly.get(&merged, b"settings/theme").unwrap(),
        Some(b"dark".to_vec())
    );
    assert_eq!(prolly.get(&merged, b"permissions/admin").unwrap(), None);
}

#[test]
fn exact_rules_override_earlier_broad_prefix_rules() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let base = prolly.create();
    let base = prolly
        .put(&base, b"documents/doc1/title".to_vec(), b"base".to_vec())
        .unwrap();

    let left = prolly
        .put(&base, b"documents/doc1/title".to_vec(), b"left".to_vec())
        .unwrap();
    let right = prolly
        .put(&base, b"documents/doc1/title".to_vec(), b"right".to_vec())
        .unwrap();

    let policies = MergePolicyRegistry::new()
        .add_prefix(b"documents/".to_vec(), resolver::prefer_left)
        .add_exact(b"documents/doc1/title".to_vec(), |_| {
            Resolution::value(b"semantic-title".to_vec())
        });

    assert_eq!(
        policies
            .matching_rule(b"documents/doc1/title")
            .unwrap()
            .label(),
        MergePolicyRuleLabel::Exact(b"documents/doc1/title")
    );

    let merged = prolly
        .merge(&base, &left, &right, Some(policies.into_resolver()))
        .unwrap();

    assert_eq!(
        prolly.get(&merged, b"documents/doc1/title").unwrap(),
        Some(b"semantic-title".to_vec())
    );
}

#[test]
fn pattern_rules_and_default_policy_are_supported() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let base = prolly.create();
    let base = prolly
        .put(&base, b"runs/r1/summary".to_vec(), b"base".to_vec())
        .unwrap();
    let base = prolly
        .put(&base, b"runs/r1/title".to_vec(), b"base".to_vec())
        .unwrap();

    let left = prolly
        .put(&base, b"runs/r1/summary".to_vec(), b"left".to_vec())
        .unwrap();
    let left = prolly
        .put(&left, b"runs/r1/title".to_vec(), b"left".to_vec())
        .unwrap();
    let right = prolly
        .put(&base, b"runs/r1/summary".to_vec(), b"right".to_vec())
        .unwrap();
    let right = prolly
        .put(&right, b"runs/r1/title".to_vec(), b"right".to_vec())
        .unwrap();

    let policies = MergePolicyRegistry::with_default(|_| Resolution::unresolved()).add_pattern(
        "summary merge",
        |key| key.ends_with(b"/summary"),
        |conflict| {
            let mut value = conflict.left.clone().unwrap_or_default();
            value.extend_from_slice(b"\n---\n");
            value.extend(conflict.right.clone().unwrap_or_default());
            Resolution::value(value)
        },
    );

    let result = prolly.merge(&base, &left, &right, Some(policies.as_resolver()));
    assert!(matches!(result, Err(Error::Conflict(_))));

    let resolved = prolly.merge(
        &base,
        &left,
        &right,
        Some(
            MergePolicyRegistry::new()
                .add_prefix(b"runs/".to_vec(), resolver::prefer_right)
                .add_pattern(
                    "summary merge",
                    |key| key.ends_with(b"/summary"),
                    |_| Resolution::value(b"summary-merged".to_vec()),
                )
                .as_resolver(),
        ),
    );
    let merged = resolved.unwrap();
    assert_eq!(
        prolly.get(&merged, b"runs/r1/summary").unwrap(),
        Some(b"summary-merged".to_vec())
    );
    assert_eq!(
        prolly.get(&merged, b"runs/r1/title").unwrap(),
        Some(b"right".to_vec())
    );
}
