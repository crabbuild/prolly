use std::collections::BTreeSet;

use prolly_s3_client::supported_input_fields;
use serde_json::Value;

#[test]
fn checked_in_manifest_matches_the_pinned_preview_surface() {
    let manifest: Value = serde_json::from_str(include_str!("../../compatibility-v1.json"))
        .expect("compatibility manifest is valid JSON");
    assert_eq!(manifest["schema"], "prolly-s3-compatibility/v1");
    assert_eq!(manifest["sdk"]["aws_sdk_s3"], "1.140.0");

    let operations = manifest["operations"]
        .as_object()
        .expect("operations is an object");
    for name in [
        "put_object",
        "get_object",
        "head_object",
        "delete_object",
        "delete_objects",
        "copy_object",
        "list_objects_v2",
        "list_object_versions",
        "multipart",
        "commit_session",
        "repository",
    ] {
        assert!(operations.contains_key(name), "missing operation {name}");
    }

    assert_eq!(operations["repository"]["merge"], true);
    assert_eq!(operations["repository"]["restore"], true);
    assert_eq!(operations["repository"]["gc_sweep"], true);
    assert_eq!(
        operations["put_object"]["adapter_options"],
        serde_json::json!([
            "operation_id",
            "expected_head",
            "logical_retry_limit",
            "deadline"
        ])
    );
    for operation in ["get_object", "head_object", "list_objects_v2"] {
        assert_eq!(
            operations[operation]["adapter_options"],
            serde_json::json!(["deadline"]),
            "adapter option policy diverged for {operation}"
        );
    }
    assert_eq!(
        manifest["fail_closed"]["unknown_official_input_field"],
        true
    );

    for operation in ["put_object", "get_object", "head_object", "list_objects_v2"] {
        let manifest_fields = operations[operation]["supported_fields"]
            .as_array()
            .expect("supported_fields is an array")
            .iter()
            .map(|field| field.as_str().expect("field is a string"))
            .collect::<BTreeSet<_>>();
        let runtime_fields = supported_input_fields(operation)
            .expect("runtime operation declaration exists")
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            manifest_fields, runtime_fields,
            "manifest/runtime field policy diverged for {operation}"
        );
    }
}
