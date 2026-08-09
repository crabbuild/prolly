use prolly_dynamodb_core::DatabaseFormatRecord;
use prolly_store_dynamodb::{DynamoDbTransactionCapabilities, TransactionPublicationMode};
use serde::{Deserialize, Serialize};

/// Compatibility level for one advertised operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityLevel {
    Exact,
    CompatibleStronger,
    Subset,
    Extension,
}

/// Machine-readable support contract for one operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationCapability {
    pub operation: String,
    pub level: CompatibilityLevel,
    pub fluent: bool,
    pub input_first: bool,
    pub supported_fields: Vec<String>,
    pub expression_forms: Vec<String>,
    pub semantic_differences: Vec<String>,
}

/// Frozen capabilities for one opened client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityReport {
    pub database_format_version: u32,
    /// Exact fixed-width durable namespace record negotiated by `Client::open`.
    pub database_format_record_hex: String,
    pub logical_protocol_major: u16,
    pub logical_protocol_minor: u16,
    pub item_codec: String,
    pub item_codec_digest: String,
    pub key_codec_digest: String,
    pub catalog_codec_digest: String,
    pub commit_codec_digest: String,
    pub tree_format_digest: String,
    pub aws_sdk_dynamodb_version: String,
    pub maximum_logical_item_bytes: usize,
    pub large_value_inline_threshold: usize,
    pub maximum_root_actions: usize,
    pub maximum_version_page_items: usize,
    pub maximum_diff_page_items: usize,
    pub maximum_collected_versions: usize,
    pub maximum_collected_diff_items: usize,
    pub maximum_retention_protected_versions: usize,
    pub maximum_retention_removals: usize,
    pub minimum_maintenance_lease_millis: u64,
    pub maximum_maintenance_lease_millis: u64,
    pub maximum_gc_candidate_page_evaluated_items: usize,
    pub maximum_gc_plan_deletes: usize,
    pub maximum_gc_blob_delete_parallelism: usize,
    pub gc_requires_maintenance_lease: bool,
    /// Retries after the first optimistic logical attempt.
    pub logical_retry_limit: usize,
    /// Clones of this opened client share one pre-publication data-write gate.
    pub process_local_write_admission: bool,
    pub node_cache_max_nodes: Option<usize>,
    pub node_cache_max_bytes: Option<usize>,
    pub transaction_publication_mode: String,
    pub staged_node_deletes: bool,
    pub operations: Vec<OperationCapability>,
    pub extensions: Vec<String>,
}

impl CapabilityReport {
    pub(crate) fn new(
        transaction: DynamoDbTransactionCapabilities,
        format: &DatabaseFormatRecord,
        runtime: &prolly::RuntimeConfig,
        logical_retry_limit: usize,
    ) -> Self {
        let publication_mode = match transaction.publication_mode {
            TransactionPublicationMode::PrepublishImmutableNodes => "prepublish_immutable_nodes",
            TransactionPublicationMode::AtomicNodesAndRoots => "atomic_nodes_and_roots",
        };
        Self {
            database_format_version: format.format_version,
            database_format_record_hex: bytes_hex(&format.encode()),
            logical_protocol_major: format.logical_protocol_major,
            logical_protocol_minor: format.logical_protocol_minor,
            item_codec: "DDBI-v1-canonical-cbor".into(),
            item_codec_digest: cid_hex(&format.item_codec_digest),
            key_codec_digest: cid_hex(&format.key_codec_digest),
            catalog_codec_digest: cid_hex(&format.catalog_codec_digest),
            commit_codec_digest: cid_hex(&format.commit_codec_digest),
            tree_format_digest: cid_hex(&format.tree_format_digest),
            aws_sdk_dynamodb_version: "1.73.0".into(),
            maximum_logical_item_bytes: prolly_dynamodb_core::MAX_ITEM_BYTES,
            large_value_inline_threshold: usize::try_from(format.large_value_inline_threshold)
                .unwrap_or(usize::MAX),
            maximum_root_actions: transaction.root_action_limit,
            maximum_version_page_items: prolly_dynamodb_core::MAX_VERSION_PAGE_ITEMS,
            maximum_diff_page_items: prolly_dynamodb_core::MAX_DIFF_PAGE_ITEMS,
            maximum_collected_versions: prolly_dynamodb_core::MAX_COLLECTED_VERSIONS,
            maximum_collected_diff_items: prolly_dynamodb_core::MAX_COLLECTED_DIFF_ITEMS,
            maximum_retention_protected_versions:
                prolly_dynamodb_core::MAX_RETENTION_PROTECTED_VERSIONS,
            maximum_retention_removals: prolly_dynamodb_core::MAX_RETENTION_REMOVALS,
            minimum_maintenance_lease_millis: prolly_dynamodb_core::MIN_MAINTENANCE_LEASE_MILLIS,
            maximum_maintenance_lease_millis: prolly_dynamodb_core::MAX_MAINTENANCE_LEASE_MILLIS,
            maximum_gc_candidate_page_evaluated_items:
                prolly_store_dynamodb::DYNAMODB_SCAN_PAGE_LIMIT,
            maximum_gc_plan_deletes: prolly_dynamodb_core::MAX_GC_PLAN_DELETES,
            maximum_gc_blob_delete_parallelism: crate::MAX_GC_BLOB_DELETE_PARALLELISM,
            gc_requires_maintenance_lease: true,
            logical_retry_limit,
            process_local_write_admission: true,
            node_cache_max_nodes: runtime.node_cache_max_nodes,
            node_cache_max_bytes: runtime.node_cache_max_bytes,
            transaction_publication_mode: publication_mode.into(),
            staged_node_deletes: transaction.staged_node_deletes,
            operations: operation_capabilities(),
            extensions: [
                "head",
                "versions",
                "commits",
                "commit",
                "at",
                "diff",
                "restore",
                "retention",
                "retention_audit",
                "export",
                "import",
                "import_audit",
                "maintenance_lease",
                "gc_plan",
                "gc_apply",
                "if_head",
                "send_with_metadata",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        }
    }

    /// Find an operation by its official DynamoDB name.
    pub fn operation(&self, name: &str) -> Option<&OperationCapability> {
        self.operations
            .iter()
            .find(|operation| operation.operation == name)
    }

    /// Serialize the frozen report for deployment checks and diagnostics.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

fn operation_capabilities() -> Vec<OperationCapability> {
    vec![
        capability("CreateTable", CompatibilityLevel::Subset, true, &[
            "table_name", "attribute_definitions", "key_schema",
            "local_secondary_indexes", "global_secondary_indexes",
        ]),
        capability("DescribeTable", CompatibilityLevel::Subset, true, &["table_name"]),
        capability("ListTables", CompatibilityLevel::Subset, true, &[
            "exclusive_start_table_name", "limit",
        ]),
        capability("DeleteTable", CompatibilityLevel::Subset, true, &["table_name"]),
        OperationCapability {
            operation: "GetItem".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "table_name", "key", "projection_expression", "expression_attribute_names",
            ]),
            expression_forms: strings(&["projection: aliased document paths"]),
            semantic_differences: strings(&["reads are always strongly consistent"]),
        },
        OperationCapability {
            operation: "PutItem".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "table_name", "item", "condition_expression",
                "expression_attribute_names", "expression_attribute_values", "return_values",
                "return_values_on_condition_check_failure",
            ]),
            expression_forms: strings(&[
                "aliased document-path comparisons = <> < <= > >=",
                "BETWEEN, IN, AND, OR, NOT, and parentheses",
                "attribute_exists, attribute_not_exists, attribute_type, contains, begins_with, size",
            ]),
            semantic_differences: Vec::new(),
        },
        OperationCapability {
            operation: "DeleteItem".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "table_name", "key", "condition_expression",
                "expression_attribute_names", "expression_attribute_values", "return_values",
                "return_values_on_condition_check_failure",
            ]),
            expression_forms: strings(&[
                "aliased document-path comparisons = <> < <= > >=",
                "BETWEEN, IN, AND, OR, NOT, and parentheses",
                "attribute_exists, attribute_not_exists, attribute_type, contains, begins_with, size",
            ]),
            semantic_differences: Vec::new(),
        },
        OperationCapability {
            operation: "UpdateItem".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "table_name", "key", "update_expression", "condition_expression",
                "expression_attribute_names", "expression_attribute_values", "return_values",
                "return_values_on_condition_check_failure",
            ]),
            expression_forms: strings(&[
                "SET path = value|path|if_not_exists(...)|list_append(...)",
                "SET path = operand +|- operand",
                "REMOVE document_path",
                "ADD #name :value (number or homogeneous set)",
                "DELETE #name :value (homogeneous set)",
                "condition: comparisons, BETWEEN, IN, boolean operators, and supported functions",
            ]),
            semantic_differences: strings(&[
                "attribute aliases are mandatory",
                "all operands are evaluated from one immutable old item",
                "ADD and DELETE are top-level only, matching DynamoDB",
            ]),
        },
        OperationCapability {
            operation: "Query".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "table_name", "key_condition_expression", "expression_attribute_names",
                "expression_attribute_values", "filter_expression", "projection_expression",
                "index_name", "exclusive_start_key", "limit", "scan_index_forward", "select",
                "consistent_read",
            ]),
            expression_forms: strings(&[
                "#partition_key = :value",
                "AND #sort_key =|<|<=|>|>= :value",
                "AND #sort_key BETWEEN :lower AND :upper",
                "AND begins_with(#sort_key, :prefix)",
                "filter: comparisons, BETWEEN, IN, boolean operators, and supported functions",
                "projection: aliased document paths",
            ]),
            semantic_differences: strings(&[
                "each operation is pinned to one immutable base/index version pair",
                "ConsistentRead=true is rejected for global secondary indexes",
                "ALL_ATTRIBUTES on a non-ALL index is not implemented",
            ]),
        },
        OperationCapability {
            operation: "Scan".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "table_name", "index_name", "exclusive_start_key", "limit", "filter_expression",
                "projection_expression", "expression_attribute_names",
                "expression_attribute_values", "select", "consistent_read",
            ]),
            expression_forms: strings(&[
                "filter: comparisons, BETWEEN, IN, boolean operators, and supported functions",
                "projection: aliased document paths",
            ]),
            semantic_differences: strings(&[
                "serial scan only",
                "each operation is pinned to one immutable base/index version pair",
                "ConsistentRead=true is rejected for global secondary indexes",
                "ALL_ATTRIBUTES on a non-ALL index is not implemented",
            ]),
        },
        OperationCapability {
            operation: "BatchGetItem".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "request_items.keys",
                "request_items.consistent_read",
                "request_items.projection_expression",
                "request_items.expression_attribute_names",
                "return_consumed_capacity=NONE",
            ]),
            expression_forms: strings(&["projection: aliased document paths"]),
            semantic_differences: strings(&[
                "reads are always strongly consistent",
                "one immutable snapshot is pinned per table",
                "partial results currently arise from the 1-MiB-per-partition or 16-MiB response limits",
            ]),
        },
        OperationCapability {
            operation: "BatchWriteItem".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "request_items.put_request",
                "request_items.delete_request",
                "return_consumed_capacity=NONE",
                "return_item_collection_metrics=NONE",
            ]),
            expression_forms: Vec::new(),
            semantic_differences: strings(&[
                "each item is an independent logical table-head transition",
                "outcome-unknown publication is a structured error and is never returned as unprocessed",
            ]),
        },
        OperationCapability {
            operation: "TransactGetItems".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "transact_items.get.table_name",
                "transact_items.get.key",
                "transact_items.get.projection_expression",
                "transact_items.get.expression_attribute_names",
                "return_consumed_capacity=NONE",
            ]),
            expression_forms: strings(&["projection: aliased document paths"]),
            semantic_differences: strings(&[
                "reads are always strongly consistent",
                "table_versions reports the atomically validated logical read set",
            ]),
        },
        OperationCapability {
            operation: "TransactWriteItems".into(),
            level: CompatibilityLevel::Subset,
            fluent: true,
            input_first: true,
            supported_fields: strings(&[
                "transact_items.put",
                "transact_items.delete",
                "transact_items.update",
                "transact_items.condition_check",
                "client_request_token",
                "return_consumed_capacity=NONE",
                "return_item_collection_metrics=NONE",
            ]),
            expression_forms: strings(&[
                "condition: declared condition-expression subset",
                "update: declared UpdateItem expression subset",
            ]),
            semantic_differences: strings(&[
                "ClientRequestToken uses a durable canonical fingerprint and 10-minute replay record",
                "one transaction publishes all participating logical table heads atomically",
                "effective modified-table count is bounded by physical root actions",
            ]),
        },
    ]
}

fn capability(
    operation: &str,
    level: CompatibilityLevel,
    input_first: bool,
    fields: &[&str],
) -> OperationCapability {
    OperationCapability {
        operation: operation.into(),
        level,
        fluent: true,
        input_first,
        supported_fields: strings(fields),
        expression_forms: Vec::new(),
        semantic_differences: Vec::new(),
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn cid_hex(cid: &prolly::Cid) -> String {
    bytes_hex(cid.as_bytes())
}

fn bytes_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_report_carries_the_exact_negotiated_format_record() {
        let format = DatabaseFormatRecord {
            format_version: 10,
            logical_protocol_major: 1,
            logical_protocol_minor: 0,
            item_codec_digest: prolly::Cid::from_bytes(b"item"),
            key_codec_digest: prolly::Cid::from_bytes(b"key"),
            catalog_codec_digest: prolly::Cid::from_bytes(b"catalog"),
            commit_codec_digest: prolly::Cid::from_bytes(b"commit"),
            tree_format_digest: prolly::Cid::from_bytes(b"tree"),
            publication_mode:
                prolly_dynamodb_core::StoragePublicationMode::PrepublishImmutableNodes,
            large_value_inline_threshold: 65_536,
            minimum_reader_version: 9,
            minimum_writer_version: 10,
        };
        let report = CapabilityReport::new(
            DynamoDbTransactionCapabilities {
                root_action_limit: 100,
                publication_mode: TransactionPublicationMode::PrepublishImmutableNodes,
                staged_node_deletes: false,
            },
            &format,
            &prolly::RuntimeConfig::default(),
            prolly_dynamodb_core::DEFAULT_LOGICAL_RETRY_LIMIT,
        );

        assert_eq!(
            report.database_format_record_hex,
            bytes_hex(&format.encode())
        );
        assert_eq!(report.database_format_record_hex.len(), 380);
        assert_eq!(
            report.logical_retry_limit,
            prolly_dynamodb_core::DEFAULT_LOGICAL_RETRY_LIMIT
        );
        assert_eq!(report.node_cache_max_nodes, None);
        assert_eq!(report.node_cache_max_bytes, Some(256 * 1024 * 1024));
        assert!(report
            .database_format_record_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    }

    #[test]
    fn operation_names_are_unique_and_report_serializes() {
        let operations = operation_capabilities();
        for (index, operation) in operations.iter().enumerate() {
            assert!(!operation.supported_fields.is_empty());
            assert!(!operations[..index]
                .iter()
                .any(|other| other.operation == operation.operation));
        }
        let json = serde_json::to_string(&operations).unwrap();
        assert!(json.contains("PutItem"));
        assert!(json.contains("condition_expression"));
    }
}
