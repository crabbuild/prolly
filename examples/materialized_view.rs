use std::collections::BTreeMap;

use prolly::{
    prefix_range, Config, Diff, Error, KeyBuilder, MemStore, Mutation, Prolly, Tree, VersionedValue,
};
use serde::{Deserialize, Serialize};

const ORDER_SCHEMA: &str = "app.order";
const STATUS_VIEW_SCHEMA: &str = "app.order.status_view";
const VIEW_MANIFEST_SCHEMA: &str = "app.materialized_view.manifest";
const SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct OrderRecord {
    tenant_id: String,
    order_id: String,
    status: String,
    total_cents: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StatusRevenueView {
    tenant_id: String,
    status: String,
    order_count: u64,
    total_cents: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ViewManifest {
    view_name: String,
    source_snapshot: Tree,
    view_snapshot: Tree,
    source_diff_count: usize,
}

fn source_prefix() -> Vec<u8> {
    KeyBuilder::new()
        .push_str("orders")
        .push_str("source")
        .finish()
}

fn order_key(tenant_id: &str, order_id: &str) -> Vec<u8> {
    KeyBuilder::from_prefix(source_prefix())
        .push_str("tenant")
        .push_str(tenant_id)
        .push_str("order")
        .push_str(order_id)
        .finish()
}

fn status_view_prefix(tenant_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("orders")
        .push_str("view")
        .push_str("by-status")
        .push_str("tenant")
        .push_str(tenant_id)
        .finish()
}

fn status_view_key(tenant_id: &str, status: &str) -> Vec<u8> {
    KeyBuilder::from_prefix(status_view_prefix(tenant_id))
        .push_str("status")
        .push_str(status)
        .finish()
}

fn root_name(name: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("materialized-view")
        .push_str("root")
        .push_str(name)
        .finish()
}

fn manifest_key(view_name: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("materialized-view")
        .push_str("manifest")
        .push_str(view_name)
        .finish()
}

fn encode_order(order: &OrderRecord) -> Result<Vec<u8>, Error> {
    VersionedValue::json(ORDER_SCHEMA, SCHEMA_VERSION, order)?.to_bytes()
}

fn decode_order(bytes: &[u8]) -> Result<OrderRecord, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(ORDER_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn encode_status_view(view: &StatusRevenueView) -> Result<Vec<u8>, Error> {
    VersionedValue::json(STATUS_VIEW_SCHEMA, SCHEMA_VERSION, view)?.to_bytes()
}

fn decode_status_view(bytes: &[u8]) -> Result<StatusRevenueView, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(STATUS_VIEW_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn encode_manifest(manifest: &ViewManifest) -> Result<Vec<u8>, Error> {
    VersionedValue::json(VIEW_MANIFEST_SCHEMA, SCHEMA_VERSION, manifest)?.to_bytes()
}

fn decode_manifest(bytes: &[u8]) -> Result<ViewManifest, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(VIEW_MANIFEST_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn put_order(prolly: &Prolly<MemStore>, tree: &Tree, order: OrderRecord) -> Result<Tree, Error> {
    prolly.put(
        tree,
        order_key(&order.tenant_id, &order.order_id),
        encode_order(&order)?,
    )
}

fn apply_order_to_aggregate(
    aggregate: &mut BTreeMap<(String, String), StatusRevenueView>,
    order: OrderRecord,
) {
    let key = (order.tenant_id.clone(), order.status.clone());
    let entry = aggregate.entry(key).or_insert_with(|| StatusRevenueView {
        tenant_id: order.tenant_id,
        status: order.status,
        order_count: 0,
        total_cents: 0,
    });
    entry.order_count += 1;
    entry.total_cents += order.total_cents;
}

fn build_status_view(prolly: &Prolly<MemStore>, source: &Tree) -> Result<Tree, Error> {
    let (start, end) = prefix_range(source_prefix());
    let mut aggregate = BTreeMap::new();

    for entry in prolly.range(source, &start, end.as_deref())? {
        let (_, bytes) = entry?;
        apply_order_to_aggregate(&mut aggregate, decode_order(&bytes)?);
    }

    let mutations = aggregate
        .into_values()
        .map(|view| {
            Ok(Mutation::Upsert {
                key: status_view_key(&view.tenant_id, &view.status),
                val: encode_status_view(&view)?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    prolly.batch(&prolly.create(), mutations)
}

fn record_delta(
    deltas: &mut BTreeMap<(String, String), (i64, i64)>,
    order: OrderRecord,
    count_delta: i64,
    cents_delta: i64,
) {
    let entry = deltas
        .entry((order.tenant_id, order.status))
        .or_insert((0, 0));
    entry.0 += count_delta;
    entry.1 += cents_delta;
}

fn apply_source_diff_to_status_view(
    prolly: &Prolly<MemStore>,
    view: &Tree,
    source_diff: &[Diff],
) -> Result<Tree, Error> {
    let mut deltas = BTreeMap::<(String, String), (i64, i64)>::new();

    for diff in source_diff {
        match diff {
            Diff::Added { val, .. } => {
                let order = decode_order(val)?;
                let cents = order.total_cents;
                record_delta(&mut deltas, order, 1, cents);
            }
            Diff::Removed { val, .. } => {
                let order = decode_order(val)?;
                let cents = order.total_cents;
                record_delta(&mut deltas, order, -1, -cents);
            }
            Diff::Changed { old, new, .. } => {
                let old_order = decode_order(old)?;
                let new_order = decode_order(new)?;
                let old_cents = old_order.total_cents;
                let new_cents = new_order.total_cents;
                record_delta(&mut deltas, old_order, -1, -old_cents);
                record_delta(&mut deltas, new_order, 1, new_cents);
            }
        }
    }

    let mut mutations = Vec::new();
    for ((tenant_id, status), (count_delta, cents_delta)) in deltas {
        let key = status_view_key(&tenant_id, &status);
        let existing = prolly
            .get(view, &key)?
            .map(|bytes| decode_status_view(&bytes))
            .transpose()?
            .unwrap_or(StatusRevenueView {
                tenant_id,
                status,
                order_count: 0,
                total_cents: 0,
            });

        let next_count = existing.order_count as i64 + count_delta;
        let next_total = existing.total_cents + cents_delta;
        if next_count <= 0 {
            mutations.push(Mutation::Delete { key });
        } else {
            let next = StatusRevenueView {
                order_count: next_count as u64,
                total_cents: next_total,
                ..existing
            };
            mutations.push(Mutation::Upsert {
                key,
                val: encode_status_view(&next)?,
            });
        }
    }

    prolly.batch(view, mutations)
}

fn list_status_views(
    prolly: &Prolly<MemStore>,
    view: &Tree,
    tenant_id: &str,
) -> Result<Vec<StatusRevenueView>, Error> {
    let (start, end) = prefix_range(status_view_prefix(tenant_id));
    prolly
        .range(view, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            decode_status_view(&bytes)
        })
        .collect()
}

fn put_view_manifest(
    prolly: &Prolly<MemStore>,
    manifests: &Tree,
    manifest: ViewManifest,
) -> Result<Tree, Error> {
    prolly.put(
        manifests,
        manifest_key(&manifest.view_name),
        encode_manifest(&manifest)?,
    )
}

fn main() -> Result<(), Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let view_name = "orders/by-status";

    let source_v1 = put_order(
        &prolly,
        &prolly.create(),
        OrderRecord {
            tenant_id: "acme".to_string(),
            order_id: "o001".to_string(),
            status: "paid".to_string(),
            total_cents: 1_200,
        },
    )?;
    let source_v1 = put_order(
        &prolly,
        &source_v1,
        OrderRecord {
            tenant_id: "acme".to_string(),
            order_id: "o002".to_string(),
            status: "pending".to_string(),
            total_cents: 800,
        },
    )?;
    let source_v1 = put_order(
        &prolly,
        &source_v1,
        OrderRecord {
            tenant_id: "globex".to_string(),
            order_id: "o100".to_string(),
            status: "paid".to_string(),
            total_cents: 500,
        },
    )?;

    let view_v1 = build_status_view(&prolly, &source_v1)?;
    let manifests_v1 = put_view_manifest(
        &prolly,
        &prolly.create(),
        ViewManifest {
            view_name: view_name.to_string(),
            source_snapshot: source_v1.clone(),
            view_snapshot: view_v1.clone(),
            source_diff_count: 0,
        },
    )?;
    prolly.publish_named_root(&root_name("source/current"), &source_v1)?;
    prolly.publish_named_root(&root_name("view/by-status/current"), &view_v1)?;
    prolly.publish_named_root(&root_name("view/manifest/current"), &manifests_v1)?;

    let source_v2 = put_order(
        &prolly,
        &source_v1,
        OrderRecord {
            tenant_id: "acme".to_string(),
            order_id: "o002".to_string(),
            status: "paid".to_string(),
            total_cents: 900,
        },
    )?;
    let source_v2 = put_order(
        &prolly,
        &source_v2,
        OrderRecord {
            tenant_id: "acme".to_string(),
            order_id: "o003".to_string(),
            status: "refunded".to_string(),
            total_cents: -200,
        },
    )?;
    let source_v2 = prolly.delete(&source_v2, &order_key("globex", "o100"))?;

    let source_changes = prolly.diff(&source_v1, &source_v2)?;
    assert_eq!(source_changes.len(), 3);
    let view_v2 = apply_source_diff_to_status_view(&prolly, &view_v1, &source_changes)?;
    let rebuilt_view_v2 = build_status_view(&prolly, &source_v2)?;
    assert_eq!(view_v2, rebuilt_view_v2);

    let manifests_v2 = put_view_manifest(
        &prolly,
        &manifests_v1,
        ViewManifest {
            view_name: view_name.to_string(),
            source_snapshot: source_v2.clone(),
            view_snapshot: view_v2.clone(),
            source_diff_count: source_changes.len(),
        },
    )?;
    prolly.publish_named_root(&root_name("source/current"), &source_v2)?;
    prolly.publish_named_root(&root_name("view/by-status/current"), &view_v2)?;
    prolly.publish_named_root(&root_name("view/manifest/current"), &manifests_v2)?;

    assert_eq!(
        prolly.load_named_root(&root_name("view/by-status/current"))?,
        Some(view_v2.clone())
    );

    let acme_views = list_status_views(&prolly, &view_v2, "acme")?;
    assert_eq!(acme_views.len(), 2);
    assert!(acme_views.iter().any(|view| {
        view.status == "paid" && view.order_count == 2 && view.total_cents == 2_100
    }));
    assert!(acme_views.iter().any(|view| {
        view.status == "refunded" && view.order_count == 1 && view.total_cents == -200
    }));
    assert!(list_status_views(&prolly, &view_v2, "globex")?.is_empty());

    let manifest_bytes = prolly
        .get(&manifests_v2, &manifest_key(view_name))?
        .expect("view manifest exists");
    let manifest = decode_manifest(&manifest_bytes)?;
    assert_eq!(manifest.source_snapshot, source_v2);
    assert_eq!(manifest.view_snapshot, view_v2);
    assert_eq!(manifest.source_diff_count, 3);

    println!(
        "updated materialized view from {} source diffs into {} tenant rows",
        source_changes.len(),
        acme_views.len()
    );
    Ok(())
}
