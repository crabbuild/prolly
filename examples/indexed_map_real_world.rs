//! Runnable secondary-index patterns for production-shaped application data.
//!
//! Run with:
//!
//! ```text
//! cargo run --example indexed_map_real_world
//! ```

use prolly::{
    Config, Error, IndexProjection, KeyBuilder, MemStore, Prolly, SecondaryIndex,
    SecondaryIndexEntry, SecondaryIndexError, SecondaryIndexRegistry,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

type ExampleResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct User {
    tenant_id: String,
    status: String,
    email: String,
    display_name: String,
    plan: String,
    tags: Vec<String>,
    group_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct UserSummary {
    display_name: String,
    plan: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Order {
    customer_id: String,
    total_cents: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Task {
    state: String,
    due_timestamp_ms: u64,
    title: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ExpiringItem {
    expires_at_ms: u64,
    payload: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Place {
    geohash: String,
    latitude: f64,
    longitude: f64,
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Document {
    path: String,
    title: String,
    body: String,
}

fn decode<T: DeserializeOwned>(value: &[u8]) -> Result<T, SecondaryIndexError> {
    serde_json::from_slice(value)
        .map_err(|_| SecondaryIndexError::new("source value is not valid application JSON"))
}

fn encode<T: Serialize>(value: &T) -> ExampleResult<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

fn normalize(value: &str) -> String {
    value.trim().to_lowercase()
}

fn tenant_status_term(tenant_id: &str, status: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str(tenant_id)
        .push_str(status)
        .finish()
}

fn tenant_prefix(tenant_id: &str) -> Vec<u8> {
    KeyBuilder::new().push_str(tenant_id).finish()
}

fn state_due_term(state: &str, due_timestamp_ms: u64) -> Vec<u8> {
    KeyBuilder::new()
        .push_str(state)
        .push_u64(due_timestamp_ms)
        .finish()
}

fn canonical_path_term(path: &str) -> Result<Vec<u8>, SecondaryIndexError> {
    let mut builder = KeyBuilder::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        if segment == "." || segment == ".." {
            return Err(SecondaryIndexError::new(
                "document paths must not contain dot segments",
            ));
        }
        builder = builder.push_str(segment);
    }
    Ok(builder.finish())
}

fn normalized_tokens(text: &str) -> Vec<Vec<u8>> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(normalize)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(String::into_bytes)
        .collect()
}

fn user_registry() -> Result<SecondaryIndexRegistry, Error> {
    let by_status =
        SecondaryIndex::non_unique("by-status", 1, "examples.users.by-status/1", |_, value| {
            let user = decode::<User>(value)?;
            Ok(vec![normalize(&user.status).into_bytes()])
        })?;

    let by_tenant_status = SecondaryIndex::non_unique(
        "by-tenant-status",
        1,
        "examples.users.by-tenant-status/1",
        |_, value| {
            let user = decode::<User>(value)?;
            Ok(vec![tenant_status_term(
                &normalize(&user.tenant_id),
                &normalize(&user.status),
            )])
        },
    )?;

    let by_tag = SecondaryIndex::non_unique("by-tag", 1, "examples.users.by-tag/1", |_, value| {
        let user = decode::<User>(value)?;
        Ok(user
            .tags
            .iter()
            .map(|tag| normalize(tag).into_bytes())
            .collect())
    })?;

    let by_group =
        SecondaryIndex::non_unique("by-group", 1, "examples.users.by-group/1", |_, value| {
            let user = decode::<User>(value)?;
            Ok(user
                .group_ids
                .iter()
                .map(|group_id| normalize(group_id).into_bytes())
                .collect())
        })?;

    let by_email =
        SecondaryIndex::non_unique("by-email", 1, "examples.users.by-email/1", |_, value| {
            let user = decode::<User>(value)?;
            Ok(vec![normalize(&user.email).into_bytes()])
        })?;

    let by_status_summary =
        SecondaryIndex::builder("by-status-summary", 1, "examples.users.by-status-summary/1")
            .projection(IndexProjection::Include)
            .extract(|_, value| {
                let user = decode::<User>(value)?;
                let summary = UserSummary {
                    display_name: user.display_name,
                    plan: user.plan,
                };
                let projection = serde_json::to_vec(&summary)
                    .map_err(|_| SecondaryIndexError::new("could not encode user summary"))?;
                Ok(vec![SecondaryIndexEntry::included(
                    normalize(&user.status),
                    projection,
                )])
            })?;

    SecondaryIndexRegistry::new()
        .register(by_status)?
        .register(by_tenant_status)?
        .register(by_tag)?
        .register(by_group)?
        .register(by_email)?
        .register(by_status_summary)
}

fn order_registry() -> Result<SecondaryIndexRegistry, Error> {
    let by_customer = SecondaryIndex::non_unique(
        "by-customer",
        1,
        "examples.orders.by-customer/1",
        |_, value| {
            let order = decode::<Order>(value)?;
            Ok(vec![normalize(&order.customer_id).into_bytes()])
        },
    )?;
    SecondaryIndexRegistry::new().register(by_customer)
}

fn task_registry() -> Result<SecondaryIndexRegistry, Error> {
    let by_state_due = SecondaryIndex::non_unique(
        "by-state-due",
        1,
        "examples.tasks.by-state-due/1",
        |_, value| {
            let task = decode::<Task>(value)?;
            Ok(vec![state_due_term(
                &normalize(&task.state),
                task.due_timestamp_ms,
            )])
        },
    )?;

    let pending_only = SecondaryIndex::non_unique(
        "pending-only",
        1,
        "examples.tasks.pending-only/1",
        |_, value| {
            let task = decode::<Task>(value)?;
            if normalize(&task.state) == "pending" {
                Ok(vec![b"pending".to_vec()])
            } else {
                Ok(Vec::new())
            }
        },
    )?;

    SecondaryIndexRegistry::new()
        .register(by_state_due)?
        .register(pending_only)
}

fn expiration_registry() -> Result<SecondaryIndexRegistry, Error> {
    let by_expiry = SecondaryIndex::non_unique(
        "by-expiry",
        1,
        "examples.expiration.by-expiry/1",
        |_, value| {
            let item = decode::<ExpiringItem>(value)?;
            Ok(vec![item.expires_at_ms.to_be_bytes().to_vec()])
        },
    )?;
    SecondaryIndexRegistry::new().register(by_expiry)
}

fn place_registry() -> Result<SecondaryIndexRegistry, Error> {
    let by_cell = SecondaryIndex::builder("by-cell", 1, "examples.places.by-cell/1")
        .projection(IndexProjection::All)
        .extract_terms(|_, value| {
            let place = decode::<Place>(value)?;
            Ok(vec![normalize(&place.geohash).into_bytes()])
        })?;
    SecondaryIndexRegistry::new().register(by_cell)
}

fn document_registry() -> Result<SecondaryIndexRegistry, Error> {
    let by_path =
        SecondaryIndex::non_unique("by-path", 1, "examples.documents.by-path/1", |_, value| {
            let document = decode::<Document>(value)?;
            Ok(vec![canonical_path_term(&document.path)?])
        })?;

    let by_token = SecondaryIndex::non_unique(
        "by-token",
        1,
        "examples.documents.by-token/1",
        |_, value| {
            let document = decode::<Document>(value)?;
            Ok(normalized_tokens(&format!(
                "{} {}",
                document.title, document.body
            )))
        },
    )?;

    SecondaryIndexRegistry::new()
        .register(by_path)?
        .register(by_token)
}

fn main() -> ExampleResult {
    let engine = Prolly::new(Arc::new(MemStore::new()), Config::default());
    user_and_access_patterns(&engine)?;
    order_pattern(&engine)?;
    task_and_expiration_patterns(&engine)?;
    path_text_and_geospatial_patterns(&engine)?;
    println!("verified 14 IndexedMap real-world secondary-index patterns");
    Ok(())
}

fn user_and_access_patterns(engine: &Prolly<Arc<MemStore>>) -> ExampleResult {
    let users = engine.indexed_map(b"example-users", user_registry()?)?;
    for name in [
        b"by-status".as_slice(),
        b"by-tenant-status".as_slice(),
        b"by-tag".as_slice(),
        b"by-group".as_slice(),
        b"by-email".as_slice(),
        b"by-status-summary".as_slice(),
    ] {
        users.ensure_index(name)?;
    }

    let ada = User {
        tenant_id: "acme".into(),
        status: "active".into(),
        email: "ADA@Example.com ".into(),
        display_name: "Ada".into(),
        plan: "enterprise".into(),
        tags: vec!["rust".into(), "database".into()],
        group_ids: vec!["admins".into(), "billing".into()],
    };
    let grace = User {
        tenant_id: "acme".into(),
        status: "invited".into(),
        email: "ada@example.com".into(),
        display_name: "Grace".into(),
        plan: "team".into(),
        tags: vec!["database".into()],
        group_ids: vec!["billing".into()],
    };
    let lin = User {
        tenant_id: "northwind".into(),
        status: "active".into(),
        email: "lin@example.com".into(),
        display_name: "Lin".into(),
        plan: "free".into(),
        tags: vec!["rust".into()],
        group_ids: vec!["readers".into()],
    };

    users.edit(|edit| {
        edit.put(b"user-ada".to_vec(), encode(&ada).unwrap());
        edit.put(b"user-grace".to_vec(), encode(&grace).unwrap());
        edit.put(b"user-lin".to_vec(), encode(&lin).unwrap());
    })?;
    let first_version = users.snapshot()?.source_version().clone();
    let snapshot = users.snapshot()?;

    // 1. Users by status: exact lookup.
    assert_eq!(
        snapshot.index(b"by-status")?.primary_keys(b"active")?,
        vec![b"user-ada".to_vec(), b"user-lin".to_vec()]
    );

    // 2. Multi-tenant status: exact lookup and tenant prefix scan.
    let acme_active = tenant_status_term("acme", "active");
    assert_eq!(
        snapshot
            .index(b"by-tenant-status")?
            .primary_keys(&acme_active)?,
        vec![b"user-ada".to_vec()]
    );
    assert_eq!(
        snapshot
            .index(b"by-tenant-status")?
            .prefix(&tenant_prefix("acme"))?
            .len(),
        2
    );

    // 3. Tags and categories: one emitted term per tag.
    assert_eq!(
        snapshot.index(b"by-tag")?.primary_keys(b"database")?,
        vec![b"user-ada".to_vec(), b"user-grace".to_vec()]
    );

    // 4. Access-control-list reverse lookup: one term per group membership.
    assert_eq!(
        snapshot.index(b"by-group")?.primary_keys(b"billing")?,
        vec![b"user-ada".to_vec(), b"user-grace".to_vec()]
    );

    // 5. Normalized email lookup: exact lookup does not enforce uniqueness.
    assert_eq!(
        snapshot
            .index(b"by-email")?
            .primary_keys(b"ada@example.com")?
            .len(),
        2
    );

    // 6. Covering dashboard: read summary projections without source fetches.
    let summaries = snapshot.index(b"by-status-summary")?.projected(b"active")?;
    let ada_summary: UserSummary = serde_json::from_slice(
        summaries
            .iter()
            .find(|(key, _)| key == b"user-ada")
            .and_then(|(_, projection)| projection.as_deref())
            .expect("Ada has an included projection"),
    )?;
    assert_eq!(ada_summary.plan, "enterprise");

    // 7. Historical audit query: retain and reopen the old source version.
    let mut updated_ada = ada;
    updated_ada.status = "suspended".into();
    users.put(b"user-ada", encode(&updated_ada)?)?;
    users.keep_last(2)?;
    let historical = users.snapshot_at(&first_version)?;
    assert_eq!(
        historical.index(b"by-status")?.primary_keys(b"active")?,
        vec![b"user-ada".to_vec(), b"user-lin".to_vec()]
    );
    assert_eq!(
        users
            .snapshot()?
            .index(b"by-status")?
            .primary_keys(b"suspended")?,
        vec![b"user-ada".to_vec()]
    );
    Ok(())
}

fn order_pattern(engine: &Prolly<Arc<MemStore>>) -> ExampleResult {
    let orders = engine.indexed_map(b"example-orders", order_registry()?)?;
    orders.ensure_index(b"by-customer")?;
    orders.edit(|edit| {
        edit.put(
            b"order-100".to_vec(),
            encode(&Order {
                customer_id: "customer-7".into(),
                total_cents: 12_500,
            })
            .unwrap(),
        );
        edit.put(
            b"order-101".to_vec(),
            encode(&Order {
                customer_id: "customer-7".into(),
                total_cents: 8_000,
            })
            .unwrap(),
        );
    })?;

    // 8. Orders by customer: exact reverse lookup from customer to orders.
    assert_eq!(
        orders
            .snapshot()?
            .index(b"by-customer")?
            .primary_keys(b"customer-7")?
            .len(),
        2
    );
    Ok(())
}

fn task_and_expiration_patterns(engine: &Prolly<Arc<MemStore>>) -> ExampleResult {
    let tasks = engine.indexed_map(b"example-tasks", task_registry()?)?;
    tasks.ensure_index(b"by-state-due")?;
    tasks.ensure_index(b"pending-only")?;
    tasks.edit(|edit| {
        for (key, state, due, title) in [
            ("task-1", "pending", 1_000, "send invoice"),
            ("task-2", "pending", 2_000, "renew certificate"),
            ("task-3", "done", 1_500, "archive export"),
        ] {
            edit.put(
                key.as_bytes().to_vec(),
                encode(&Task {
                    state: state.into(),
                    due_timestamp_ms: due,
                    title: title.into(),
                })
                .unwrap(),
            );
        }
    })?;
    let snapshot = tasks.snapshot()?;

    // 9. Tasks ordered by state and time: range and reverse-range lookup.
    let start = state_due_term("pending", 0);
    let end = state_due_term("pending", 3_000);
    let ascending = snapshot.index(b"by-state-due")?.range(&start, Some(&end))?;
    let descending =
        snapshot
            .index(b"by-state-due")?
            .range_reverse_page(&start, Some(&end), None, 10)?;
    assert_eq!(ascending.len(), 2);
    assert_eq!(descending.matches[0].primary_key, b"task-2");

    // 10. Sparse pending jobs: completed tasks emit no index term.
    assert_eq!(
        snapshot
            .index(b"pending-only")?
            .primary_keys(b"pending")?
            .len(),
        2
    );

    let expiring = engine.indexed_map(b"example-expiring", expiration_registry()?)?;
    expiring.ensure_index(b"by-expiry")?;
    expiring.edit(|edit| {
        for (key, expires_at_ms) in [("session-1", 900_u64), ("session-2", 1_100)] {
            edit.put(
                key.as_bytes().to_vec(),
                encode(&ExpiringItem {
                    expires_at_ms,
                    payload: key.into(),
                })
                .unwrap(),
            );
        }
    })?;

    // 11. Expiration processing: big-endian timestamps preserve numeric order.
    let minimum = 0_u64.to_be_bytes();
    let cutoff_exclusive = 1_001_u64.to_be_bytes();
    let expired = expiring
        .snapshot()?
        .index(b"by-expiry")?
        .range(&minimum, Some(&cutoff_exclusive))?;
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].primary_key, b"session-1");
    Ok(())
}

fn path_text_and_geospatial_patterns(engine: &Prolly<Arc<MemStore>>) -> ExampleResult {
    let documents = engine.indexed_map(b"example-documents", document_registry()?)?;
    documents.ensure_index(b"by-path")?;
    documents.ensure_index(b"by-token")?;
    documents.edit(|edit| {
        for (key, path, title, body) in [
            (
                "doc-1",
                "/acme/projects/alpha/readme",
                "Rust storage",
                "Content addressed database nodes",
            ),
            (
                "doc-2",
                "/acme/projects/beta/readme",
                "Database guide",
                "Rust indexing patterns",
            ),
            (
                "doc-3",
                "/northwind/reports/q1",
                "Quarterly report",
                "Revenue and expenses",
            ),
        ] {
            edit.put(
                key.as_bytes().to_vec(),
                encode(&Document {
                    path: path.into(),
                    title: title.into(),
                    body: body.into(),
                })
                .unwrap(),
            );
        }
    })?;
    let snapshot = documents.snapshot()?;

    // 12. Hierarchical paths: canonical segments support subtree prefixes.
    let acme_projects = canonical_path_term("/acme/projects")?;
    assert_eq!(snapshot.index(b"by-path")?.prefix(&acme_projects)?.len(), 2);

    // 13. Basic inverted text: emit one normalized term per distinct token.
    assert_eq!(
        snapshot.index(b"by-token")?.primary_keys(b"rust")?,
        vec![b"doc-1".to_vec(), b"doc-2".to_vec()]
    );
    assert_eq!(snapshot.index(b"by-token")?.prefix(b"data")?.len(), 2);

    let places = engine.indexed_map(b"example-places", place_registry()?)?;
    places.ensure_index(b"by-cell")?;
    places.edit(|edit| {
        for (key, geohash, latitude, longitude, name) in [
            ("place-1", "c2b2q", 49.2827, -123.1207, "Vancouver"),
            ("place-2", "c2b2r", 49.25, -123.1, "South Vancouver"),
            ("place-3", "dr5ru", 40.7128, -74.006, "New York"),
        ] {
            edit.put(
                key.as_bytes().to_vec(),
                encode(&Place {
                    geohash: geohash.into(),
                    latitude,
                    longitude,
                    name: name.into(),
                })
                .unwrap(),
            );
        }
    })?;

    // 14. Geospatial buckets: prefix candidates, then apply exact geometry.
    let candidates = places.snapshot()?.index(b"by-cell")?.prefix(b"c2b2")?;
    let inside_box = candidates
        .iter()
        .filter_map(|matched| matched.projection.as_deref())
        .map(serde_json::from_slice::<Place>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|place| {
            (49.20..=49.30).contains(&place.latitude)
                && (-123.20..=-123.00).contains(&place.longitude)
        })
        .collect::<Vec<_>>();
    assert_eq!(inside_box.len(), 2);
    Ok(())
}
