use prolly::{
    prefix_range, Config, Diff, Error, KeyBuilder, MemStore, Mutation, Prolly, VersionedValue,
};
use serde::{Deserialize, Serialize};

const USER_SCHEMA: &str = "app.user";
const USER_SCHEMA_VERSION: u64 = 1;
const INDEX_VALUE: &[u8] = b"1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct UserRecord {
    tenant_id: String,
    user_id: String,
    status: String,
    display_name: String,
}

fn user_key(tenant_id: &str, user_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("source")
        .push_str("tenant")
        .push_str(tenant_id)
        .push_str("user")
        .push_str(user_id)
        .finish()
}

fn status_index_prefix(tenant_id: &str, status: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("index")
        .push_str("user-by-status")
        .push_str("tenant")
        .push_str(tenant_id)
        .push_str("status")
        .push_str(status)
        .finish()
}

fn status_index_key(user: &UserRecord) -> Vec<u8> {
    KeyBuilder::from_prefix(status_index_prefix(&user.tenant_id, &user.status))
        .push_str(&user.user_id)
        .finish()
}

fn root_name(name: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("secondary-index")
        .push_str("root")
        .push_str(name)
        .finish()
}

fn encode_user(user: &UserRecord) -> Result<Vec<u8>, Error> {
    VersionedValue::json(USER_SCHEMA, USER_SCHEMA_VERSION, user)?.to_bytes()
}

fn decode_user(bytes: &[u8]) -> Result<UserRecord, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(USER_SCHEMA, USER_SCHEMA_VERSION)?;
    value.decode_json()
}

fn put_user(
    prolly: &Prolly<MemStore>,
    tree: &prolly::Tree,
    user: UserRecord,
) -> Result<prolly::Tree, Error> {
    prolly.put(
        tree,
        user_key(&user.tenant_id, &user.user_id),
        encode_user(&user)?,
    )
}

fn build_status_index(
    prolly: &Prolly<MemStore>,
    source: &prolly::Tree,
) -> Result<prolly::Tree, Error> {
    let mutations = prolly
        .range(source, b"source", Some(b"sourcf"))?
        .map(|entry| {
            let (_, bytes) = entry?;
            let user = decode_user(&bytes)?;
            Ok(Mutation::Upsert {
                key: status_index_key(&user),
                val: INDEX_VALUE.to_vec(),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    prolly.batch(&prolly.create(), mutations)
}

fn apply_source_diff_to_status_index(
    prolly: &Prolly<MemStore>,
    index: &prolly::Tree,
    source_diff: &[Diff],
) -> Result<prolly::Tree, Error> {
    let mut mutations = Vec::new();

    for diff in source_diff {
        match diff {
            Diff::Added { val, .. } => {
                let user = decode_user(val)?;
                mutations.push(Mutation::Upsert {
                    key: status_index_key(&user),
                    val: INDEX_VALUE.to_vec(),
                });
            }
            Diff::Removed { val, .. } => {
                let user = decode_user(val)?;
                mutations.push(Mutation::Delete {
                    key: status_index_key(&user),
                });
            }
            Diff::Changed { old, new, .. } => {
                let old_user = decode_user(old)?;
                let new_user = decode_user(new)?;
                let old_index_key = status_index_key(&old_user);
                let new_index_key = status_index_key(&new_user);
                if old_index_key != new_index_key {
                    mutations.push(Mutation::Delete { key: old_index_key });
                    mutations.push(Mutation::Upsert {
                        key: new_index_key,
                        val: INDEX_VALUE.to_vec(),
                    });
                }
            }
        }
    }

    prolly.batch(index, mutations)
}

fn users_by_status(
    prolly: &Prolly<MemStore>,
    index: &prolly::Tree,
    tenant_id: &str,
    status: &str,
) -> Result<Vec<Vec<u8>>, Error> {
    let (start, end) = prefix_range(status_index_prefix(tenant_id, status));
    prolly
        .range(index, &start, end.as_deref())?
        .map(|entry| entry.map(|(key, _)| key))
        .collect()
}

fn main() -> Result<(), Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let empty = prolly.create();

    let source_v1 = put_user(
        &prolly,
        &empty,
        UserRecord {
            tenant_id: "acme".to_string(),
            user_id: "u001".to_string(),
            status: "active".to_string(),
            display_name: "Ada".to_string(),
        },
    )?;
    let source_v1 = put_user(
        &prolly,
        &source_v1,
        UserRecord {
            tenant_id: "acme".to_string(),
            user_id: "u002".to_string(),
            status: "invited".to_string(),
            display_name: "Grace".to_string(),
        },
    )?;

    let index_v1 = build_status_index(&prolly, &source_v1)?;
    prolly.publish_named_root(&root_name("users/source"), &source_v1)?;
    prolly.publish_named_root(&root_name("users/by-status"), &index_v1)?;

    let source_v2 = put_user(
        &prolly,
        &source_v1,
        UserRecord {
            tenant_id: "acme".to_string(),
            user_id: "u002".to_string(),
            status: "active".to_string(),
            display_name: "Grace".to_string(),
        },
    )?;
    let source_v2 = put_user(
        &prolly,
        &source_v2,
        UserRecord {
            tenant_id: "globex".to_string(),
            user_id: "u003".to_string(),
            status: "active".to_string(),
            display_name: "Linus".to_string(),
        },
    )?;

    let source_changes = prolly.diff(&source_v1, &source_v2)?;
    assert_eq!(source_changes.len(), 2);
    let index_v2 = apply_source_diff_to_status_index(&prolly, &index_v1, &source_changes)?;
    let rebuilt_index_v2 = build_status_index(&prolly, &source_v2)?;
    assert_eq!(index_v2, rebuilt_index_v2);

    let acme_active = users_by_status(&prolly, &index_v2, "acme", "active")?;
    let acme_invited = users_by_status(&prolly, &index_v2, "acme", "invited")?;
    let globex_active = users_by_status(&prolly, &index_v2, "globex", "active")?;

    assert_eq!(acme_active.len(), 2);
    assert!(acme_invited.is_empty());
    assert_eq!(globex_active.len(), 1);

    println!(
        "applied {} source diffs into secondary index",
        source_changes.len()
    );
    Ok(())
}
