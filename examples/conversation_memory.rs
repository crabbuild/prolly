use std::sync::Arc;

use prolly::{
    prefix_range, Config, Error, KeyBuilder, MemStore, NamedRootUpdate, Prolly, VersionedValue,
};
use serde::{Deserialize, Serialize};

const MEMORY_SCHEMA: &str = "ai.memory.record";
const MEMORY_SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct MemoryRecord {
    subject: String,
    fact: String,
    source: String,
    confidence: f32,
}

fn memory_prefix(conversation_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("conversation")
        .push_str(conversation_id)
        .push_str("memory")
        .finish()
}

fn memory_key(conversation_id: &str, memory_id: &str) -> Vec<u8> {
    KeyBuilder::from_prefix(memory_prefix(conversation_id))
        .push_str(memory_id)
        .finish()
}

fn root_name(conversation_id: &str, name: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("conversation")
        .push_str(conversation_id)
        .push_str("root")
        .push_str(name)
        .finish()
}

fn attempt_name(conversation_id: &str, actor: &str, attempt: u64) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("conversation")
        .push_str(conversation_id)
        .push_str("attempt")
        .push_str(actor)
        .push_u64(attempt)
        .finish()
}

fn encode_memory(record: &MemoryRecord) -> Result<Vec<u8>, Error> {
    VersionedValue::json(MEMORY_SCHEMA, MEMORY_SCHEMA_VERSION, record)?.to_bytes()
}

fn decode_memory(bytes: &[u8]) -> Result<MemoryRecord, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(MEMORY_SCHEMA, MEMORY_SCHEMA_VERSION)?;
    value.decode_json()
}

fn put_memory(
    prolly: &Prolly<Arc<MemStore>>,
    tree: &prolly::Tree,
    conversation_id: &str,
    memory_id: &str,
    record: MemoryRecord,
) -> Result<prolly::Tree, Error> {
    prolly.put(
        tree,
        memory_key(conversation_id, memory_id),
        encode_memory(&record)?,
    )
}

fn list_memories(
    prolly: &Prolly<Arc<MemStore>>,
    tree: &prolly::Tree,
    conversation_id: &str,
) -> Result<Vec<MemoryRecord>, Error> {
    let (start, end) = prefix_range(memory_prefix(conversation_id));
    prolly
        .range(tree, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            decode_memory(&bytes)
        })
        .collect()
}

fn main() -> Result<(), Error> {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, Config::default());
    let conversation_id = "c42";
    let main_root = root_name(conversation_id, "main");

    let base = put_memory(
        &prolly,
        &prolly.create(),
        conversation_id,
        "profile/name",
        MemoryRecord {
            subject: "user".to_string(),
            fact: "Name is Ada".to_string(),
            source: "chat/turn/0001".to_string(),
            confidence: 0.99,
        },
    )?;

    let update = prolly.compare_and_swap_named_root(&main_root, None, Some(&base))?;
    assert!(matches!(update, NamedRootUpdate::Applied));

    let agent_attempt = put_memory(
        &prolly,
        &base,
        conversation_id,
        "preference/storage",
        MemoryRecord {
            subject: "user".to_string(),
            fact: "Prefers local-first durable state".to_string(),
            source: "agent/extraction/0001".to_string(),
            confidence: 0.82,
        },
    )?;
    prolly.publish_named_root(
        &attempt_name(conversation_id, "memory-agent", 1),
        &agent_attempt,
    )?;

    let concurrent_main = put_memory(
        &prolly,
        &base,
        conversation_id,
        "preference/editor",
        MemoryRecord {
            subject: "user".to_string(),
            fact: "Often edits code in a terminal".to_string(),
            source: "chat/turn/0002".to_string(),
            confidence: 0.74,
        },
    )?;
    let update =
        prolly.compare_and_swap_named_root(&main_root, Some(&base), Some(&concurrent_main))?;
    assert!(matches!(update, NamedRootUpdate::Applied));

    let canonical_before_accept = prolly.load_named_root(&main_root)?.expect("main exists");
    let merged = prolly.merge(&base, &canonical_before_accept, &agent_attempt, None)?;
    let update = prolly.compare_and_swap_named_root(
        &main_root,
        Some(&canonical_before_accept),
        Some(&merged),
    )?;
    assert!(matches!(update, NamedRootUpdate::Applied));

    let canonical = prolly.load_named_root(&main_root)?.expect("main exists");
    let memories = list_memories(&prolly, &canonical, conversation_id)?;

    assert_eq!(memories.len(), 3);
    assert!(memories
        .iter()
        .any(|record| record.fact == "Prefers local-first durable state"));
    assert!(memories
        .iter()
        .any(|record| record.fact == "Often edits code in a terminal"));

    println!(
        "conversation {conversation_id} has {} accepted memories",
        memories.len()
    );
    Ok(())
}
