use prolly::{Config, Error, KeyBuilder, MemStore, Mutation, Prolly, Tree, VersionedValue};
use serde::{Deserialize, Serialize};

const EVENT_SCHEMA: &str = "ai.agent.event";
const EVENT_SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AgentEvent {
    run_id: String,
    actor: String,
    timestamp_millis: u64,
    sequence: u64,
    kind: AgentEventKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum AgentEventKind {
    UserMessage {
        content: String,
    },
    AssistantMessage {
        content: String,
    },
    ToolCall {
        call_id: String,
        tool: String,
        arguments_json: String,
    },
    ToolResult {
        call_id: String,
        output: String,
    },
    MemoryWrite {
        key: Vec<u8>,
        value: String,
    },
    Checkpoint {
        label: String,
        root: Tree,
    },
    SummaryCompaction {
        first_sequence: u64,
        last_sequence: u64,
        summary: String,
    },
}

fn event_prefix(run_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("agent-log")
        .push_str(run_id)
        .push_str("event")
        .finish()
}

fn event_key(event: &AgentEvent) -> Vec<u8> {
    KeyBuilder::from_prefix(event_prefix(&event.run_id))
        .push_timestamp_millis(event.timestamp_millis)
        .push_u64(event.sequence)
        .finish()
}

fn memory_key(run_id: &str, key: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("agent-log")
        .push_str(run_id)
        .push_str("memory")
        .push_str(key)
        .finish()
}

fn root_name(run_id: &str, name: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("agent-log")
        .push_str(run_id)
        .push_str("root")
        .push_str(name)
        .finish()
}

fn encode_event(event: &AgentEvent) -> Result<Vec<u8>, Error> {
    VersionedValue::json(EVENT_SCHEMA, EVENT_SCHEMA_VERSION, event)?.to_bytes()
}

fn decode_event(bytes: &[u8]) -> Result<AgentEvent, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(EVENT_SCHEMA, EVENT_SCHEMA_VERSION)?;
    value.decode_json()
}

fn append_events(
    prolly: &Prolly<MemStore>,
    tree: &Tree,
    events: &[AgentEvent],
) -> Result<Tree, Error> {
    let mutations = events
        .iter()
        .map(|event| {
            Ok(Mutation::Upsert {
                key: event_key(event),
                val: encode_event(event)?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    prolly.batch(tree, mutations)
}

fn load_events(
    prolly: &Prolly<MemStore>,
    tree: &Tree,
    run_id: &str,
) -> Result<Vec<AgentEvent>, Error> {
    let start = event_prefix(run_id);
    let end = prolly::prefix_end(&start);
    prolly
        .range(tree, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            decode_event(&bytes)
        })
        .collect()
}

fn main() -> Result<(), Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let run_id = "run-2026-07-01-0001";
    let base = prolly.create();
    let memory = prolly.put(
        &base,
        memory_key(run_id, "user/preference/storage"),
        b"Prefers local-first durable storage".to_vec(),
    )?;

    let events = vec![
        AgentEvent {
            run_id: run_id.to_string(),
            actor: "user".to_string(),
            timestamp_millis: 1_783_036_800_000,
            sequence: 0,
            kind: AgentEventKind::UserMessage {
                content: "Please make memory durable.".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            actor: "assistant".to_string(),
            timestamp_millis: 1_783_036_801_000,
            sequence: 1,
            kind: AgentEventKind::AssistantMessage {
                content: "I will inspect the current storage layer.".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            actor: "assistant".to_string(),
            timestamp_millis: 1_783_036_802_000,
            sequence: 2,
            kind: AgentEventKind::ToolCall {
                call_id: "tool-1".to_string(),
                tool: "search".to_string(),
                arguments_json: "{\"query\":\"memory storage\"}".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            actor: "tool".to_string(),
            timestamp_millis: 1_783_036_803_000,
            sequence: 3,
            kind: AgentEventKind::ToolResult {
                call_id: "tool-1".to_string(),
                output: "Found existing named roots and GC helpers.".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            actor: "assistant".to_string(),
            timestamp_millis: 1_783_036_804_000,
            sequence: 4,
            kind: AgentEventKind::MemoryWrite {
                key: memory_key(run_id, "user/preference/storage"),
                value: "Prefers local-first durable storage".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            actor: "assistant".to_string(),
            timestamp_millis: 1_783_036_805_000,
            sequence: 5,
            kind: AgentEventKind::Checkpoint {
                label: "accepted-memory".to_string(),
                root: memory.clone(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            actor: "assistant".to_string(),
            timestamp_millis: 1_783_036_806_000,
            sequence: 6,
            kind: AgentEventKind::SummaryCompaction {
                first_sequence: 0,
                last_sequence: 3,
                summary: "User asked for durable memory; assistant inspected storage.".to_string(),
            },
        },
    ];

    let log = append_events(&prolly, &base, &events)?;
    prolly.publish_named_root(&root_name(run_id, "events/current"), &log)?;

    let loaded = load_events(&prolly, &log, run_id)?;
    assert_eq!(loaded, events);
    assert!(matches!(
        loaded[2].kind,
        AgentEventKind::ToolCall { ref tool, .. } if tool == "search"
    ));
    assert!(matches!(
        loaded[5].kind,
        AgentEventKind::Checkpoint { ref root, .. } if root == &memory
    ));
    assert_eq!(
        prolly.get(&memory, &memory_key(run_id, "user/preference/storage"))?,
        Some(b"Prefers local-first durable storage".to_vec())
    );

    println!("loaded {} ordered events for {run_id}", loaded.len());
    Ok(())
}
