use prolly::{
    prefix_range, Config, Error, KeyBuilder, MemStore, Mutation, NamedRootRetention, Prolly, Tree,
    VersionedValue,
};
use serde::{Deserialize, Serialize};

const EVENT_SCHEMA: &str = "ai.compaction.event";
const SUMMARY_SCHEMA: &str = "ai.compaction.summary_index";
const SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AgentEvent {
    run_id: String,
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
        tool: String,
    },
    ToolResult {
        output: String,
    },
    MemoryWrite {
        key: String,
        value: String,
    },
    SummaryCompaction {
        first_sequence: u64,
        last_sequence: u64,
        source_event_count: u64,
        summary: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SummaryIndexRecord {
    run_id: String,
    first_sequence: u64,
    last_sequence: u64,
    source_event_count: u64,
    summary: String,
}

fn event_prefix(run_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("compaction")
        .push_str("run")
        .push_str(run_id)
        .push_str("event")
        .finish()
}

fn event_key(run_id: &str, sequence: u64) -> Vec<u8> {
    KeyBuilder::from_prefix(event_prefix(run_id))
        .push_u64(sequence)
        .finish()
}

fn summary_index_prefix(run_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("compaction")
        .push_str("run")
        .push_str(run_id)
        .push_str("summary-index")
        .finish()
}

fn summary_index_key(run_id: &str, first_sequence: u64) -> Vec<u8> {
    KeyBuilder::from_prefix(summary_index_prefix(run_id))
        .push_u64(first_sequence)
        .finish()
}

fn root_name(run_id: &str, name: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("compaction")
        .push_str("run")
        .push_str(run_id)
        .push_str("root")
        .push_str(name)
        .finish()
}

fn encode_event(event: &AgentEvent) -> Result<Vec<u8>, Error> {
    VersionedValue::json(EVENT_SCHEMA, SCHEMA_VERSION, event)?.to_bytes()
}

fn decode_event(bytes: &[u8]) -> Result<AgentEvent, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(EVENT_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn encode_summary(record: &SummaryIndexRecord) -> Result<Vec<u8>, Error> {
    VersionedValue::json(SUMMARY_SCHEMA, SCHEMA_VERSION, record)?.to_bytes()
}

fn decode_summary(bytes: &[u8]) -> Result<SummaryIndexRecord, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(SUMMARY_SCHEMA, SCHEMA_VERSION)?;
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
                key: event_key(&event.run_id, event.sequence),
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
    let (start, end) = prefix_range(event_prefix(run_id));
    prolly
        .range(tree, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            decode_event(&bytes)
        })
        .collect()
}

fn compact_event_window(
    prolly: &Prolly<MemStore>,
    tree: &Tree,
    run_id: &str,
    first_sequence: u64,
    last_sequence: u64,
    compacted_sequence: u64,
    summary: &str,
) -> Result<Tree, Error> {
    let events = load_events(prolly, tree, run_id)?;
    let compacted = events
        .iter()
        .filter(|event| event.sequence >= first_sequence && event.sequence <= last_sequence)
        .count();
    assert!(compacted > 0);

    let mut mutations = (first_sequence..=last_sequence)
        .map(|sequence| Mutation::Delete {
            key: event_key(run_id, sequence),
        })
        .collect::<Vec<_>>();
    mutations.push(Mutation::Upsert {
        key: event_key(run_id, compacted_sequence),
        val: encode_event(&AgentEvent {
            run_id: run_id.to_string(),
            sequence: compacted_sequence,
            kind: AgentEventKind::SummaryCompaction {
                first_sequence,
                last_sequence,
                source_event_count: compacted as u64,
                summary: summary.to_string(),
            },
        })?,
    });

    prolly.batch(tree, mutations)
}

fn rebuild_summary_index(
    prolly: &Prolly<MemStore>,
    canonical_log: &Tree,
    run_id: &str,
) -> Result<Tree, Error> {
    let summaries = load_events(prolly, canonical_log, run_id)?
        .into_iter()
        .filter_map(|event| match event.kind {
            AgentEventKind::SummaryCompaction {
                first_sequence,
                last_sequence,
                source_event_count,
                summary,
            } => Some(SummaryIndexRecord {
                run_id: event.run_id,
                first_sequence,
                last_sequence,
                source_event_count,
                summary,
            }),
            _ => None,
        })
        .map(|record| {
            Ok(Mutation::Upsert {
                key: summary_index_key(run_id, record.first_sequence),
                val: encode_summary(&record)?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    prolly.batch(&prolly.create(), summaries)
}

fn load_summaries(
    prolly: &Prolly<MemStore>,
    index: &Tree,
    run_id: &str,
) -> Result<Vec<SummaryIndexRecord>, Error> {
    let (start, end) = prefix_range(summary_index_prefix(run_id));
    prolly
        .range(index, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            decode_summary(&bytes)
        })
        .collect()
}

fn main() -> Result<(), Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let run_id = "run-compact-0001";

    let initial_events = vec![
        AgentEvent {
            run_id: run_id.to_string(),
            sequence: 0,
            kind: AgentEventKind::UserMessage {
                content: "Summarize the codebase storage plan.".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            sequence: 1,
            kind: AgentEventKind::AssistantMessage {
                content: "I will inspect the index and manifest APIs.".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            sequence: 2,
            kind: AgentEventKind::ToolCall {
                tool: "rg".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            sequence: 3,
            kind: AgentEventKind::ToolResult {
                output: "Found named roots, retention, and GC helpers.".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            sequence: 4,
            kind: AgentEventKind::MemoryWrite {
                key: "storage/plan".to_string(),
                value: "Use named roots and retention-aware GC.".to_string(),
            },
        },
        AgentEvent {
            run_id: run_id.to_string(),
            sequence: 5,
            kind: AgentEventKind::AssistantMessage {
                content: "The plan is ready.".to_string(),
            },
        },
    ];

    let log_v1 = append_events(&prolly, &prolly.create(), &initial_events)?;
    prolly.publish_named_root_at_millis(&root_name(run_id, "events/0001"), &log_v1, 1_000)?;

    let scratch = prolly.put(
        &log_v1,
        b"scratch/unpublished".to_vec(),
        b"temporary rewrite".to_vec(),
    )?;
    assert_ne!(scratch, log_v1);

    let compacted_log = compact_event_window(
        &prolly,
        &log_v1,
        run_id,
        0,
        3,
        10,
        "User asked for a storage plan; assistant inspected roots, retention, and GC.",
    )?;
    prolly.publish_named_root_at_millis(
        &root_name(run_id, "events/0002"),
        &compacted_log,
        2_000,
    )?;
    prolly.publish_named_root_at_millis(
        &root_name(run_id, "events/current"),
        &compacted_log,
        2_000,
    )?;

    let summary_index = rebuild_summary_index(&prolly, &compacted_log, run_id)?;
    prolly.publish_named_root_at_millis(
        &root_name(run_id, "summary-index/current"),
        &summary_index,
        2_100,
    )?;

    let compacted_events = load_events(&prolly, &compacted_log, run_id)?;
    assert_eq!(compacted_events.len(), 3);
    assert!(compacted_events.iter().all(|event| event.sequence >= 4));
    assert!(matches!(
        compacted_events.last().map(|event| &event.kind),
        Some(AgentEventKind::SummaryCompaction {
            first_sequence: 0,
            last_sequence: 3,
            source_event_count: 4,
            ..
        })
    ));

    let summaries = load_summaries(&prolly, &summary_index, run_id)?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].first_sequence, 0);
    assert_eq!(summaries[0].last_sequence, 3);
    assert_eq!(summaries[0].source_event_count, 4);

    let retention = NamedRootRetention::exact(vec![
        root_name(run_id, "events/0001"),
        root_name(run_id, "events/current"),
        root_name(run_id, "summary-index/current"),
    ]);
    let retained = prolly.load_retained_named_roots(&retention)?;
    assert!(retained.is_complete());
    assert_eq!(retained.roots.len(), 3);

    let gc_plan = prolly.plan_store_gc_for_retention(&retention)?;
    assert!(gc_plan.reclaimable_nodes > 0);
    let sweep = prolly.sweep_store_gc_for_retention(&retention)?;
    assert_eq!(sweep.deleted_nodes, gc_plan.reclaimable_nodes);

    assert_eq!(
        prolly.load_named_root(&root_name(run_id, "events/current"))?,
        Some(compacted_log)
    );
    assert_eq!(
        prolly.load_named_root(&root_name(run_id, "summary-index/current"))?,
        Some(summary_index)
    );

    println!(
        "compacted {} source events into {} summary row; retained {} roots; swept {} nodes",
        4,
        summaries.len(),
        retained.roots.len(),
        sweep.deleted_nodes
    );
    Ok(())
}
