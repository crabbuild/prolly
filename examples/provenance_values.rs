use prolly::{prefix_range, Cid, Config, Error, KeyBuilder, MemStore, Prolly, VersionedValue};
use serde::{Deserialize, Serialize};

const CHUNK_SCHEMA: &str = "ai.provenance.chunk";
const CLAIM_SCHEMA: &str = "ai.provenance.claim";
const SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SourceRef {
    source_uri: String,
    file_id: String,
    source_cid: Cid,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PipelineRef {
    parser_version: String,
    embedding_model: String,
    embedding_dimensions: u32,
    summarizer_model: String,
    summarizer_prompt_version: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ChunkRecord {
    source: SourceRef,
    chunk_id: String,
    byte_range: (u64, u64),
    text: String,
    chunk_cid: Cid,
    pipeline: PipelineRef,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct DerivedClaim {
    subject: String,
    claim: String,
    confidence_millis: u32,
    source: SourceRef,
    parent_chunk_key: Vec<u8>,
    parent_chunk_cid: Cid,
    source_cids: Vec<Cid>,
    pipeline: PipelineRef,
}

fn chunk_key(file_id: &str, chunk_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("provenance")
        .push_str("chunk")
        .push_str(file_id)
        .push_str(chunk_id)
        .finish()
}

fn claim_prefix(file_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("provenance")
        .push_str("claim")
        .push_str(file_id)
        .finish()
}

fn claim_key(file_id: &str, claim_id: &str) -> Vec<u8> {
    KeyBuilder::from_prefix(claim_prefix(file_id))
        .push_str(claim_id)
        .finish()
}

fn encode_chunk(chunk: &ChunkRecord) -> Result<Vec<u8>, Error> {
    VersionedValue::json(CHUNK_SCHEMA, SCHEMA_VERSION, chunk)?.to_bytes()
}

fn decode_chunk(bytes: &[u8]) -> Result<ChunkRecord, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(CHUNK_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn encode_claim(claim: &DerivedClaim) -> Result<Vec<u8>, Error> {
    VersionedValue::json(CLAIM_SCHEMA, SCHEMA_VERSION, claim)?.to_bytes()
}

fn decode_claim(bytes: &[u8]) -> Result<DerivedClaim, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(CLAIM_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn main() -> Result<(), Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let source_bytes = b"# Guide\nprolly-map keeps immutable roots for auditable AI workflows.";
    let source = SourceRef {
        source_uri: "docs://prolly-map/guide.md".to_string(),
        file_id: "guide.md".to_string(),
        source_cid: Cid::from_bytes(source_bytes),
    };
    let pipeline = PipelineRef {
        parser_version: "markdown-parser@1.2.0".to_string(),
        embedding_model: "text-embedding-3-small".to_string(),
        embedding_dimensions: 1536,
        summarizer_model: "gpt-4.1-mini".to_string(),
        summarizer_prompt_version: "summary-prompt@2026-07-01".to_string(),
    };

    let chunk_text = "prolly-map keeps immutable roots for auditable AI workflows.";
    let chunk = ChunkRecord {
        source: source.clone(),
        chunk_id: "chunk-0001".to_string(),
        byte_range: (8, 68),
        text: chunk_text.to_string(),
        chunk_cid: Cid::from_bytes(chunk_text.as_bytes()),
        pipeline: pipeline.clone(),
    };
    let parent_chunk_key = chunk_key(&source.file_id, &chunk.chunk_id);

    let claim = DerivedClaim {
        subject: "prolly-map".to_string(),
        claim: "Immutable roots make AI workflow outputs auditable.".to_string(),
        confidence_millis: 910,
        source: source.clone(),
        parent_chunk_key: parent_chunk_key.clone(),
        parent_chunk_cid: chunk.chunk_cid.clone(),
        source_cids: vec![source.source_cid.clone(), chunk.chunk_cid.clone()],
        pipeline: pipeline.clone(),
    };

    let tree = prolly.create();
    let tree = prolly.put(&tree, parent_chunk_key.clone(), encode_chunk(&chunk)?)?;
    let tree = prolly.put(
        &tree,
        claim_key(&source.file_id, "claim-0001"),
        encode_claim(&claim)?,
    )?;
    prolly.publish_named_root(b"provenance/main", &tree)?;

    let stored_chunk = prolly
        .get(&tree, &parent_chunk_key)?
        .map(|bytes| decode_chunk(&bytes))
        .transpose()?
        .expect("chunk exists");
    assert_eq!(
        stored_chunk.chunk_cid,
        Cid::from_bytes(stored_chunk.text.as_bytes())
    );

    let (start, end) = prefix_range(claim_prefix(&source.file_id));
    let claims = prolly
        .range(&tree, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            decode_claim(&bytes)
        })
        .collect::<Result<Vec<_>, Error>>()?;

    assert_eq!(claims, vec![claim]);
    assert_eq!(claims[0].source.source_uri, "docs://prolly-map/guide.md");
    assert_eq!(claims[0].pipeline.parser_version, "markdown-parser@1.2.0");
    assert_eq!(claims[0].pipeline.embedding_dimensions, 1536);
    assert_eq!(claims[0].pipeline.summarizer_model, "gpt-4.1-mini");
    assert!(claims[0].source_cids.contains(&source.source_cid));
    assert!(claims[0].source_cids.contains(&stored_chunk.chunk_cid));

    println!(
        "stored {} provenance claim with {} source CIDs",
        claims.len(),
        claims[0].source_cids.len()
    );
    Ok(())
}
