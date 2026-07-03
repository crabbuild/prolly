use std::sync::Arc;

use prolly::{
    prefix_range, Config, Error, KeyBuilder, MemStore, NamedRootUpdate, Prolly, Tree,
    VersionedValue,
};
use serde::{Deserialize, Serialize};

const CHUNK_SCHEMA: &str = "rag.chunk";
const ANSWER_SCHEMA: &str = "rag.answer";
const SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct DocumentChunk {
    doc_id: String,
    chunk_id: String,
    source_uri: String,
    parser_version: String,
    text: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Citation {
    doc_id: String,
    chunk_id: String,
    source_uri: String,
    parser_version: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AnswerRecord {
    query: String,
    answer: String,
    index_snapshot: Tree,
    citations: Vec<Citation>,
}

#[derive(Clone, Debug)]
struct RetrievedChunk {
    chunk: DocumentChunk,
    score: usize,
}

fn chunk_prefix(corpus_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("rag")
        .push_str("corpus")
        .push_str(corpus_id)
        .push_str("chunk")
        .finish()
}

fn chunk_key(corpus_id: &str, doc_id: &str, chunk_id: &str) -> Vec<u8> {
    KeyBuilder::from_prefix(chunk_prefix(corpus_id))
        .push_str(doc_id)
        .push_str(chunk_id)
        .finish()
}

fn root_name(corpus_id: &str, name: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("rag")
        .push_str("corpus")
        .push_str(corpus_id)
        .push_str("root")
        .push_str(name)
        .finish()
}

fn answer_key(query_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("rag")
        .push_str("answer")
        .push_str(query_id)
        .finish()
}

fn encode_chunk(chunk: &DocumentChunk) -> Result<Vec<u8>, Error> {
    VersionedValue::json(CHUNK_SCHEMA, SCHEMA_VERSION, chunk)?.to_bytes()
}

fn decode_chunk(bytes: &[u8]) -> Result<DocumentChunk, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(CHUNK_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn encode_answer(answer: &AnswerRecord) -> Result<Vec<u8>, Error> {
    VersionedValue::json(ANSWER_SCHEMA, SCHEMA_VERSION, answer)?.to_bytes()
}

fn decode_answer(bytes: &[u8]) -> Result<AnswerRecord, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(ANSWER_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn put_chunk(
    prolly: &Prolly<Arc<MemStore>>,
    tree: &Tree,
    corpus_id: &str,
    chunk: DocumentChunk,
) -> Result<Tree, Error> {
    prolly.put(
        tree,
        chunk_key(corpus_id, &chunk.doc_id, &chunk.chunk_id),
        encode_chunk(&chunk)?,
    )
}

fn retrieve(
    prolly: &Prolly<Arc<MemStore>>,
    index: &Tree,
    corpus_id: &str,
    query: &str,
    limit: usize,
) -> Result<Vec<RetrievedChunk>, Error> {
    let terms = query_terms(query);
    let (start, end) = prefix_range(chunk_prefix(corpus_id));
    let mut matches = prolly
        .range(index, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            let chunk = decode_chunk(&bytes)?;
            let score = score_chunk(&terms, &chunk.text);
            Ok(RetrievedChunk { chunk, score })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    matches.retain(|retrieved| retrieved.score > 0);
    matches.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.chunk.doc_id.cmp(&right.chunk.doc_id))
            .then_with(|| left.chunk.chunk_id.cmp(&right.chunk.chunk_id))
    });
    matches.truncate(limit);
    Ok(matches)
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn score_chunk(terms: &[String], text: &str) -> usize {
    let text = text.to_ascii_lowercase();
    terms
        .iter()
        .filter(|term| text.contains(term.as_str()))
        .count()
}

fn synthesize_answer(
    query: &str,
    retrieved: &[RetrievedChunk],
    index_snapshot: &Tree,
) -> AnswerRecord {
    let answer = retrieved
        .iter()
        .map(|retrieved| retrieved.chunk.text.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let citations = retrieved
        .iter()
        .map(|retrieved| Citation {
            doc_id: retrieved.chunk.doc_id.clone(),
            chunk_id: retrieved.chunk.chunk_id.clone(),
            source_uri: retrieved.chunk.source_uri.clone(),
            parser_version: retrieved.chunk.parser_version.clone(),
        })
        .collect();

    AnswerRecord {
        query: query.to_string(),
        answer,
        index_snapshot: index_snapshot.clone(),
        citations,
    }
}

fn answer_from_snapshot(
    prolly: &Prolly<Arc<MemStore>>,
    index_snapshot: &Tree,
    corpus_id: &str,
    query: &str,
) -> Result<AnswerRecord, Error> {
    let retrieved = retrieve(prolly, index_snapshot, corpus_id, query, 2)?;
    Ok(synthesize_answer(query, &retrieved, index_snapshot))
}

fn main() -> Result<(), Error> {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, Config::default());
    let corpus_id = "docs";
    let current_index_name = root_name(corpus_id, "index/current");

    let empty = prolly.create();
    let index_v1 = put_chunk(
        &prolly,
        &empty,
        corpus_id,
        DocumentChunk {
            doc_id: "prolly-map".to_string(),
            chunk_id: "0001".to_string(),
            source_uri: "docs://prolly-map/intro".to_string(),
            parser_version: "parser-v1".to_string(),
            text: "prolly-map stores versioned map roots for deterministic retrieval.".to_string(),
        },
    )?;
    let index_v1 = put_chunk(
        &prolly,
        &index_v1,
        corpus_id,
        DocumentChunk {
            doc_id: "sync".to_string(),
            chunk_id: "0001".to_string(),
            source_uri: "docs://prolly-map/sync".to_string(),
            parser_version: "parser-v1".to_string(),
            text: "Recording the exact index root makes RAG answers reproducible.".to_string(),
        },
    )?;
    let index_v1 = put_chunk(
        &prolly,
        &index_v1,
        corpus_id,
        DocumentChunk {
            doc_id: "storage".to_string(),
            chunk_id: "0001".to_string(),
            source_uri: "docs://prolly-map/storage".to_string(),
            parser_version: "parser-v1".to_string(),
            text: "Object stores can sync missing content-addressed nodes.".to_string(),
        },
    )?;

    let update = prolly.compare_and_swap_named_root(&current_index_name, None, Some(&index_v1))?;
    assert!(matches!(update, NamedRootUpdate::Applied));

    let query = "How do versioned map roots make RAG reproducible?";
    let query_id = "answer-0001";
    let index_snapshot = prolly
        .load_named_root(&current_index_name)?
        .expect("current index exists");
    let answer = answer_from_snapshot(&prolly, &index_snapshot, corpus_id, query)?;

    let answers = prolly.put(
        &prolly.create(),
        answer_key(query_id),
        encode_answer(&answer)?,
    )?;
    prolly.publish_named_root(&root_name(corpus_id, "answers"), &answers)?;

    let bad_index = put_chunk(
        &prolly,
        &index_v1,
        corpus_id,
        DocumentChunk {
            doc_id: "prolly-map".to_string(),
            chunk_id: "0001".to_string(),
            source_uri: "docs://prolly-map/intro".to_string(),
            parser_version: "parser-bad".to_string(),
            text: "This bad parse dropped the retrieval facts.".to_string(),
        },
    )?;
    let update = prolly.compare_and_swap_named_root(
        &current_index_name,
        Some(&index_v1),
        Some(&bad_index),
    )?;
    assert!(matches!(update, NamedRootUpdate::Applied));

    let stored_answer_bytes = prolly
        .get(&answers, &answer_key(query_id))?
        .expect("answer record exists");
    let stored_answer = decode_answer(&stored_answer_bytes)?;
    let replayed = answer_from_snapshot(
        &prolly,
        &stored_answer.index_snapshot,
        corpus_id,
        &stored_answer.query,
    )?;
    assert_eq!(replayed, stored_answer);

    let update = prolly.compare_and_swap_named_root(
        &current_index_name,
        Some(&bad_index),
        Some(&stored_answer.index_snapshot),
    )?;
    assert!(matches!(update, NamedRootUpdate::Applied));
    assert_eq!(
        prolly.load_named_root(&current_index_name)?,
        Some(stored_answer.index_snapshot.clone())
    );

    println!(
        "replayed answer from index root {:?}: {}",
        stored_answer.index_snapshot.root, stored_answer.answer
    );
    Ok(())
}
