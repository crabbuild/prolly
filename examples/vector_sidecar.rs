use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use prolly::{
    prefix_range, Config, Error, KeyBuilder, MemStore, NamedRootUpdate, Prolly, Tree,
    VersionedValue,
};
use serde::{Deserialize, Serialize};

const CHUNK_SCHEMA: &str = "rag.vector.chunk";
const ANSWER_SCHEMA: &str = "rag.vector.answer";
const SCHEMA_VERSION: u64 = 1;
const EMBEDDING_MODEL: &str = "toy-embedding@1";
const EMBEDDING_DIMENSIONS: u32 = 3;

#[derive(Clone, Debug)]
struct ChunkInput {
    doc_id: String,
    chunk_id: String,
    source_uri: String,
    parser_version: String,
    text: String,
    embedding: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ChunkMetadata {
    corpus_id: String,
    doc_id: String,
    chunk_id: String,
    source_uri: String,
    parser_version: String,
    text: String,
    vector_id: String,
    embedding_model: String,
    embedding_dimensions: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Citation {
    doc_id: String,
    chunk_id: String,
    source_uri: String,
    vector_id: String,
    score_millis: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AnswerRecord {
    query: String,
    embedding_model: String,
    index_snapshot: Tree,
    citations: Vec<Citation>,
    answer: String,
}

#[derive(Clone, Debug)]
struct RetrievedChunk {
    metadata: ChunkMetadata,
    score: f32,
}

#[derive(Default)]
struct VectorSidecar {
    vectors: HashMap<String, Vec<f32>>,
}

impl VectorSidecar {
    fn upsert(&mut self, vector_id: String, embedding: Vec<f32>) {
        assert_eq!(embedding.len(), EMBEDDING_DIMENSIONS as usize);
        self.vectors.insert(vector_id, embedding);
    }

    fn search_filtered(
        &self,
        query_embedding: &[f32],
        allowed_vector_ids: &HashSet<String>,
        limit: usize,
    ) -> Vec<(String, f32)> {
        assert_eq!(query_embedding.len(), EMBEDDING_DIMENSIONS as usize);

        let mut scored = self
            .vectors
            .iter()
            .filter(|(vector_id, _)| allowed_vector_ids.contains(vector_id.as_str()))
            .map(|(vector_id, embedding)| {
                (
                    vector_id.clone(),
                    cosine_similarity(query_embedding, embedding),
                )
            })
            .collect::<Vec<_>>();

        scored.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.0.cmp(&right.0))
        });
        scored.truncate(limit);
        scored
    }
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;

    for (left, right) in left.iter().zip(right) {
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }

    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }

    dot / left_norm.sqrt() / right_norm.sqrt()
}

fn chunk_prefix(corpus_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("vector-sidecar")
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
        .push_str("vector-sidecar")
        .push_str("corpus")
        .push_str(corpus_id)
        .push_str("root")
        .push_str(name)
        .finish()
}

fn answer_key(answer_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("vector-sidecar")
        .push_str("answer")
        .push_str(answer_id)
        .finish()
}

fn vector_id(corpus_id: &str, doc_id: &str, chunk_id: &str) -> String {
    format!("{corpus_id}:{EMBEDDING_MODEL}:{doc_id}:{chunk_id}")
}

fn encode_chunk(metadata: &ChunkMetadata) -> Result<Vec<u8>, Error> {
    VersionedValue::json(CHUNK_SCHEMA, SCHEMA_VERSION, metadata)?.to_bytes()
}

fn decode_chunk(bytes: &[u8]) -> Result<ChunkMetadata, Error> {
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
    prolly: &Prolly<MemStore>,
    sidecar: &mut VectorSidecar,
    tree: &Tree,
    corpus_id: &str,
    input: ChunkInput,
) -> Result<Tree, Error> {
    let vector_id = vector_id(corpus_id, &input.doc_id, &input.chunk_id);
    sidecar.upsert(vector_id.clone(), input.embedding);

    let metadata = ChunkMetadata {
        corpus_id: corpus_id.to_string(),
        doc_id: input.doc_id,
        chunk_id: input.chunk_id,
        source_uri: input.source_uri,
        parser_version: input.parser_version,
        text: input.text,
        vector_id,
        embedding_model: EMBEDDING_MODEL.to_string(),
        embedding_dimensions: EMBEDDING_DIMENSIONS,
    };

    prolly.put(
        tree,
        chunk_key(corpus_id, &metadata.doc_id, &metadata.chunk_id),
        encode_chunk(&metadata)?,
    )
}

fn metadata_by_vector_id(
    prolly: &Prolly<MemStore>,
    index: &Tree,
    corpus_id: &str,
) -> Result<HashMap<String, ChunkMetadata>, Error> {
    let (start, end) = prefix_range(chunk_prefix(corpus_id));
    prolly
        .range(index, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            let metadata = decode_chunk(&bytes)?;
            Ok((metadata.vector_id.clone(), metadata))
        })
        .collect()
}

fn retrieve_from_snapshot(
    prolly: &Prolly<MemStore>,
    sidecar: &VectorSidecar,
    index_snapshot: &Tree,
    corpus_id: &str,
    query_embedding: &[f32],
    limit: usize,
) -> Result<Vec<RetrievedChunk>, Error> {
    let metadata = metadata_by_vector_id(prolly, index_snapshot, corpus_id)?;
    let allowed_vector_ids = metadata.keys().cloned().collect::<HashSet<_>>();
    let hits = sidecar.search_filtered(query_embedding, &allowed_vector_ids, limit);

    Ok(hits
        .into_iter()
        .filter_map(|(vector_id, score)| {
            metadata
                .get(&vector_id)
                .cloned()
                .map(|metadata| RetrievedChunk { metadata, score })
        })
        .collect())
}

fn synthesize_answer(
    query: &str,
    index_snapshot: &Tree,
    retrieved: &[RetrievedChunk],
) -> AnswerRecord {
    let citations = retrieved
        .iter()
        .map(|chunk| Citation {
            doc_id: chunk.metadata.doc_id.clone(),
            chunk_id: chunk.metadata.chunk_id.clone(),
            source_uri: chunk.metadata.source_uri.clone(),
            vector_id: chunk.metadata.vector_id.clone(),
            score_millis: (chunk.score.clamp(0.0, 1.0) * 1000.0).round() as u32,
        })
        .collect();

    let answer = retrieved
        .iter()
        .map(|chunk| chunk.metadata.text.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    AnswerRecord {
        query: query.to_string(),
        embedding_model: EMBEDDING_MODEL.to_string(),
        index_snapshot: index_snapshot.clone(),
        citations,
        answer,
    }
}

fn answer_from_snapshot(
    prolly: &Prolly<MemStore>,
    sidecar: &VectorSidecar,
    index_snapshot: &Tree,
    corpus_id: &str,
    query: &str,
    query_embedding: &[f32],
) -> Result<AnswerRecord, Error> {
    let retrieved = retrieve_from_snapshot(
        prolly,
        sidecar,
        index_snapshot,
        corpus_id,
        query_embedding,
        2,
    )?;
    Ok(synthesize_answer(query, index_snapshot, &retrieved))
}

fn main() -> Result<(), Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let mut sidecar = VectorSidecar::default();
    let corpus_id = "docs";
    let current_index_name = root_name(corpus_id, "chunks/current");

    let index_v1 = put_chunk(
        &prolly,
        &mut sidecar,
        &prolly.create(),
        corpus_id,
        ChunkInput {
            doc_id: "roots".to_string(),
            chunk_id: "0001".to_string(),
            source_uri: "docs://prolly-map/roots".to_string(),
            parser_version: "markdown-parser@1".to_string(),
            text: "Prolly roots pin the exact RAG metadata snapshot used for retrieval."
                .to_string(),
            embedding: vec![0.90, 0.10, 0.0],
        },
    )?;
    let index_v1 = put_chunk(
        &prolly,
        &mut sidecar,
        &index_v1,
        corpus_id,
        ChunkInput {
            doc_id: "sync".to_string(),
            chunk_id: "0001".to_string(),
            source_uri: "docs://prolly-map/sync".to_string(),
            parser_version: "markdown-parser@1".to_string(),
            text: "Missing-node sync copies content-addressed tree nodes between peers."
                .to_string(),
            embedding: vec![0.70, 0.20, 0.0],
        },
    )?;
    let index_v1 = put_chunk(
        &prolly,
        &mut sidecar,
        &index_v1,
        corpus_id,
        ChunkInput {
            doc_id: "sidecars".to_string(),
            chunk_id: "0001".to_string(),
            source_uri: "docs://prolly-map/vector-sidecars".to_string(),
            parser_version: "markdown-parser@1".to_string(),
            text: "A vector database can score embeddings while prolly stores provenance."
                .to_string(),
            embedding: vec![0.0, 1.0, 0.0],
        },
    )?;

    let update = prolly.compare_and_swap_named_root(&current_index_name, None, Some(&index_v1))?;
    assert!(matches!(update, NamedRootUpdate::Applied));

    let query = "How do vector sidecars stay reproducible?";
    let query_embedding = [1.0, 0.0, 0.0];
    let query_id = "answer-0001";
    let index_snapshot = prolly
        .load_named_root(&current_index_name)?
        .expect("current index exists");
    let answer = answer_from_snapshot(
        &prolly,
        &sidecar,
        &index_snapshot,
        corpus_id,
        query,
        &query_embedding,
    )?;

    let answers = prolly.put(
        &prolly.create(),
        answer_key(query_id),
        encode_answer(&answer)?,
    )?;
    prolly.publish_named_root(&root_name(corpus_id, "answers"), &answers)?;

    let index_v2 = put_chunk(
        &prolly,
        &mut sidecar,
        &index_v1,
        corpus_id,
        ChunkInput {
            doc_id: "newer-parser".to_string(),
            chunk_id: "0001".to_string(),
            source_uri: "docs://prolly-map/new-parser".to_string(),
            parser_version: "markdown-parser@2".to_string(),
            text: "A newer sidecar vector may rank highly but should not change old answers."
                .to_string(),
            embedding: vec![1.0, 0.0, 0.0],
        },
    )?;
    let update = prolly.compare_and_swap_named_root(
        &current_index_name,
        Some(&index_v1),
        Some(&index_v2),
    )?;
    assert!(matches!(update, NamedRootUpdate::Applied));

    let current_answer = answer_from_snapshot(
        &prolly,
        &sidecar,
        &index_v2,
        corpus_id,
        query,
        &query_embedding,
    )?;
    assert!(current_answer
        .citations
        .iter()
        .any(|citation| citation.doc_id == "newer-parser"));

    let stored_answer_bytes = prolly
        .get(&answers, &answer_key(query_id))?
        .expect("answer record exists");
    let stored_answer = decode_answer(&stored_answer_bytes)?;
    let replayed = answer_from_snapshot(
        &prolly,
        &sidecar,
        &stored_answer.index_snapshot,
        corpus_id,
        &stored_answer.query,
        &query_embedding,
    )?;

    assert_eq!(replayed, stored_answer);
    assert!(replayed
        .citations
        .iter()
        .all(|citation| citation.doc_id != "newer-parser"));

    let update = prolly.compare_and_swap_named_root(
        &current_index_name,
        Some(&index_v2),
        Some(&stored_answer.index_snapshot),
    )?;
    assert!(matches!(update, NamedRootUpdate::Applied));
    assert_eq!(
        prolly.load_named_root(&current_index_name)?,
        Some(stored_answer.index_snapshot.clone())
    );

    println!(
        "replayed {} citations from prolly root {:?} while sidecar held {} vectors",
        replayed.citations.len(),
        replayed.index_snapshot.root,
        sidecar.vectors.len()
    );
    Ok(())
}
