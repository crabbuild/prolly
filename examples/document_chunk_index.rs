use std::collections::HashMap;

use prolly::{
    prefix_range, Config, Error, KeyBuilder, LargeValueConfig, MemBlobStore, MemStore, Prolly,
    ValueRef, VersionedValue,
};
use serde::{Deserialize, Serialize};

const DOCUMENT_SCHEMA: &str = "rag.document";
const CHUNK_SCHEMA: &str = "rag.document.chunk";
const SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct DocumentMetadata {
    doc_id: String,
    source_uri: String,
    content_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ChunkMetadata {
    doc_id: String,
    chunk_id: String,
    parser_version: String,
    start_byte: u64,
    end_byte: u64,
    text_key: Vec<u8>,
    vector_id: String,
}

#[derive(Clone, Debug)]
struct ChunkInput {
    chunk_id: String,
    start_byte: u64,
    end_byte: u64,
    text: String,
    embedding: Vec<f32>,
}

fn document_key(corpus_id: &str, doc_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("doc-index")
        .push_str(corpus_id)
        .push_str("document")
        .push_str(doc_id)
        .push_str("meta")
        .finish()
}

fn chunk_prefix(corpus_id: &str, parser_version: &str, doc_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("doc-index")
        .push_str(corpus_id)
        .push_str("parser")
        .push_str(parser_version)
        .push_str("document")
        .push_str(doc_id)
        .push_str("chunk")
        .finish()
}

fn chunk_metadata_key(
    corpus_id: &str,
    parser_version: &str,
    doc_id: &str,
    start_byte: u64,
) -> Vec<u8> {
    KeyBuilder::from_prefix(chunk_prefix(corpus_id, parser_version, doc_id))
        .push_u64(start_byte)
        .finish()
}

fn chunk_text_key(corpus_id: &str, parser_version: &str, doc_id: &str, chunk_id: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("doc-index")
        .push_str(corpus_id)
        .push_str("text")
        .push_str(parser_version)
        .push_str(doc_id)
        .push_str(chunk_id)
        .finish()
}

fn root_name(corpus_id: &str, name: &str) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("doc-index")
        .push_str(corpus_id)
        .push_str("root")
        .push_str(name)
        .finish()
}

fn vector_id(corpus_id: &str, parser_version: &str, doc_id: &str, chunk_id: &str) -> String {
    format!("{corpus_id}:{parser_version}:{doc_id}:{chunk_id}")
}

fn encode_document(metadata: &DocumentMetadata) -> Result<Vec<u8>, Error> {
    VersionedValue::json(DOCUMENT_SCHEMA, SCHEMA_VERSION, metadata)?.to_bytes()
}

fn encode_chunk(metadata: &ChunkMetadata) -> Result<Vec<u8>, Error> {
    VersionedValue::json(CHUNK_SCHEMA, SCHEMA_VERSION, metadata)?.to_bytes()
}

fn decode_chunk(bytes: &[u8]) -> Result<ChunkMetadata, Error> {
    let value = VersionedValue::from_bytes(bytes)?;
    value.require_schema(CHUNK_SCHEMA, SCHEMA_VERSION)?;
    value.decode_json()
}

fn ingest_document(
    prolly: &Prolly<MemStore>,
    blobs: &MemBlobStore,
    tree: &prolly::Tree,
    vector_sidecar: &mut HashMap<String, Vec<f32>>,
    document: DocumentMetadata,
    parser_version: &str,
    chunks: Vec<ChunkInput>,
) -> Result<prolly::Tree, Error> {
    let policy = LargeValueConfig::new(32);
    let mut tree = prolly.put(
        tree,
        document_key("main", &document.doc_id),
        encode_document(&document)?,
    )?;

    for chunk in chunks {
        let text_key = chunk_text_key("main", parser_version, &document.doc_id, &chunk.chunk_id);
        tree = prolly.put_large_value(
            blobs,
            &tree,
            text_key.clone(),
            chunk.text.into_bytes(),
            policy.clone(),
        )?;

        let vector_id = vector_id("main", parser_version, &document.doc_id, &chunk.chunk_id);
        vector_sidecar.insert(vector_id.clone(), chunk.embedding);

        let metadata = ChunkMetadata {
            doc_id: document.doc_id.clone(),
            chunk_id: chunk.chunk_id,
            parser_version: parser_version.to_string(),
            start_byte: chunk.start_byte,
            end_byte: chunk.end_byte,
            text_key,
            vector_id,
        };
        tree = prolly.put(
            &tree,
            chunk_metadata_key(
                "main",
                parser_version,
                &document.doc_id,
                metadata.start_byte,
            ),
            encode_chunk(&metadata)?,
        )?;
    }

    Ok(tree)
}

fn list_chunks_for_document(
    prolly: &Prolly<MemStore>,
    tree: &prolly::Tree,
    corpus_id: &str,
    parser_version: &str,
    doc_id: &str,
) -> Result<Vec<ChunkMetadata>, Error> {
    let (start, end) = prefix_range(chunk_prefix(corpus_id, parser_version, doc_id));
    prolly
        .range(tree, &start, end.as_deref())?
        .map(|entry| {
            let (_, bytes) = entry?;
            decode_chunk(&bytes)
        })
        .collect()
}

fn main() -> Result<(), Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let blobs = MemBlobStore::new();
    let mut vector_sidecar = HashMap::<String, Vec<f32>>::new();

    let document = DocumentMetadata {
        doc_id: "guide".to_string(),
        source_uri: "docs://prolly-map/guide".to_string(),
        content_sha256: "sha256:example".to_string(),
    };
    let chunks = vec![
        ChunkInput {
            chunk_id: "0001".to_string(),
            start_byte: 0,
            end_byte: 64,
            text: "Prolly trees provide immutable roots for versioned application state."
                .to_string(),
            embedding: vec![0.12, 0.74, 0.31],
        },
        ChunkInput {
            chunk_id: "0002".to_string(),
            start_byte: 65,
            end_byte: 142,
            text:
                "Large chunk text can live in content-addressed blobs while metadata stays indexed."
                    .to_string(),
            embedding: vec![0.08, 0.67, 0.44],
        },
    ];

    let tree = ingest_document(
        &prolly,
        &blobs,
        &prolly.create(),
        &mut vector_sidecar,
        document,
        "parser-v1",
        chunks,
    )?;
    prolly.publish_named_root(&root_name("main", "chunks/current"), &tree)?;

    let chunks = list_chunks_for_document(&prolly, &tree, "main", "parser-v1", "guide")?;
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].start_byte, 0);
    assert_eq!(chunks[1].start_byte, 65);

    let first_text = prolly
        .get_large_value(&blobs, &tree, &chunks[0].text_key)?
        .expect("chunk text exists");
    assert!(String::from_utf8_lossy(&first_text).contains("immutable roots"));

    let stored_text_ref = prolly
        .get_value_ref(&tree, &chunks[0].text_key)?
        .expect("stored text reference exists");
    assert!(matches!(stored_text_ref, ValueRef::Blob(_)));
    assert_eq!(vector_sidecar[&chunks[0].vector_id], vec![0.12, 0.74, 0.31]);

    println!(
        "indexed {} chunks, {} sidecar vectors, {} blobs",
        chunks.len(),
        vector_sidecar.len(),
        blobs.len().map_err(|err| Error::Store(Box::new(err)))?
    );
    Ok(())
}
