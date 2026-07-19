use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use prolly::{
    load_named_content_root, put_named_content_root, ContentObjectKind, ContentRootManifest,
    DistanceMetric, ProximityConfig, ProximityFilter, ProximityMap, ProximityRecord, SearchPolicy,
    SearchRequest, TypedContentRoot,
};
use prolly_store_sqlite::{SqliteStore, SqliteStoreConfig};
use serde::{Deserialize, Serialize};

const DIMENSIONS: usize = 1_536;
const CORPUS_PREFIX: &[u8] = b"tenant/acme/docs/";
const ROOT_NAME: &[u8] = b"rag/corpus/main";

type AppResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Debug, Deserialize)]
struct Fixture {
    embedding_model: String,
    dimensions: usize,
    corpus_version: u64,
    chunks: Vec<ChunkFixture>,
    queries: Vec<QueryFixture>,
}

#[derive(Clone, Debug, Deserialize)]
struct ChunkFixture {
    key: String,
    title: String,
    section: String,
    source: String,
    text: String,
    embedding: Vec<f32>,
}

#[derive(Clone, Debug, Deserialize)]
struct QueryFixture {
    name: String,
    prompt: String,
    embedding: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ChunkMetadata {
    title: String,
    section: String,
    source: String,
    text: String,
}

#[derive(Clone, Debug, PartialEq)]
struct RetrievedChunk {
    key: Vec<u8>,
    distance: f64,
    metadata: ChunkMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CorpusState {
    Built,
    Reopened,
}

fn load_fixture() -> AppResult<Fixture> {
    Ok(serde_json::from_str(include_str!(
        "data/semantic_rag_embeddings.json"
    ))?)
}

fn validate_fixture(fixture: &Fixture) -> AppResult<()> {
    if fixture.embedding_model.trim().is_empty() {
        return Err("embedding model identifier must not be empty".into());
    }
    if fixture.dimensions != DIMENSIONS {
        return Err(format!(
            "fixture must declare {DIMENSIONS} dimensions, found {}",
            fixture.dimensions
        )
        .into());
    }
    if fixture.corpus_version != 1 {
        return Err(format!(
            "fixture corpus version must be 1, found {}",
            fixture.corpus_version
        )
        .into());
    }
    if fixture.chunks.is_empty() || fixture.queries.is_empty() {
        return Err("fixture must contain chunks and queries".into());
    }

    let mut keys = HashSet::new();
    for chunk in &fixture.chunks {
        if !keys.insert(chunk.key.as_str()) {
            return Err(format!("duplicate chunk key: {}", chunk.key).into());
        }
        if !chunk.key.starts_with("tenant/acme/docs/") {
            return Err(format!("chunk key is outside the corpus prefix: {}", chunk.key).into());
        }
        if [
            chunk.title.as_str(),
            chunk.section.as_str(),
            chunk.source.as_str(),
            chunk.text.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err(format!("chunk {} has empty metadata", chunk.key).into());
        }
        validate_vector(
            &format!("chunk {}", chunk.key),
            &chunk.embedding,
            DIMENSIONS,
        )?;
    }

    let mut names = HashSet::new();
    for query in &fixture.queries {
        if !names.insert(query.name.as_str()) {
            return Err(format!("duplicate query name: {}", query.name).into());
        }
        if query.prompt.trim().is_empty() {
            return Err(format!("query {} has an empty prompt", query.name).into());
        }
        validate_vector(
            &format!("query {}", query.name),
            &query.embedding,
            DIMENSIONS,
        )?;
    }
    Ok(())
}

fn validate_vector(label: &str, vector: &[f32], dimensions: usize) -> AppResult<()> {
    if vector.len() != dimensions {
        return Err(format!(
            "{label} must contain {dimensions} dimensions, found {}",
            vector.len()
        )
        .into());
    }
    if vector.iter().any(|component| !component.is_finite()) {
        return Err(format!("{label} contains a non-finite component").into());
    }
    let norm = vector
        .iter()
        .map(|value| f64::from(*value).powi(2))
        .sum::<f64>()
        .sqrt();
    if (norm - 1.0).abs() > 1e-4 {
        return Err(format!("{label} must be unit-normalized; norm={norm}").into());
    }
    Ok(())
}

fn embedding_for_query<'a>(fixture: &'a Fixture, name: &str) -> AppResult<&'a [f32]> {
    // Offline integration seam: production applications replace this lookup
    // with their embedding provider and retain the same dimension/model checks.
    Ok(query_for_name(fixture, name)?.embedding.as_slice())
}

fn query_for_name<'a>(fixture: &'a Fixture, name: &str) -> AppResult<&'a QueryFixture> {
    fixture
        .queries
        .iter()
        .find(|query| query.name == name)
        .ok_or_else(|| {
            let supported = fixture
                .queries
                .iter()
                .map(|query| query.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("unknown query '{name}'; supported queries: {supported}").into()
        })
}

fn build_map(
    store: Arc<SqliteStore>,
    fixture: &Fixture,
) -> AppResult<ProximityMap<Arc<SqliteStore>>> {
    validate_fixture(fixture)?;
    let records = fixture
        .chunks
        .iter()
        .map(|chunk| {
            let metadata = ChunkMetadata {
                title: chunk.title.clone(),
                section: chunk.section.clone(),
                source: chunk.source.clone(),
                text: chunk.text.clone(),
            };
            Ok(ProximityRecord {
                key: chunk.key.as_bytes().to_vec(),
                vector: chunk.embedding.clone(),
                value: serde_json::to_vec(&metadata)?,
            })
        })
        .collect::<AppResult<Vec<_>>>()?;

    let mut config = ProximityConfig::new(fixture.dimensions as u32);
    config.metric = DistanceMetric::Cosine;
    config.hierarchy.level_hash_seed = 42;
    config.overflow.max_page_bytes = 256 * 1024;
    config.vector_storage.inline_threshold_bytes = 4 * 1024;
    let map = ProximityMap::build(store, config, records)?;
    map.verify()?;
    Ok(map)
}

fn retrieve(
    map: &ProximityMap<Arc<SqliteStore>>,
    fixture: &Fixture,
    query_name: &str,
) -> AppResult<Vec<RetrievedChunk>> {
    let query_embedding = embedding_for_query(fixture, query_name)?;
    let mut request = SearchRequest::exact(query_embedding, 3);
    request.policy = SearchPolicy::Exact;
    request.filter = ProximityFilter::Prefix(CORPUS_PREFIX);
    let result = map.search(request)?;

    result
        .neighbors
        .into_iter()
        .map(|neighbor| {
            Ok(RetrievedChunk {
                key: neighbor.key,
                distance: neighbor.distance,
                metadata: serde_json::from_slice(&neighbor.value)?,
            })
        })
        .collect()
}

fn render_context(hits: &[RetrievedChunk]) -> String {
    let mut context = String::from("<context>\n");
    for (index, hit) in hits.iter().enumerate() {
        context.push_str(&format!(
            "[{}] {}\nTitle: {}\nSource: {}\n{}\n",
            index + 1,
            hit.metadata.section,
            hit.metadata.title,
            hit.metadata.source,
            hit.metadata.text
        ));
        if index + 1 != hits.len() {
            context.push('\n');
        }
    }
    context.push_str("</context>");
    context
}

fn durable_store(path: &Path) -> AppResult<Arc<SqliteStore>> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    Ok(Arc::new(SqliteStore::open_with_config(
        path,
        SqliteStoreConfig {
            busy_timeout_ms: 5_000,
            enable_wal: true,
            persist_wal: false,
            synchronous_normal: false,
        },
    )?))
}

fn open_or_build_corpus(
    store: Arc<SqliteStore>,
    fixture: &Fixture,
) -> AppResult<(ProximityMap<Arc<SqliteStore>>, CorpusState)> {
    validate_fixture(fixture)?;
    if let Some(publication) = load_named_content_root(store.as_ref(), ROOT_NAME)? {
        validate_manifest(&publication.manifest, fixture)?;
        let map = ProximityMap::load(store, publication.manifest.root.cid)?;
        map.verify()?;
        return Ok((map, CorpusState::Reopened));
    }

    let map = build_map(store.clone(), fixture)?;
    let manifest = ContentRootManifest {
        root: TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone()),
        logical_version: fixture.corpus_version,
        created_at_millis: 0,
        metadata: expected_manifest_metadata(fixture),
    };
    put_named_content_root(store.as_ref(), ROOT_NAME, manifest)?;
    Ok((map, CorpusState::Built))
}

fn expected_manifest_metadata(fixture: &Fixture) -> BTreeMap<Vec<u8>, Vec<u8>> {
    BTreeMap::from([
        (
            b"corpus-version".to_vec(),
            fixture.corpus_version.to_string().into_bytes(),
        ),
        (
            b"embedding-model".to_vec(),
            fixture.embedding_model.as_bytes().to_vec(),
        ),
        (
            b"dimensions".to_vec(),
            fixture.dimensions.to_string().into_bytes(),
        ),
    ])
}

fn validate_manifest(manifest: &ContentRootManifest, fixture: &Fixture) -> AppResult<()> {
    if manifest.root.kind != ContentObjectKind::ProximityDescriptor {
        return Err(format!(
            "named root has kind {:?}, expected ProximityDescriptor",
            manifest.root.kind
        )
        .into());
    }
    if manifest.root.dimensions.is_some() {
        return Err(format!(
            "descriptor root must not carry PRXN dimension context, found {:?}",
            manifest.root.dimensions
        )
        .into());
    }
    if manifest.logical_version != fixture.corpus_version {
        return Err(format!(
            "named root corpus version {} does not match fixture version {}",
            manifest.logical_version, fixture.corpus_version
        )
        .into());
    }
    let expected = expected_manifest_metadata(fixture);
    if manifest.metadata != expected {
        return Err("named root embedding metadata does not match the fixture".into());
    }
    Ok(())
}

fn excerpt(text: &str, maximum_chars: usize) -> String {
    let mut chars = text.chars();
    let excerpt: String = chars.by_ref().take(maximum_chars).collect();
    if chars.next().is_some() {
        format!("{excerpt}…")
    } else {
        excerpt
    }
}

fn cid_hex(cid: &prolly::Cid) -> String {
    cid.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
fn temp_db_path(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "prolly-{label}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

#[cfg(test)]
fn remove_sqlite_files(path: &Path) {
    for candidate in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        let _ = fs::remove_file(candidate);
    }
}

fn main() -> AppResult<()> {
    let fixture = load_fixture()?;
    validate_fixture(&fixture)?;

    let mut arguments = std::env::args();
    let program = arguments
        .next()
        .unwrap_or_else(|| "semantic_rag".to_string());
    let database = arguments.next();
    let query_name = arguments.next();
    if database.is_none() || query_name.is_none() || arguments.next().is_some() {
        let supported = fixture
            .queries
            .iter()
            .map(|query| query.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "usage: {program} <database-path> <query-name>\nsupported queries: {supported}"
        )
        .into());
    }
    let database = PathBuf::from(database.expect("validated database argument"));
    let query_name = query_name.expect("validated query argument");
    let query = query_for_name(&fixture, &query_name)?;
    let store = durable_store(&database)?;
    let (map, state) = open_or_build_corpus(store, &fixture)?;
    let hits = retrieve(&map, &fixture, &query_name)?;

    println!(
        "Corpus: {} {}",
        match state {
            CorpusState::Built => "built",
            CorpusState::Reopened => "reopened",
        },
        database.display()
    );
    println!("Descriptor: {}", cid_hex(&map.tree().descriptor));
    println!("Query: {} ({})", query.prompt, query.name);
    println!("Retrieved {} chunks:", hits.len());
    for (index, hit) in hits.iter().enumerate() {
        println!(
            "{}. {} — {}\n   cosine distance: {:.6}\n   key: {}\n   source: {}\n   excerpt: {}",
            index + 1,
            hit.metadata.title,
            hit.metadata.section,
            hit.distance,
            String::from_utf8_lossy(&hit.key),
            hit.metadata.source,
            excerpt(&hit.metadata.text, 96)
        );
    }
    println!("\n{}", render_context(&hits));
    println!("\nAnswer generation omitted: pass this context to your LLM.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_is_1536_dimensional_finite_normalized_and_unique() {
        let fixture = load_fixture().unwrap();
        validate_fixture(&fixture).unwrap();
        assert_eq!(fixture.dimensions, DIMENSIONS);
        assert_eq!(fixture.chunks.len(), 6);
    }

    #[test]
    fn validation_rejects_wrong_dimensions_and_non_unit_vectors() {
        let mut fixture = load_fixture().unwrap();
        fixture.queries[0].embedding.pop();
        assert!(validate_fixture(&fixture)
            .unwrap_err()
            .to_string()
            .contains("1536"));

        let mut fixture = load_fixture().unwrap();
        fixture.queries[0].embedding.fill(0.0);
        assert!(validate_fixture(&fixture)
            .unwrap_err()
            .to_string()
            .contains("unit-normalized"));
    }

    #[test]
    fn unknown_query_lists_supported_names() {
        let fixture = load_fixture().unwrap();
        let error = embedding_for_query(&fixture, "unknown")
            .unwrap_err()
            .to_string();
        assert!(error.contains("password-reset"));
        assert!(error.contains("lost-2fa"));
        assert!(error.contains("rotate-api-key"));
    }

    #[test]
    fn validation_rejects_non_finite_vectors_and_duplicate_names() {
        let mut fixture = load_fixture().unwrap();
        fixture.chunks[0].embedding[0] = f32::INFINITY;
        assert!(validate_fixture(&fixture)
            .unwrap_err()
            .to_string()
            .contains("non-finite"));

        let mut fixture = load_fixture().unwrap();
        fixture.queries.push(fixture.queries[0].clone());
        assert!(validate_fixture(&fixture)
            .unwrap_err()
            .to_string()
            .contains("duplicate query name"));
    }

    #[test]
    fn password_query_returns_ranked_cited_chunks() {
        let fixture = load_fixture().unwrap();
        let store = Arc::new(SqliteStore::open_in_memory().unwrap());
        let map = build_map(store, &fixture).unwrap();
        let hits = retrieve(&map, &fixture, "password-reset").unwrap();

        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].metadata.section, "Reset a forgotten password");
        assert!(hits
            .iter()
            .all(|hit| hit.key.starts_with(b"tenant/acme/docs/")));
    }

    #[test]
    fn context_contains_numbered_sources_and_no_generated_answer() {
        let fixture = load_fixture().unwrap();
        let store = Arc::new(SqliteStore::open_in_memory().unwrap());
        let map = build_map(store, &fixture).unwrap();
        let hits = retrieve(&map, &fixture, "rotate-api-key").unwrap();
        let context = render_context(&hits);

        assert!(context.starts_with("<context>\n"));
        assert!(context.contains("[1] Rotate an API key"));
        assert!(context.contains("Source: https://docs.example.com/security/api-keys"));
        assert!(context.ends_with("</context>"));
        assert!(!context.contains("Generated answer:"));
    }

    #[test]
    fn sqlite_named_root_reopens_the_same_verified_proximity_map() {
        let fixture = load_fixture().unwrap();
        let path = temp_db_path("semantic-rag");
        remove_sqlite_files(&path);

        let descriptor = {
            let store = durable_store(&path).unwrap();
            let (map, state) = open_or_build_corpus(store, &fixture).unwrap();
            assert_eq!(state, CorpusState::Built);
            map.tree().descriptor.clone()
        };

        {
            let store = durable_store(&path).unwrap();
            let (map, state) = open_or_build_corpus(store, &fixture).unwrap();
            assert_eq!(state, CorpusState::Reopened);
            assert_eq!(map.tree().descriptor, descriptor);
            assert_eq!(retrieve(&map, &fixture, "lost-2fa").unwrap().len(), 3);
        }

        remove_sqlite_files(&path);
    }

    #[test]
    fn persisted_embedding_metadata_must_match_the_fixture() {
        let fixture = load_fixture().unwrap();
        let mut metadata = expected_manifest_metadata(&fixture);
        metadata.insert(b"dimensions".to_vec(), b"768".to_vec());
        let manifest = ContentRootManifest {
            root: TypedContentRoot::proximity_descriptor(prolly::Cid::from_bytes(b"descriptor")),
            logical_version: fixture.corpus_version,
            created_at_millis: 0,
            metadata,
        };

        assert!(validate_manifest(&manifest, &fixture)
            .unwrap_err()
            .to_string()
            .contains("embedding metadata"));
    }
}
