use prolly_store_redis::redis::RedisBackend;
use std::time::{Duration, Instant};

use crate::model::CellSpec;

const CLONE_CHUNK_SIZE: usize = 256;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RedisStats {
    pub used_memory_bytes: u64,
    pub used_memory_rss_bytes: u64,
    pub used_memory_dataset_bytes: u64,
    pub aof_current_size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureLayout {
    redis_url: String,
    records: usize,
    repetition: usize,
}

impl FixtureLayout {
    pub fn new(redis_url: String, records: usize, repetition: usize) -> Self {
        Self {
            redis_url,
            records,
            repetition,
        }
    }

    pub fn redis_url(&self) -> &str {
        &self.redis_url
    }

    pub fn source_prefix(&self) -> Vec<u8> {
        format!(
            "prolly:redis-scale:{}:run:{}:source:",
            self.records, self.repetition
        )
        .into_bytes()
    }

    pub fn cell_prefix(&self, spec: &CellSpec) -> Vec<u8> {
        format!(
            "prolly:redis-scale:{}:run:{}:cell:{}:{}:{}:",
            self.records,
            self.repetition,
            spec.operation.as_str(),
            spec.pattern.as_str(),
            spec.cache_state.as_str()
        )
        .into_bytes()
    }

    pub async fn clear_source(&self) -> Result<(), String> {
        clear_namespace(&self.redis_url, self.source_prefix()).await
    }

    pub async fn clear_cell(&self, spec: &CellSpec) -> Result<(), String> {
        clear_namespace(&self.redis_url, self.cell_prefix(spec)).await
    }

    pub async fn clone_for(&self, spec: &CellSpec) -> Result<usize, String> {
        let source = self.source_prefix();
        let destination = self.cell_prefix(spec);
        if source.is_empty() || destination.is_empty() || source == destination {
            return Err("unsafe Redis fixture namespace".to_string());
        }
        self.clear_cell(spec).await?;

        let backend = RedisBackend::connect(&self.redis_url)
            .await
            .map_err(|error| format!("failed to connect to Redis for clone: {error}"))?;
        let mut connection = backend.connection().clone();
        let keys = scan_keys(&mut connection, &source).await?;
        for chunk in keys.chunks(CLONE_CHUNK_SIZE) {
            let mut pipeline = redis_client::pipe();
            pipeline.atomic();
            for key in chunk {
                let suffix = key.strip_prefix(source.as_slice()).ok_or_else(|| {
                    "Redis SCAN returned a key outside the source namespace".to_string()
                })?;
                let mut target = destination.clone();
                target.extend_from_slice(suffix);
                pipeline
                    .cmd("COPY")
                    .arg(key.as_slice())
                    .arg(target)
                    .arg("REPLACE")
                    .ignore();
            }
            pipeline
                .query_async::<()>(&mut connection)
                .await
                .map_err(|error| format!("failed to clone Redis fixture: {error}"))?;
        }
        let observed = namespace_key_count(&self.redis_url, &destination).await?;
        if observed != keys.len() {
            return Err(format!(
                "Redis fixture clone key count mismatch: observed {observed}, expected {}",
                keys.len()
            ));
        }
        Ok(observed)
    }
}

pub async fn clear_namespace(redis_url: &str, prefix: Vec<u8>) -> Result<(), String> {
    if prefix.is_empty() {
        return Err("refusing to clear an empty Redis namespace".to_string());
    }
    let backend = RedisBackend::connect(redis_url)
        .await
        .map_err(|error| format!("failed to connect to Redis for cleanup: {error}"))?
        .with_key_prefix(prefix);
    backend
        .clear_namespace()
        .await
        .map_err(|error| format!("failed to clear Redis namespace: {error}"))
}

pub async fn namespace_key_count(redis_url: &str, prefix: &[u8]) -> Result<usize, String> {
    let backend = RedisBackend::connect(redis_url)
        .await
        .map_err(|error| format!("failed to connect to Redis for key count: {error}"))?;
    let mut connection = backend.connection().clone();
    Ok(scan_keys(&mut connection, prefix).await?.len())
}

pub async fn redis_stats(redis_url: &str) -> Result<RedisStats, String> {
    let backend = RedisBackend::connect(redis_url)
        .await
        .map_err(|error| format!("failed to connect to Redis for INFO: {error}"))?;
    let mut connection = backend.connection().clone();
    let memory: String = redis_client::cmd("INFO")
        .arg("memory")
        .query_async(&mut connection)
        .await
        .map_err(|error| format!("Redis INFO memory failed: {error}"))?;
    let persistence: String = redis_client::cmd("INFO")
        .arg("persistence")
        .query_async(&mut connection)
        .await
        .map_err(|error| format!("Redis INFO persistence failed: {error}"))?;
    Ok(RedisStats {
        used_memory_bytes: info_u64(&memory, "used_memory"),
        used_memory_rss_bytes: info_u64(&memory, "used_memory_rss"),
        used_memory_dataset_bytes: info_u64(&memory, "used_memory_dataset"),
        aof_current_size_bytes: info_u64(&persistence, "aof_current_size"),
    })
}

pub async fn validate_strong_durability(redis_url: &str) -> Result<(), String> {
    let backend = RedisBackend::connect(redis_url)
        .await
        .map_err(|error| format!("failed to connect to Redis: {error}"))?;
    let mut connection = backend.connection().clone();
    let pong: String = redis_client::cmd("PING")
        .query_async(&mut connection)
        .await
        .map_err(|error| format!("Redis PING failed: {error}"))?;
    if pong != "PONG" {
        return Err(format!("unexpected Redis PING response: {pong}"));
    }
    for (name, expected) in [
        ("appendonly", "yes"),
        ("appendfsync", "always"),
        ("save", ""),
        ("auto-aof-rewrite-percentage", "0"),
        ("no-appendfsync-on-rewrite", "no"),
    ] {
        let values: Vec<String> = redis_client::cmd("CONFIG")
            .arg("GET")
            .arg(name)
            .query_async(&mut connection)
            .await
            .map_err(|error| format!("Redis CONFIG GET {name} failed: {error}"))?;
        let actual = values.get(1).map(String::as_str).unwrap_or_default();
        if actual != expected {
            return Err(format!(
                "Redis strong-durability requirement failed: {name}={actual:?}, expected {expected:?}"
            ));
        }
    }
    Ok(())
}

pub async fn compact_aof(redis_url: &str) -> Result<(), String> {
    let backend = RedisBackend::connect(redis_url)
        .await
        .map_err(|error| format!("failed to connect to Redis for AOF rewrite: {error}"))?;
    let mut connection = backend.connection().clone();
    redis_client::cmd("BGREWRITEAOF")
        .query_async::<String>(&mut connection)
        .await
        .map_err(|error| format!("Redis BGREWRITEAOF failed: {error}"))?;

    let started = Instant::now();
    loop {
        let persistence: String = redis_client::cmd("INFO")
            .arg("persistence")
            .query_async(&mut connection)
            .await
            .map_err(|error| format!("Redis INFO persistence failed during rewrite: {error}"))?;
        let in_progress = info_u64(&persistence, "aof_rewrite_in_progress");
        let scheduled = info_u64(&persistence, "aof_rewrite_scheduled");
        if in_progress == 0 && scheduled == 0 {
            let rewrite_status = info_value(&persistence, "aof_last_bgrewrite_status");
            let write_status = info_value(&persistence, "aof_last_write_status");
            if rewrite_status != "ok" || write_status != "ok" {
                return Err(format!(
                    "Redis AOF rewrite was not durable: rewrite={rewrite_status:?}, write={write_status:?}"
                ));
            }
            return Ok(());
        }
        if started.elapsed() > Duration::from_secs(600) {
            return Err("Redis AOF rewrite did not finish within 600 seconds".to_string());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn scan_keys(
    connection: &mut redis_client::aio::ConnectionManager,
    prefix: &[u8],
) -> Result<Vec<Vec<u8>>, String> {
    if prefix.is_empty() {
        return Err("refusing to scan an empty Redis namespace".to_string());
    }
    let mut pattern = prefix.to_vec();
    pattern.push(b'*');
    let mut cursor = 0_u64;
    let mut keys = Vec::new();
    loop {
        let (next, batch): (u64, Vec<Vec<u8>>) = redis_client::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern.as_slice())
            .arg("COUNT")
            .arg(1_000)
            .query_async(connection)
            .await
            .map_err(|error| format!("Redis SCAN failed: {error}"))?;
        keys.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(keys)
}

fn info_u64(info: &str, field: &str) -> u64 {
    info_value(info, field).parse().unwrap_or(0)
}

fn info_value<'a>(info: &'a str, field: &str) -> &'a str {
    info.lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
        .map(|value| value.trim_end_matches('\r'))
        .unwrap_or_default()
}
