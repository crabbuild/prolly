use super::{HnswConfig, HnswRoutingVectorEncoding};
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::encoding::Encoding;
use crate::prolly::error::Error;
use crate::prolly::proximity::storage::codec::{
    put_bytes, put_cid, put_f32, put_varint, Reader, MAX_KEY_BYTES, MAX_OBJECT_ENTRIES,
};
use crate::prolly::proximity::DistanceMetric;
use crate::prolly::store::{NodePublication, PublicationOrigin, Store};

const MANIFEST_MAGIC: &[u8; 4] = b"HNSW";
const NODE_MAGIC: &[u8; 4] = b"HNSN";
const HNSW_FORMAT_VERSION: u8 = 2;

#[derive(Clone, Debug)]
pub(crate) struct GraphNode {
    pub level: u8,
    pub routing_vector_encoding: HnswRoutingVectorEncoding,
    pub routing_vector: Vec<f32>,
    pub neighbors: Vec<Vec<Vec<u8>>>,
}

impl GraphNode {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(NODE_MAGIC);
        bytes.push(HNSW_FORMAT_VERSION);
        bytes.push(0);
        bytes.push(self.level);
        bytes.push(self.routing_vector_encoding.id());
        put_varint(self.routing_vector.len() as u64, &mut bytes);
        for component in &self.routing_vector {
            put_f32(*component, &mut bytes)?;
        }
        put_varint(self.neighbors.len() as u64, &mut bytes);
        for layer in &self.neighbors {
            put_varint(layer.len() as u64, &mut bytes);
            for neighbor in layer {
                put_bytes(neighbor, &mut bytes);
            }
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "HNSW node");
        reader.exact(NODE_MAGIC)?;
        require_hnsw_version(reader.u8()?)?;
        if reader.u8()? != 0 {
            return Err(reader.invalid("unknown flags"));
        }
        let level = reader.u8()?;
        let routing_vector_encoding = HnswRoutingVectorEncoding::from_id(reader.u8()?)?;
        let dimensions = reader.bounded_usize(MAX_OBJECT_ENTRIES)?;
        let mut routing_vector = Vec::with_capacity(dimensions);
        for _ in 0..dimensions {
            routing_vector.push(reader.f32()?);
        }
        let layers = reader.bounded_usize(65)?;
        let mut neighbors = Vec::with_capacity(layers);
        for _ in 0..layers {
            let count = reader.bounded_usize(MAX_OBJECT_ENTRIES)?;
            let mut layer = Vec::with_capacity(count);
            for _ in 0..count {
                layer.push(reader.bytes(MAX_KEY_BYTES)?);
            }
            neighbors.push(layer);
        }
        reader.finish()?;
        let node = Self {
            level,
            routing_vector_encoding,
            routing_vector,
            neighbors,
        };
        node.validate()?;
        Ok(node)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.routing_vector.is_empty()
            || self.neighbors.len() != usize::from(self.level) + 1
            || self
                .neighbors
                .iter()
                .any(|layer| layer.windows(2).any(|pair| pair[0] >= pair[1]))
        {
            return Err(invalid("invalid HNSW node layers or neighbor ordering"));
        }
        Ok(())
    }
}

pub(crate) struct Manifest {
    pub(crate) source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) count: u64,
    pub(crate) config: HnswConfig,
    pub(crate) graph_root: Cid,
    pub(crate) entry_point: Vec<u8>,
    pub(crate) maximum_level: u8,
    pub(crate) canonical: bool,
}

impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.push(HNSW_FORMAT_VERSION);
        bytes.push(u8::from(self.canonical));
        put_cid(&self.source, &mut bytes);
        put_varint(u64::from(self.dimensions), &mut bytes);
        bytes.push(self.metric.id());
        put_varint(self.count, &mut bytes);
        encode_config(&self.config, &mut bytes);
        put_cid(&self.graph_root, &mut bytes);
        put_bytes(&self.entry_point, &mut bytes);
        bytes.push(self.maximum_level);
        put_cid(&config_fingerprint(&self.config), &mut bytes);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "HNSW manifest");
        reader.exact(MANIFEST_MAGIC)?;
        require_hnsw_version(reader.u8()?)?;
        let canonical = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(reader.invalid("invalid canonical flag")),
        };
        let source = reader.cid()?;
        let dimensions =
            u32::try_from(reader.varint()?).map_err(|_| reader.invalid("dimensions exceed u32"))?;
        let metric = DistanceMetric::from_id(reader.u8()?)?;
        let count = reader.varint()?;
        let config = decode_config(&mut reader)?;
        let graph_root = reader.cid()?;
        let entry_point = reader.bytes(MAX_KEY_BYTES)?;
        let maximum_level = reader.u8()?;
        if count == 0
            || entry_point.is_empty()
            || maximum_level > 64
            || reader.cid()? != config_fingerprint(&config)
        {
            return Err(reader.invalid("invalid entry point or configuration fingerprint"));
        }
        reader.finish()?;
        Ok(Self {
            source,
            dimensions,
            metric,
            count,
            config,
            graph_root,
            entry_point,
            maximum_level,
            canonical,
        })
    }
}

fn encode_config(config: &HnswConfig, bytes: &mut Vec<u8>) {
    put_varint(u64::from(config.max_connections), bytes);
    put_varint(u64::from(config.ef_construction), bytes);
    put_varint(u64::from(config.ef_search), bytes);
    bytes.push(config.level_bits);
    put_varint(u64::from(config.overfetch_multiplier), bytes);
    bytes.extend_from_slice(&config.seed.to_le_bytes());
    bytes.push(config.routing_vector_encoding.id());
}

fn decode_config(reader: &mut Reader<'_>) -> Result<HnswConfig, Error> {
    Ok(HnswConfig {
        max_connections: u16::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("max connections exceed u16"))?,
        ef_construction: u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("ef construction exceeds u32"))?,
        ef_search: u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("ef search exceeds u32"))?,
        level_bits: reader.u8()?,
        overfetch_multiplier: u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("overfetch multiplier exceeds u32"))?,
        seed: reader.u64_le()?,
        routing_vector_encoding: HnswRoutingVectorEncoding::from_id(reader.u8()?)?,
    })
}

fn require_hnsw_version(found: u8) -> Result<(), Error> {
    if found == HNSW_FORMAT_VERSION {
        Ok(())
    } else {
        Err(Error::UnsupportedProximityVersion {
            found,
            required: HNSW_FORMAT_VERSION,
        })
    }
}

pub(crate) fn config_fingerprint(config: &HnswConfig) -> Cid {
    let mut bytes = Vec::new();
    encode_config(config, &mut bytes);
    Cid::from_bytes(&bytes)
}

pub(crate) fn graph_config() -> Config {
    Config::builder()
        .min_chunk_size(4)
        .max_chunk_size(1024 * 1024)
        .chunking_factor(128)
        .hash_seed(0)
        .encoding(Encoding::Raw)
        .build()
}

pub(super) fn load_content<S: Store>(store: &S, cid: &Cid) -> Result<Vec<u8>, Error> {
    let bytes = store
        .get(cid.as_bytes())
        .map_err(|error| Error::Store(Box::new(error)))?
        .ok_or_else(|| Error::NotFound(cid.clone()))?;
    let actual = Cid::from_bytes(&bytes);
    if actual != *cid {
        return Err(Error::CidMismatch {
            expected: cid.clone(),
            actual,
        });
    }
    Ok(bytes)
}

pub(super) fn put_content<S: Store>(store: &S, cid: &Cid, bytes: &[u8]) -> Result<(), Error> {
    if let Some(existing) = store
        .get(cid.as_bytes())
        .map_err(|error| Error::Store(Box::new(error)))?
    {
        let actual = Cid::from_bytes(&existing);
        if actual != *cid {
            return Err(Error::CidMismatch {
                expected: cid.clone(),
                actual,
            });
        }
        return Ok(());
    }
    let entries = [(cid.as_bytes(), bytes)];
    store
        .publish_nodes(NodePublication::new(
            &entries,
            PublicationOrigin::Maintenance,
        ))
        .map_err(|error| Error::Store(Box::new(error)))
}

pub(super) fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "HNSW",
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_manifest_and_graph_nodes_require_rebuild() {
        assert!(matches!(
            Manifest::decode(b"HNSW\x01"),
            Err(Error::UnsupportedProximityVersion {
                found: 1,
                required: 2
            })
        ));
        assert!(matches!(
            GraphNode::decode(b"HNSN\x01"),
            Err(Error::UnsupportedProximityVersion {
                found: 1,
                required: 2
            })
        ));
    }
}
