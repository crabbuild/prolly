use super::super::cid::Cid;
use super::super::config::Config;
use super::super::encoding::Encoding;
use super::super::error::Error;
use super::super::tree::Tree;
use super::codec::{put_varint, Reader};
use super::{DistanceMetric, ProximityConfig};

const MAGIC: &[u8; 4] = b"PRXI";
const VERSION: u8 = 1;
const VECTOR_ENCODING_F32_LE: u8 = 1;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Descriptor {
    pub(crate) config: ProximityConfig,
    pub(crate) count: u64,
    pub(crate) directory: Tree,
    pub(crate) proximity_root: Cid,
}

fn put_option(value: Option<usize>, out: &mut Vec<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            put_varint(value as u64, out);
        }
        None => out.push(0),
    }
}

fn get_option(reader: &mut Reader<'_>) -> Result<Option<usize>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.usize()?)),
        _ => Err(Error::InvalidProximityObject {
            kind: "descriptor",
            reason: "invalid option tag".to_owned(),
        }),
    }
}

impl Descriptor {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.push(VECTOR_ENCODING_F32_LE);
        put_varint(u64::from(self.config.dimensions), &mut bytes);
        bytes.push(self.config.metric.id());
        bytes.push(self.config.log_chunk_size);
        bytes.extend_from_slice(&self.config.level_hash_seed.to_le_bytes());
        put_varint(u64::from(self.config.max_node_bytes), &mut bytes);
        put_varint(self.count, &mut bytes);
        match &self.directory.root {
            Some(root) => {
                bytes.push(1);
                bytes.extend_from_slice(root.as_bytes());
            }
            None => bytes.push(0),
        }
        put_varint(self.directory.config.min_chunk_size as u64, &mut bytes);
        put_varint(self.directory.config.max_chunk_size as u64, &mut bytes);
        put_varint(u64::from(self.directory.config.chunking_factor), &mut bytes);
        bytes.extend_from_slice(&self.directory.config.hash_seed.to_le_bytes());
        match &self.directory.config.encoding {
            Encoding::Raw => bytes.push(0),
            Encoding::Cbor => bytes.push(1),
            Encoding::Json => bytes.push(2),
            Encoding::Custom(name) => {
                bytes.push(3);
                put_varint(name.len() as u64, &mut bytes);
                bytes.extend_from_slice(name.as_bytes());
            }
        }
        put_option(self.directory.config.node_cache_max_nodes, &mut bytes);
        put_option(self.directory.config.node_cache_max_bytes, &mut bytes);
        bytes.extend_from_slice(self.proximity_root.as_bytes());
        bytes.push(0); // reserved field count
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "descriptor");
        reader.exact(MAGIC)?;
        if reader.u8()? != VERSION || reader.u8()? != VECTOR_ENCODING_F32_LE {
            return Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "unsupported version or vector encoding".to_owned(),
            });
        }
        let dimensions =
            u32::try_from(reader.varint()?).map_err(|_| Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "dimensions exceed u32".to_owned(),
            })?;
        let metric = DistanceMetric::from_id(reader.u8()?)?;
        let log_chunk_size = reader.u8()?;
        let level_hash_seed = reader.u64_le()?;
        let max_node_bytes =
            u32::try_from(reader.varint()?).map_err(|_| Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "max_node_bytes exceeds u32".to_owned(),
            })?;
        let count = reader.varint()?;
        let root = match reader.u8()? {
            0 => None,
            1 => {
                let raw: [u8; 32] =
                    reader
                        .take(32)?
                        .try_into()
                        .map_err(|_| Error::InvalidProximityObject {
                            kind: "descriptor",
                            reason: "invalid directory root CID".to_owned(),
                        })?;
                Some(Cid(raw))
            }
            _ => {
                return Err(Error::InvalidProximityObject {
                    kind: "descriptor",
                    reason: "invalid directory root tag".to_owned(),
                })
            }
        };
        let min_chunk_size = reader.usize()?;
        let max_chunk_size = reader.usize()?;
        let chunking_factor =
            u32::try_from(reader.varint()?).map_err(|_| Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "chunking factor exceeds u32".to_owned(),
            })?;
        let hash_seed = reader.u64_le()?;
        let encoding = match reader.u8()? {
            0 => Encoding::Raw,
            1 => Encoding::Cbor,
            2 => Encoding::Json,
            3 => {
                let len = reader.usize()?;
                let name = std::str::from_utf8(reader.take(len)?)
                    .map_err(|_| Error::InvalidProximityObject {
                        kind: "descriptor",
                        reason: "custom encoding name is not UTF-8".to_owned(),
                    })?
                    .to_owned();
                Encoding::Custom(name)
            }
            _ => {
                return Err(Error::InvalidProximityObject {
                    kind: "descriptor",
                    reason: "unknown directory encoding".to_owned(),
                })
            }
        };
        let node_cache_max_nodes = get_option(&mut reader)?;
        let node_cache_max_bytes = get_option(&mut reader)?;
        let proximity_root = {
            let raw: [u8; 32] =
                reader
                    .take(32)?
                    .try_into()
                    .map_err(|_| Error::InvalidProximityObject {
                        kind: "descriptor",
                        reason: "invalid proximity root CID".to_owned(),
                    })?;
            Cid(raw)
        };
        if reader.u8()? != 0 {
            return Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "reserved fields are not supported".to_owned(),
            });
        }
        reader.finish()?;
        let config = ProximityConfig {
            dimensions,
            metric,
            log_chunk_size,
            level_hash_seed,
            max_node_bytes,
        };
        config.validate()?;
        Ok(Self {
            config,
            count,
            directory: Tree {
                root,
                config: Config {
                    min_chunk_size,
                    max_chunk_size,
                    chunking_factor,
                    hash_seed,
                    encoding,
                    node_cache_max_nodes,
                    node_cache_max_bytes,
                },
            },
            proximity_root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_round_trip_preserves_shape_and_directory_config() {
        let descriptor = Descriptor {
            config: ProximityConfig {
                dimensions: 4,
                metric: DistanceMetric::L2Squared,
                log_chunk_size: 8,
                level_hash_seed: 99,
                max_node_bytes: 4096,
            },
            count: 12,
            directory: Tree::new(Config::default()),
            proximity_root: Cid::from_bytes(b"root"),
        };
        let bytes = descriptor.encode();
        assert_eq!(Descriptor::decode(&bytes).unwrap(), descriptor);
    }
}
