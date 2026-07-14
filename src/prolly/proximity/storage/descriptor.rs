use super::codec::{put_cid, put_varint, Reader, FORMAT_VERSION, VECTOR_ENCODING_F32_LE};
use super::{ReferenceKind, TypedReference};
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::error::Error;
use crate::prolly::format::TreeFormat;
use crate::prolly::proximity::{
    DistanceMetric, HierarchyConfig, OverflowConfig, ProximityConfig, ScalarQuantizationConfig,
    VectorStorageConfig,
};
use crate::prolly::tree::Tree;

const MAGIC: &[u8; 4] = b"PRXI";
const COVERING_RADIUS_EUCLIDEAN_F64_UP: u8 = 1;
const NORMALIZATION_NONE: u8 = 0;
const NORMALIZATION_UNIT_F32_FIXED_POINT: u8 = 1;
const MAX_TREE_FORMAT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Descriptor {
    pub(crate) config: ProximityConfig,
    pub(crate) count: u64,
    pub(crate) directory: Tree,
    pub(crate) proximity_root: Cid,
}

impl Descriptor {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(FORMAT_VERSION);
        bytes.push(0); // required flags
        bytes.push(VECTOR_ENCODING_F32_LE);
        put_varint(u64::from(self.config.dimensions), &mut bytes);
        bytes.push(self.config.metric.id());
        bytes.push(normalization_id(self.config.metric));
        put_varint(self.count, &mut bytes);
        bytes.push(self.config.hierarchy.log_chunk_size);
        bytes.extend_from_slice(&self.config.hierarchy.level_hash_seed.to_le_bytes());
        put_varint(u64::from(self.config.overflow.min_page_bytes), &mut bytes);
        put_varint(
            u64::from(self.config.overflow.target_page_bytes),
            &mut bytes,
        );
        put_varint(u64::from(self.config.overflow.max_page_bytes), &mut bytes);
        bytes.extend_from_slice(&self.config.overflow.hash_seed.to_le_bytes());
        put_varint(
            u64::from(self.config.vector_storage.inline_threshold_bytes),
            &mut bytes,
        );
        match &self.config.scalar_quantization {
            None => bytes.push(0),
            Some(config) => {
                bytes.push(1);
                put_varint(u64::from(config.group_size), &mut bytes);
            }
        }
        encode_directory(&self.directory, &mut bytes);
        put_cid(&self.proximity_root, &mut bytes);
        bytes.push(COVERING_RADIUS_EUCLIDEAN_F64_UP);
        put_cid(&configuration_fingerprint(&self.config), &mut bytes);
        put_varint(0, &mut bytes); // reserved extension bytes
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "descriptor");
        reader.exact(MAGIC)?;
        reader.version()?;
        if reader.u8()? != 0 {
            return Err(reader.invalid("unknown required flags"));
        }
        if reader.u8()? != VECTOR_ENCODING_F32_LE {
            return Err(reader.invalid("unsupported vector encoding"));
        }
        let dimensions =
            u32::try_from(reader.varint()?).map_err(|_| reader.invalid("dimensions exceed u32"))?;
        let metric = DistanceMetric::from_id(reader.u8()?)?;
        if reader.u8()? != normalization_id(metric) {
            return Err(reader.invalid("metric normalization policy mismatch"));
        }
        let count = reader.varint()?;
        let hierarchy = HierarchyConfig {
            log_chunk_size: reader.u8()?,
            level_hash_seed: reader.u64_le()?,
        };
        let overflow = OverflowConfig {
            min_page_bytes: read_u32(&mut reader, "minimum page bytes")?,
            target_page_bytes: read_u32(&mut reader, "target page bytes")?,
            max_page_bytes: read_u32(&mut reader, "maximum page bytes")?,
            hash_seed: reader.u64_le()?,
        };
        let vector_storage = VectorStorageConfig {
            inline_threshold_bytes: read_u32(&mut reader, "inline vector threshold")?,
        };
        let scalar_quantization = match reader.u8()? {
            0 => None,
            1 => Some(ScalarQuantizationConfig {
                group_size: read_u32(&mut reader, "scalar quantization group size")?,
            }),
            _ => return Err(reader.invalid("invalid scalar quantization tag")),
        };
        let directory = decode_directory(&mut reader)?;
        let proximity_root = reader.cid()?;
        if reader.u8()? != COVERING_RADIUS_EUCLIDEAN_F64_UP {
            return Err(reader.invalid("unsupported covering-bound encoding"));
        }
        let stored_fingerprint = reader.cid()?;
        if reader.varint()? != 0 {
            return Err(reader.invalid("reserved extensions are not supported"));
        }
        reader.finish()?;

        let config = ProximityConfig {
            dimensions,
            metric,
            hierarchy,
            overflow,
            vector_storage,
            scalar_quantization,
        };
        config.validate()?;
        let expected_fingerprint = configuration_fingerprint(&config);
        if stored_fingerprint != expected_fingerprint {
            return Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "configuration fingerprint mismatch".to_owned(),
            });
        }
        validate_directory_config(&directory.config)?;
        Ok(Self {
            config,
            count,
            directory,
            proximity_root,
        })
    }

    #[allow(dead_code)] // Used by typed graph traversal in the integration slice.
    pub(crate) fn references(bytes: &[u8]) -> Result<Vec<TypedReference>, Error> {
        let descriptor = Self::decode(bytes)?;
        let mut references = Vec::with_capacity(2);
        if let Some(root) = descriptor.directory.root {
            references.push(TypedReference {
                kind: ReferenceKind::OrderedNode,
                cid: root,
            });
        }
        references.push(TypedReference {
            kind: ReferenceKind::ProximityNode,
            cid: descriptor.proximity_root,
        });
        Ok(references)
    }
}

fn normalization_id(metric: DistanceMetric) -> u8 {
    if metric == DistanceMetric::Cosine {
        NORMALIZATION_UNIT_F32_FIXED_POINT
    } else {
        NORMALIZATION_NONE
    }
}

fn read_u32(reader: &mut Reader<'_>, field: &str) -> Result<u32, Error> {
    u32::try_from(reader.varint()?).map_err(|_| reader.invalid(format!("{field} exceeds u32")))
}

fn encode_directory(tree: &Tree, out: &mut Vec<u8>) {
    match &tree.root {
        Some(root) => {
            out.push(1);
            put_cid(root, out);
        }
        None => out.push(0),
    }
    let format = tree
        .config
        .format
        .canonical_bytes()
        .expect("directory tree format must be valid");
    put_varint(format.len() as u64, out);
    out.extend_from_slice(&format);
}

fn decode_directory(reader: &mut Reader<'_>) -> Result<Tree, Error> {
    let root = match reader.u8()? {
        0 => None,
        1 => Some(reader.cid()?),
        _ => return Err(reader.invalid("invalid directory root tag")),
    };
    let format = TreeFormat::from_canonical_bytes(&reader.bytes(MAX_TREE_FORMAT_BYTES)?)?;
    Ok(Tree {
        root,
        config: Config {
            format,
            runtime: Default::default(),
        },
    })
}

fn validate_directory_config(config: &Config) -> Result<(), Error> {
    config
        .format
        .validate()
        .map_err(|_| Error::InvalidProximityObject {
            kind: "descriptor",
            reason: "invalid ordered-directory configuration".to_owned(),
        })
}

fn configuration_fingerprint(config: &ProximityConfig) -> Cid {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"PCFG");
    bytes.push(FORMAT_VERSION);
    put_varint(u64::from(config.dimensions), &mut bytes);
    bytes.push(config.metric.id());
    bytes.push(normalization_id(config.metric));
    bytes.push(config.hierarchy.log_chunk_size);
    bytes.extend_from_slice(&config.hierarchy.level_hash_seed.to_le_bytes());
    put_varint(u64::from(config.overflow.min_page_bytes), &mut bytes);
    put_varint(u64::from(config.overflow.target_page_bytes), &mut bytes);
    put_varint(u64::from(config.overflow.max_page_bytes), &mut bytes);
    bytes.extend_from_slice(&config.overflow.hash_seed.to_le_bytes());
    put_varint(
        u64::from(config.vector_storage.inline_threshold_bytes),
        &mut bytes,
    );
    match &config.scalar_quantization {
        None => bytes.push(0),
        Some(config) => {
            bytes.push(1);
            put_varint(u64::from(config.group_size), &mut bytes);
        }
    }
    Cid::from_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_round_trip_preserves_every_shape_field() {
        let mut config = ProximityConfig::new(4);
        config.metric = DistanceMetric::Cosine;
        config.hierarchy.level_hash_seed = 99;
        config.overflow.hash_seed = 17;
        config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 2 });
        let descriptor = Descriptor {
            config,
            count: 12,
            directory: Tree::new(Config::default()),
            proximity_root: Cid::from_bytes(b"root"),
        };
        let bytes = descriptor.encode();
        assert_eq!(&bytes[..5], b"PRXI\x02");
        assert_eq!(Descriptor::decode(&bytes).unwrap(), descriptor);
        assert_eq!(Descriptor::references(&bytes).unwrap().len(), 1);

        let mut bad_flags = bytes.clone();
        bad_flags[5] = 1;
        assert!(Descriptor::decode(&bad_flags).is_err());

        let mut bad_fingerprint = bytes.clone();
        let fingerprint_last = bad_fingerprint.len() - 2;
        bad_fingerprint[fingerprint_last] ^= 1;
        assert!(Descriptor::decode(&bad_fingerprint).is_err());

        let mut trailing = bytes;
        trailing.push(0);
        assert!(Descriptor::decode(&trailing).is_err());
    }
}
