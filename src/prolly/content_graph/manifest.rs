use super::{walk_content_graph, ContentGraphLimits, ContentObjectKind, TypedContentRoot};
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::encoding::Encoding;
use crate::prolly::error::Error;
use crate::prolly::manifest::{ManifestStore, ManifestUpdate, RootManifest};
use crate::prolly::proximity::storage::codec::{
    put_bytes, put_cid, put_varint, Reader, MAX_KEY_BYTES,
};
use crate::prolly::store::Store;
use std::collections::BTreeMap;

const MAGIC: &[u8; 4] = b"CRMF";
const VERSION: u8 = 1;
const ENCODING_NAME: &str = "typed-content-root-v1";

/// Immutable typed-root publication payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentRootManifest {
    pub root: TypedContentRoot,
    pub logical_version: u64,
    pub created_at_millis: u64,
    pub metadata: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl ContentRootManifest {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.push(self.root.kind.id());
        put_cid(&self.root.cid, &mut bytes);
        match self.root.dimensions {
            Some(dimensions) => {
                bytes.push(1);
                put_varint(u64::from(dimensions), &mut bytes);
            }
            None => bytes.push(0),
        }
        put_varint(self.logical_version, &mut bytes);
        put_varint(self.created_at_millis, &mut bytes);
        put_varint(self.metadata.len() as u64, &mut bytes);
        for (key, value) in &self.metadata {
            put_bytes(key, &mut bytes);
            put_bytes(value, &mut bytes);
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "content root manifest");
        reader.exact(MAGIC)?;
        if reader.u8()? != VERSION {
            return Err(reader.invalid("unsupported content manifest version"));
        }
        let kind = ContentObjectKind::from_id(reader.u8()?)
            .ok_or_else(|| reader.invalid("unknown content root kind"))?;
        let cid = reader.cid()?;
        let dimensions = match reader.u8()? {
            0 => None,
            1 => Some(
                u32::try_from(reader.varint()?)
                    .map_err(|_| reader.invalid("dimensions exceed u32"))?,
            ),
            _ => return Err(reader.invalid("invalid dimensions tag")),
        };
        let logical_version = reader.varint()?;
        let created_at_millis = reader.varint()?;
        let count = reader.bounded_usize(1_000_000)?;
        let mut metadata = BTreeMap::new();
        for _ in 0..count {
            let key = reader.bytes(MAX_KEY_BYTES)?;
            let value = reader.bytes(MAX_KEY_BYTES)?;
            if metadata.insert(key, value).is_some() {
                return Err(reader.invalid("duplicate metadata key"));
            }
        }
        reader.finish()?;
        Ok(Self {
            root: TypedContentRoot {
                kind,
                cid,
                dimensions,
            },
            logical_version,
            created_at_millis,
            metadata,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentRootPublication {
    pub manifest_cid: Cid,
    pub manifest: ContentRootManifest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentManifestUpdate {
    Applied(ContentRootPublication),
    Conflict { current_manifest_cid: Option<Cid> },
}

pub fn put_named_content_root<S>(
    store: &S,
    name: &[u8],
    manifest: ContentRootManifest,
) -> Result<ContentRootPublication, Error>
where
    S: Store + ManifestStore,
{
    put_named_content_root_with_limits(store, name, manifest, &ContentGraphLimits::default())
}

pub fn put_named_content_root_with_limits<S>(
    store: &S,
    name: &[u8],
    manifest: ContentRootManifest,
    limits: &ContentGraphLimits,
) -> Result<ContentRootPublication, Error>
where
    S: Store + ManifestStore,
{
    walk_content_graph(store, std::slice::from_ref(&manifest.root), limits)?;
    let publication = persist_manifest(store, manifest)?;
    store
        .put_root(name, &named_pointer(&publication))
        .map_err(|error| Error::Store(Box::new(error)))?;
    Ok(publication)
}

pub fn load_named_content_root<S>(
    store: &S,
    name: &[u8],
) -> Result<Option<ContentRootPublication>, Error>
where
    S: Store + ManifestStore,
{
    load_named_content_root_with_limits(store, name, &ContentGraphLimits::default())
}

pub fn load_named_content_root_with_limits<S>(
    store: &S,
    name: &[u8],
    limits: &ContentGraphLimits,
) -> Result<Option<ContentRootPublication>, Error>
where
    S: Store + ManifestStore,
{
    let Some(pointer) = store
        .get_root(name)
        .map_err(|error| Error::Store(Box::new(error)))?
    else {
        return Ok(None);
    };
    validate_pointer(&pointer)?;
    let cid = pointer.root.expect("validated content manifest pointer");
    let publication = load_manifest(store, cid)?;
    walk_content_graph(
        store,
        std::slice::from_ref(&publication.manifest.root),
        limits,
    )?;
    Ok(Some(publication))
}

pub fn compare_and_swap_named_content_root<S>(
    store: &S,
    name: &[u8],
    expected_manifest_cid: Option<&Cid>,
    manifest: ContentRootManifest,
) -> Result<ContentManifestUpdate, Error>
where
    S: Store + ManifestStore,
{
    compare_and_swap_named_content_root_with_limits(
        store,
        name,
        expected_manifest_cid,
        manifest,
        &ContentGraphLimits::default(),
    )
}

pub fn compare_and_swap_named_content_root_with_limits<S>(
    store: &S,
    name: &[u8],
    expected_manifest_cid: Option<&Cid>,
    manifest: ContentRootManifest,
    limits: &ContentGraphLimits,
) -> Result<ContentManifestUpdate, Error>
where
    S: Store + ManifestStore,
{
    walk_content_graph(store, std::slice::from_ref(&manifest.root), limits)?;
    let publication = persist_manifest(store, manifest)?;
    let expected = expected_manifest_cid.map(pointer_for_cid);
    let new = named_pointer(&publication);
    match store
        .compare_and_swap_root(name, expected.as_ref(), Some(&new))
        .map_err(|error| Error::Store(Box::new(error)))?
    {
        ManifestUpdate::Applied => Ok(ContentManifestUpdate::Applied(publication)),
        ManifestUpdate::Conflict { current } => Ok(ContentManifestUpdate::Conflict {
            current_manifest_cid: current.and_then(|manifest| manifest.root),
        }),
    }
}

fn persist_manifest<S: Store>(
    store: &S,
    manifest: ContentRootManifest,
) -> Result<ContentRootPublication, Error> {
    let bytes = manifest.to_bytes()?;
    let cid = Cid::from_bytes(&bytes);
    if let Some(existing) = store
        .get(cid.as_bytes())
        .map_err(|error| Error::Store(Box::new(error)))?
    {
        if Cid::from_bytes(&existing) != cid {
            return Err(Error::CidMismatch {
                expected: cid,
                actual: Cid::from_bytes(&existing),
            });
        }
    } else {
        store
            .put(cid.as_bytes(), &bytes)
            .map_err(|error| Error::Store(Box::new(error)))?;
    }
    Ok(ContentRootPublication {
        manifest_cid: cid,
        manifest,
    })
}

fn load_manifest<S: Store>(store: &S, cid: Cid) -> Result<ContentRootPublication, Error> {
    let bytes = store
        .get(cid.as_bytes())
        .map_err(|error| Error::Store(Box::new(error)))?
        .ok_or_else(|| Error::NotFound(cid.clone()))?;
    let actual = Cid::from_bytes(&bytes);
    if actual != cid {
        return Err(Error::CidMismatch {
            expected: cid,
            actual,
        });
    }
    Ok(ContentRootPublication {
        manifest_cid: cid,
        manifest: ContentRootManifest::from_bytes(&bytes)?,
    })
}

fn named_pointer(publication: &ContentRootPublication) -> RootManifest {
    pointer_for_cid(&publication.manifest_cid)
}

fn pointer_for_cid(cid: &Cid) -> RootManifest {
    RootManifest::new(Some(cid.clone()), pointer_config())
}

fn validate_pointer(pointer: &RootManifest) -> Result<(), Error> {
    if pointer.root.is_none() || pointer.config != pointer_config() {
        return Err(Error::InvalidProximityObject {
            kind: "content root manifest",
            reason: "named root is not a typed-content manifest pointer".to_owned(),
        });
    }
    Ok(())
}

fn pointer_config() -> Config {
    Config {
        min_chunk_size: 1,
        max_chunk_size: 1,
        chunking_factor: 1,
        hash_seed: 0,
        encoding: Encoding::Custom(ENCODING_NAME.to_owned()),
        node_cache_max_nodes: Some(0),
        node_cache_max_bytes: Some(0),
    }
}
