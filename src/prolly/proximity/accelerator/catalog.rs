use super::composite::{config_fingerprint as composite_fingerprint, CompositeAccelerator};
use super::hnsw::storage::config_fingerprint as hnsw_fingerprint;
use super::pq::config_fingerprint as pq_fingerprint;
use super::{AcceleratorSet, HnswIndex, ProductQuantizer};
use crate::prolly::cid::Cid;
use crate::prolly::content_graph::{ContentObjectKind, TypedContentRoot};
use crate::prolly::error::Error;
use crate::prolly::proximity::storage::codec::{put_cid, put_varint, Reader, MAX_OBJECT_ENTRIES};
use crate::prolly::proximity::ProximityTree;
use crate::prolly::store::{NodePublication, PublicationOrigin, Store};

const MAGIC: &[u8; 4] = b"PACL";
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CatalogAcceleratorKind {
    Hnsw,
    ProductQuantized,
    Composite,
}

impl CatalogAcceleratorKind {
    pub(crate) const fn id(self) -> u8 {
        match self {
            Self::Hnsw => 1,
            Self::ProductQuantized => 2,
            Self::Composite => 3,
        }
    }

    fn from_id(id: u8) -> Result<Self, Error> {
        match id {
            1 => Ok(Self::Hnsw),
            2 => Ok(Self::ProductQuantized),
            3 => Ok(Self::Composite),
            _ => Err(invalid("unknown catalog accelerator kind")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceleratorCatalogEntry {
    pub kind: CatalogAcceleratorKind,
    pub configuration_fingerprint: Cid,
    pub manifest: Cid,
}

pub struct AcceleratorCatalog<S: Store> {
    manifest: Cid,
    source: Cid,
    entries: Vec<AcceleratorCatalogEntry>,
    accelerators: AcceleratorSet<S>,
}

impl<S> AcceleratorCatalog<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    pub fn build(
        store: S,
        source: &ProximityTree,
        accelerators: AcceleratorSet<S>,
    ) -> Result<Self, Error> {
        let entries = entries_from_set(&accelerators);
        if entries.is_empty() {
            return Err(invalid("accelerator catalog must not be empty"));
        }
        let object = Manifest {
            source: source.descriptor.clone(),
            entries: entries.clone(),
        };
        let bytes = object.encode()?;
        let manifest = Cid::from_bytes(&bytes);
        put_content(&store, &manifest, &bytes)?;
        Ok(Self {
            manifest,
            source: source.descriptor.clone(),
            entries,
            accelerators,
        })
    }

    pub fn load(store: S, manifest: Cid, source: &ProximityTree) -> Result<Self, Error> {
        let bytes = load_content(&store, &manifest)?;
        let object = Manifest::decode(&bytes)?;
        if object.source != source.descriptor {
            return Err(invalid("catalog is bound to a different source snapshot"));
        }
        let mut accelerators = AcceleratorSet::empty();
        for entry in &object.entries {
            accelerators = match entry.kind {
                CatalogAcceleratorKind::Hnsw => {
                    let index = HnswIndex::load(store.clone(), entry.manifest.clone())?;
                    if hnsw_fingerprint(index.config()) != entry.configuration_fingerprint {
                        return Err(invalid("catalog HNSW fingerprint mismatch"));
                    }
                    accelerators.with_hnsw(source, index)?
                }
                CatalogAcceleratorKind::ProductQuantized => {
                    let index = ProductQuantizer::load(store.clone(), entry.manifest.clone())?;
                    if pq_fingerprint(index.config()) != entry.configuration_fingerprint {
                        return Err(invalid("catalog PQ fingerprint mismatch"));
                    }
                    accelerators.with_pq(source, index)?
                }
                CatalogAcceleratorKind::Composite => {
                    let index = CompositeAccelerator::load(store.clone(), entry.manifest.clone())?;
                    if composite_fingerprint(index.config()) != entry.configuration_fingerprint {
                        return Err(invalid("catalog composite fingerprint mismatch"));
                    }
                    accelerators.with_composite(source, index)?
                }
            };
        }
        Ok(Self {
            manifest,
            source: object.source,
            entries: object.entries,
            accelerators,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }
    pub fn typed_root(&self) -> TypedContentRoot {
        TypedContentRoot::new(ContentObjectKind::AcceleratorCatalog, self.manifest.clone())
    }
    pub fn source_descriptor(&self) -> &Cid {
        &self.source
    }
    pub fn entries(&self) -> &[AcceleratorCatalogEntry] {
        &self.entries
    }
    pub fn accelerators(&self) -> &AcceleratorSet<S> {
        &self.accelerators
    }
    pub fn into_accelerators(self) -> AcceleratorSet<S> {
        self.accelerators
    }
}

fn entries_from_set<S>(set: &AcceleratorSet<S>) -> Vec<AcceleratorCatalogEntry>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let mut entries = Vec::new();
    if let Some(index) = set.hnsw() {
        entries.push(AcceleratorCatalogEntry {
            kind: CatalogAcceleratorKind::Hnsw,
            configuration_fingerprint: hnsw_fingerprint(index.config()),
            manifest: index.manifest_cid().clone(),
        });
    }
    if let Some(index) = set.pq() {
        entries.push(AcceleratorCatalogEntry {
            kind: CatalogAcceleratorKind::ProductQuantized,
            configuration_fingerprint: pq_fingerprint(index.config()),
            manifest: index.manifest_cid().clone(),
        });
    }
    if let Some(index) = set.composite() {
        entries.push(AcceleratorCatalogEntry {
            kind: CatalogAcceleratorKind::Composite,
            configuration_fingerprint: composite_fingerprint(index.config()),
            manifest: index.manifest_cid().clone(),
        });
    }
    entries.sort_by(compare_entry);
    entries
}

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) source: Cid,
    pub(crate) entries: Vec<AcceleratorCatalogEntry>,
}

impl Manifest {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        put_cid(&self.source, &mut bytes);
        put_varint(self.entries.len() as u64, &mut bytes);
        for entry in &self.entries {
            bytes.push(entry.kind.id());
            put_cid(&entry.configuration_fingerprint, &mut bytes);
            put_cid(&entry.manifest, &mut bytes);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "accelerator catalog");
        reader.exact(MAGIC)?;
        if reader.u8()? != VERSION {
            return Err(reader.invalid("unsupported accelerator catalog version"));
        }
        let source = reader.cid()?;
        let count = reader.bounded_usize(MAX_OBJECT_ENTRIES)?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(AcceleratorCatalogEntry {
                kind: CatalogAcceleratorKind::from_id(reader.u8()?)?,
                configuration_fingerprint: reader.cid()?,
                manifest: reader.cid()?,
            });
        }
        reader.finish()?;
        let object = Self { source, entries };
        object.validate()?;
        Ok(object)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.entries.is_empty()
            || self.entries.windows(2).any(|pair| {
                compare_entry(&pair[0], &pair[1]) != std::cmp::Ordering::Less
                    || pair[0].kind == pair[1].kind
            })
        {
            return Err(invalid(
                "catalog entries must be sorted, unique, and contain one entry per kind",
            ));
        }
        Ok(())
    }
}

fn compare_entry(
    left: &AcceleratorCatalogEntry,
    right: &AcceleratorCatalogEntry,
) -> std::cmp::Ordering {
    left.kind
        .cmp(&right.kind)
        .then_with(|| {
            left.configuration_fingerprint
                .as_bytes()
                .cmp(right.configuration_fingerprint.as_bytes())
        })
        .then_with(|| left.manifest.as_bytes().cmp(right.manifest.as_bytes()))
}

fn load_content<S: Store>(store: &S, cid: &Cid) -> Result<Vec<u8>, Error> {
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

fn put_content<S: Store>(store: &S, cid: &Cid, bytes: &[u8]) -> Result<(), Error> {
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

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "accelerator catalog",
        reason: reason.into(),
    }
}
