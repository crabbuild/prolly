use super::vector::ExternalVector;
use super::{PhysicalNodeKind, ProximityEntry, ProximityNode, VectorRef};
use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::{euclidean_radius_up, score};
use crate::prolly::proximity::{DistanceMetric, ProximityConfig};
use std::collections::HashMap;
use xxhash_rust::xxh64::xxh64;

type EncodedNode = (Vec<u8>, HashMap<Cid, Vec<u8>>);

#[derive(Clone, Debug)]
pub(crate) struct NodeSummary {
    pub(crate) cid: Cid,
    pub(crate) count: u64,
    pub(crate) covering_radius: f64,
    pub(crate) min_key: Vec<u8>,
    pub(crate) max_key: Vec<u8>,
    pub(crate) representative_key: Vec<u8>,
    pub(crate) representative_vector: Vec<f32>,
}

pub(crate) fn persist_logical_node(
    kind: PhysicalNodeKind,
    level: u8,
    entries: Vec<ProximityEntry>,
    config: &ProximityConfig,
    objects: &mut HashMap<Cid, Vec<u8>>,
) -> Result<NodeSummary, Error> {
    let logical = ProximityNode {
        kind,
        level,
        subtree_count: count(&entries)?,
        quantizer: None,
        entries,
    };
    if encoded(&logical, config)?.0.len() <= config.overflow.max_page_bytes as usize {
        return persist(logical, config, objects);
    }

    let pages = split(
        logical.entries,
        PhysicalNodeKind::OverflowPage,
        level,
        config,
    )?;
    let mut directory_entries = Vec::with_capacity(pages.len());
    for page in pages {
        let summary = persist(page, config, objects)?;
        directory_entries.push(summary.into_entry());
    }
    persist_directory(level, directory_entries, config, objects)
}

pub(crate) fn summarize(
    cid: Cid,
    entries: &[ProximityEntry],
    representative_key: &[u8],
    representative_vector: &[f32],
) -> Result<NodeSummary, Error> {
    let mut radius = 0.0f64;
    let mut min_key: Option<Vec<u8>> = None;
    let mut max_key: Option<Vec<u8>> = None;
    for entry in entries {
        let vector = entry.vector.inline()?;
        let distance = score(DistanceMetric::L2Squared, representative_vector, vector);
        radius = radius.max(euclidean_radius_up(distance, entry.covering_radius));
        if min_key.as_ref().map_or(true, |key| entry.min_key < *key) {
            min_key = Some(entry.min_key.clone());
        }
        if max_key.as_ref().map_or(true, |key| entry.max_key > *key) {
            max_key = Some(entry.max_key.clone());
        }
    }
    Ok(NodeSummary {
        cid,
        count: count(entries)?,
        covering_radius: radius,
        min_key: min_key.unwrap_or_else(|| representative_key.to_vec()),
        max_key: max_key.unwrap_or_else(|| representative_key.to_vec()),
        representative_key: representative_key.to_vec(),
        representative_vector: representative_vector.to_vec(),
    })
}

impl NodeSummary {
    pub(crate) fn into_entry(self) -> ProximityEntry {
        ProximityEntry {
            key: self.representative_key,
            vector: VectorRef::Inline(self.representative_vector),
            child: Some(self.cid),
            child_count: self.count,
            covering_radius: self.covering_radius,
            min_key: self.min_key,
            max_key: self.max_key,
        }
    }
}

fn persist_directory(
    level: u8,
    entries: Vec<ProximityEntry>,
    config: &ProximityConfig,
    objects: &mut HashMap<Cid, Vec<u8>>,
) -> Result<NodeSummary, Error> {
    let node = ProximityNode {
        kind: PhysicalNodeKind::OverflowDirectory,
        level,
        subtree_count: count(&entries)?,
        quantizer: None,
        entries: entries.clone(),
    };
    if encoded(&node, config)?.0.len() <= config.overflow.max_page_bytes as usize {
        return persist(node, config, objects);
    }

    let groups = split(entries, PhysicalNodeKind::OverflowDirectory, level, config)?;
    if groups.len() <= 1 {
        return too_large(&node, encoded(&node, config)?.0.len(), config);
    }
    let mut parents = Vec::with_capacity(groups.len());
    for group in groups {
        parents.push(persist(group, config, objects)?.into_entry());
    }
    if parents.len() >= node.entries.len() {
        return too_large(&node, encoded(&node, config)?.0.len(), config);
    }
    persist_directory(level, parents, config, objects)
}

fn split(
    entries: Vec<ProximityEntry>,
    kind: PhysicalNodeKind,
    level: u8,
    config: &ProximityConfig,
) -> Result<Vec<ProximityNode>, Error> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    for entry in entries {
        let mut candidate = current.clone();
        candidate.push(entry.clone());
        let candidate_node = physical_node(kind, level, candidate);
        let candidate_size = encoded(&candidate_node, config)?.0.len();
        if candidate_size > config.overflow.max_page_bytes as usize && !current.is_empty() {
            groups.push(physical_node(kind, level, std::mem::take(&mut current)));
            current.push(entry);
        } else {
            current.push(entry);
        }

        let current_node = physical_node(kind, level, current.clone());
        let size = encoded(&current_node, config)?.0.len();
        if size > config.overflow.max_page_bytes as usize {
            return too_large(&current_node, size, config);
        }
        let boundary = size >= config.overflow.target_page_bytes as usize
            || (size >= config.overflow.min_page_bytes as usize
                && xxh64(
                    &current.last().expect("non-empty page").key,
                    config.overflow.hash_seed,
                ) % u64::from(config.overflow.target_page_bytes)
                    < size as u64);
        if boundary {
            groups.push(physical_node(kind, level, std::mem::take(&mut current)));
        }
    }
    if !current.is_empty() {
        groups.push(physical_node(kind, level, current));
    }
    Ok(groups)
}

fn physical_node(kind: PhysicalNodeKind, level: u8, entries: Vec<ProximityEntry>) -> ProximityNode {
    ProximityNode {
        kind,
        level,
        subtree_count: count(&entries).expect("validated counts"),
        quantizer: None,
        entries,
    }
}

fn persist(
    node: ProximityNode,
    config: &ProximityConfig,
    objects: &mut HashMap<Cid, Vec<u8>>,
) -> Result<NodeSummary, Error> {
    let representative = node
        .entries
        .first()
        .ok_or_else(|| Error::InvalidProximityObject {
            kind: "overflow",
            reason: "overflow physical node cannot be empty".to_owned(),
        })?;
    let representative_key = representative.key.clone();
    let representative_vector = representative.vector.inline()?.to_vec();
    let (bytes, vectors) = encoded(&node, config)?;
    if bytes.len() > config.overflow.max_page_bytes as usize {
        return too_large(&node, bytes.len(), config);
    }
    let cid = Cid::from_bytes(&bytes);
    let summary = summarize(
        cid.clone(),
        &node.entries,
        &representative_key,
        &representative_vector,
    )?;
    objects.insert(cid, bytes);
    objects.extend(vectors);
    Ok(summary)
}

fn encoded(node: &ProximityNode, config: &ProximityConfig) -> Result<EncodedNode, Error> {
    let mut stored = node.clone();
    let mut vectors = HashMap::new();
    for entry in &mut stored.entries {
        let VectorRef::Inline(vector) = &entry.vector else {
            continue;
        };
        if vector.len().saturating_mul(4) > config.vector_storage.inline_threshold_bytes as usize {
            let external = ExternalVector {
                vector: vector.clone(),
                norm: None,
            };
            let bytes = external.encode()?;
            let cid = Cid::from_bytes(&bytes);
            vectors.insert(cid.clone(), bytes);
            entry.vector = VectorRef::External(cid);
        }
    }
    Ok((stored.encode()?, vectors))
}

fn count(entries: &[ProximityEntry]) -> Result<u64, Error> {
    entries.iter().try_fold(0u64, |count, entry| {
        count
            .checked_add(entry.child_count)
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "overflow",
                reason: "subtree count overflow".to_owned(),
            })
    })
}

fn too_large<T>(
    node: &ProximityNode,
    encoded_bytes: usize,
    config: &ProximityConfig,
) -> Result<T, Error> {
    Err(Error::ProximityNodeTooLarge {
        level: node.level,
        entries: node.entries.len(),
        encoded_bytes,
        limit: config.overflow.max_page_bytes as usize,
    })
}
