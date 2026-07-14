use super::super::cid::Cid;
use super::super::error::Error;
use super::distance::score;
use super::storage::{PhysicalNodeKind, ProximityEntry, ProximityNode, VectorRef};
use super::vector::promotion_level;
use super::ProximityConfig;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Clone, Debug)]
pub(crate) struct IndexedRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) vector: Vec<f32>,
}

pub(crate) struct BuiltHierarchy {
    pub(crate) root: Cid,
    pub(crate) nodes: Vec<(Cid, Vec<u8>)>,
    pub(crate) distance_evaluations: usize,
}

pub(crate) fn build_hierarchy(
    records: &[IndexedRecord],
    config: &ProximityConfig,
) -> Result<BuiltHierarchy, Error> {
    build_hierarchy_at_level(records, config, None)
}

pub(crate) fn build_hierarchy_at_level(
    records: &[IndexedRecord],
    config: &ProximityConfig,
    forced_max_level: Option<u8>,
) -> Result<BuiltHierarchy, Error> {
    if records.is_empty() {
        let node = ProximityNode {
            kind: PhysicalNodeKind::Leaf,
            level: 0,
            subtree_count: 0,
            quantizer: None,
            entries: Vec::new(),
        };
        let bytes = node.encode()?;
        enforce_size(&node, bytes.len(), config)?;
        let cid = Cid::from_bytes(&bytes);
        return Ok(BuiltHierarchy {
            root: cid.clone(),
            nodes: vec![(cid, bytes)],
            distance_evaluations: 0,
        });
    }

    let levels: Vec<u8> = records
        .iter()
        .map(|record| {
            let level = promotion_level(
                &record.key,
                config.hierarchy.log_chunk_size,
                config.hierarchy.level_hash_seed,
            );
            forced_max_level.map_or(level, |maximum| level.min(maximum))
        })
        .collect();
    let max_level =
        forced_max_level.unwrap_or_else(|| *levels.iter().max().expect("non-empty levels"));
    let mut ordered: Vec<usize> = (0..records.len()).collect();
    ordered.sort_by(|&left, &right| {
        levels[right]
            .cmp(&levels[left])
            .then_with(|| records[left].key.cmp(&records[right].key))
    });

    let mut route_maps: Vec<BTreeMap<Vec<usize>, BTreeSet<usize>>> =
        (0..=max_level).map(|_| BTreeMap::new()).collect();
    let mut distance_evaluations = 0usize;

    for id in ordered {
        let level = levels[id];
        let depth = usize::from(max_level - level);
        let mut path = Vec::with_capacity(usize::from(max_level) + 1);
        for path_depth in 0..depth {
            let lookup_level = usize::from(max_level) - path_depth;
            let candidates = route_maps[lookup_level].get(&path).ok_or_else(|| {
                Error::InvalidProximityObject {
                    kind: "builder",
                    reason: "missing representative candidate set".to_owned(),
                }
            })?;
            let mut closest: Option<(usize, f64)> = None;
            for &candidate in candidates {
                distance_evaluations += 1;
                let distance = score(
                    config.metric,
                    &records[id].vector,
                    &records[candidate].vector,
                );
                if closest.map_or(true, |(best, best_distance)| {
                    distance
                        .total_cmp(&best_distance)
                        .then_with(|| records[candidate].key.cmp(&records[best].key))
                        .is_lt()
                }) {
                    closest = Some((candidate, distance));
                }
            }
            let closest = closest.map(|(candidate, _)| candidate).ok_or_else(|| {
                Error::InvalidProximityObject {
                    kind: "builder",
                    reason: "missing representative candidate".to_owned(),
                }
            })?;
            path.push(closest);
        }
        path.push(id);
        register_route(&mut route_maps, max_level, level, &path)?;
        for child_level in (0..level).rev() {
            path.push(id);
            register_route(&mut route_maps, max_level, child_level, &path)?;
        }
    }

    let mut nodes = HashMap::<Cid, Vec<u8>>::new();
    let root = build_node(max_level, &[], &route_maps, records, config, &mut nodes)?.0;
    let mut nodes: Vec<_> = nodes.into_iter().collect();
    nodes.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
    Ok(BuiltHierarchy {
        root,
        nodes,
        distance_evaluations,
    })
}

fn build_node(
    level: u8,
    prefix: &[usize],
    route_maps: &[BTreeMap<Vec<usize>, BTreeSet<usize>>],
    records: &[IndexedRecord],
    config: &ProximityConfig,
    nodes: &mut HashMap<Cid, Vec<u8>>,
) -> Result<(Cid, u64), Error> {
    let mut ids: Vec<_> = route_maps[usize::from(level)]
        .get(prefix)
        .ok_or_else(|| Error::InvalidProximityObject {
            kind: "builder",
            reason: "missing serialized route group".to_owned(),
        })?
        .iter()
        .copied()
        .collect();
    ids.sort_by(|&left, &right| records[left].key.cmp(&records[right].key));

    let mut entries = Vec::with_capacity(ids.len());
    let mut subtree_count = 0u64;
    for id in ids {
        let (child, child_count) = if level == 0 {
            subtree_count =
                subtree_count
                    .checked_add(1)
                    .ok_or_else(|| Error::InvalidProximityObject {
                        kind: "builder",
                        reason: "subtree count overflow".to_owned(),
                    })?;
            (None, 1)
        } else {
            let mut child_prefix = prefix.to_vec();
            child_prefix.push(id);
            let (child, child_count) =
                build_node(level - 1, &child_prefix, route_maps, records, config, nodes)?;
            subtree_count = subtree_count.checked_add(child_count).ok_or_else(|| {
                Error::InvalidProximityObject {
                    kind: "builder",
                    reason: "subtree count overflow".to_owned(),
                }
            })?;
            (Some(child), child_count)
        };
        let key = records[id].key.clone();
        entries.push(ProximityEntry {
            min_key: key.clone(),
            max_key: key.clone(),
            key,
            vector: VectorRef::Inline(records[id].vector.clone()),
            child,
            child_count,
            covering_radius: 0.0,
        });
    }
    let node = ProximityNode {
        kind: if level == 0 {
            PhysicalNodeKind::Leaf
        } else {
            PhysicalNodeKind::Route
        },
        level,
        subtree_count,
        quantizer: None,
        entries,
    };
    let bytes = node.encode()?;
    enforce_size(&node, bytes.len(), config)?;
    let cid = Cid::from_bytes(&bytes);
    nodes.insert(cid.clone(), bytes);
    Ok((cid, subtree_count))
}

fn register_route(
    route_maps: &mut [BTreeMap<Vec<usize>, BTreeSet<usize>>],
    max_level: u8,
    level: u8,
    path: &[usize],
) -> Result<(), Error> {
    let component = usize::from(max_level - level);
    let id = *path
        .get(component)
        .ok_or_else(|| Error::InvalidProximityObject {
            kind: "builder",
            reason: "route path is shorter than its logical level".to_owned(),
        })?;
    route_maps[usize::from(level)]
        .entry(path[..component].to_vec())
        .or_default()
        .insert(id);
    Ok(())
}

fn enforce_size(
    node: &ProximityNode,
    encoded_bytes: usize,
    config: &ProximityConfig,
) -> Result<(), Error> {
    let limit = config.overflow.max_page_bytes as usize;
    if encoded_bytes > limit {
        return Err(Error::ProximityNodeTooLarge {
            level: node.level,
            entries: node.entries.len(),
            encoded_bytes,
            limit,
        });
    }
    Ok(())
}
