use super::super::cid::Cid;
use super::super::error::Error;
use super::distance::score;
use super::storage::overflow::{persist_empty_leaf, persist_logical_node, summarize, NodeSummary};
use super::storage::{PhysicalNodeKind, ProximityEntry};
use super::vector::promotion_level;
use super::ProximityConfig;
use rayon::prelude::*;
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
    build_hierarchy_at_level_parallel(records, config, None, 1)
}

pub(crate) fn build_hierarchy_parallel(
    records: &[IndexedRecord],
    config: &ProximityConfig,
    threads: usize,
) -> Result<BuiltHierarchy, Error> {
    build_hierarchy_at_level_parallel(records, config, None, threads)
}

pub(crate) fn build_hierarchy_at_level(
    records: &[IndexedRecord],
    config: &ProximityConfig,
    forced_max_level: Option<u8>,
) -> Result<BuiltHierarchy, Error> {
    build_hierarchy_at_level_parallel(records, config, forced_max_level, 1)
}

fn build_hierarchy_at_level_parallel(
    records: &[IndexedRecord],
    config: &ProximityConfig,
    forced_max_level: Option<u8>,
    threads: usize,
) -> Result<BuiltHierarchy, Error> {
    if records.is_empty() {
        let mut objects = HashMap::new();
        let root = persist_empty_leaf(config, &mut objects)?;
        let mut nodes: Vec<_> = objects.into_iter().collect();
        nodes.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
        return Ok(BuiltHierarchy {
            root,
            nodes,
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
    let mut route_maps: Vec<BTreeMap<Vec<usize>, BTreeSet<usize>>> =
        (0..=max_level).map(|_| BTreeMap::new()).collect();
    let mut distance_evaluations = 0usize;

    let pool = (threads > 1)
        .then(|| rayon::ThreadPoolBuilder::new().num_threads(threads).build())
        .transpose()
        .map_err(|error| Error::InvalidProximityConfig {
            reason: format!("cannot create build worker pool: {error}"),
        })?;
    for level in (0..=max_level).rev() {
        let mut ids: Vec<_> = (0..records.len())
            .filter(|&id| levels[id] == level)
            .collect();
        ids.sort_by(|&left, &right| records[left].key.cmp(&records[right].key));
        let compute = || {
            ids.par_iter()
                .map(|&id| {
                    compute_path(id, level, max_level, &route_maps, records, config)
                        .map(|(path, evaluations)| (id, path, evaluations))
                })
                .collect::<Vec<_>>()
        };
        let computed = if let Some(pool) = &pool {
            pool.install(compute)
        } else {
            ids.iter()
                .map(|&id| {
                    compute_path(id, level, max_level, &route_maps, records, config)
                        .map(|(path, evaluations)| (id, path, evaluations))
                })
                .collect()
        };
        for result in computed {
            let (id, mut path, evaluations) = result?;
            distance_evaluations += evaluations;
            register_route(&mut route_maps, max_level, level, &path)?;
            for child_level in (0..level).rev() {
                path.push(id);
                register_route(&mut route_maps, max_level, child_level, &path)?;
            }
        }
    }

    let mut nodes = HashMap::<Cid, Vec<u8>>::new();
    let root = build_node(max_level, &[], &route_maps, records, config, &mut nodes)?.cid;
    let mut nodes: Vec<_> = nodes.into_iter().collect();
    nodes.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
    Ok(BuiltHierarchy {
        root,
        nodes,
        distance_evaluations,
    })
}

fn compute_path(
    id: usize,
    level: u8,
    max_level: u8,
    route_maps: &[BTreeMap<Vec<usize>, BTreeSet<usize>>],
    records: &[IndexedRecord],
    config: &ProximityConfig,
) -> Result<(Vec<usize>, usize), Error> {
    let depth = usize::from(max_level - level);
    let mut path = Vec::with_capacity(usize::from(max_level) + 1);
    let mut evaluations = 0usize;
    for path_depth in 0..depth {
        let lookup_level = usize::from(max_level) - path_depth;
        let candidates =
            route_maps[lookup_level]
                .get(&path)
                .ok_or_else(|| Error::InvalidProximityObject {
                    kind: "builder",
                    reason: "missing representative candidate set".to_owned(),
                })?;
        let mut closest: Option<(usize, f64)> = None;
        for &candidate in candidates {
            evaluations += 1;
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
    Ok((path, evaluations))
}

fn build_node(
    level: u8,
    prefix: &[usize],
    route_maps: &[BTreeMap<Vec<usize>, BTreeSet<usize>>],
    records: &[IndexedRecord],
    config: &ProximityConfig,
    nodes: &mut HashMap<Cid, Vec<u8>>,
) -> Result<NodeSummary, Error> {
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
    let representative_id = prefix.last().copied().unwrap_or(ids[0]);

    let mut entries = Vec::with_capacity(ids.len());
    let mut subtree_count = 0u64;
    for id in ids {
        let entry = if level == 0 {
            subtree_count =
                subtree_count
                    .checked_add(1)
                    .ok_or_else(|| Error::InvalidProximityObject {
                        kind: "builder",
                        reason: "subtree count overflow".to_owned(),
                    })?;
            ProximityEntry::inline_leaf(records[id].key.clone(), records[id].vector.clone())
        } else {
            let mut child_prefix = prefix.to_vec();
            child_prefix.push(id);
            let child = build_node(level - 1, &child_prefix, route_maps, records, config, nodes)?;
            subtree_count = subtree_count.checked_add(child.count).ok_or_else(|| {
                Error::InvalidProximityObject {
                    kind: "builder",
                    reason: "subtree count overflow".to_owned(),
                }
            })?;
            child.into_entry()
        };
        entries.push(entry);
    }
    let kind = if level == 0 {
        PhysicalNodeKind::Leaf
    } else {
        PhysicalNodeKind::Route
    };
    let logical_entries = entries.clone();
    let persisted = persist_logical_node(kind, level, entries, config, nodes)?;
    summarize(
        persisted.cid,
        &logical_entries,
        &records[representative_id].key,
        &records[representative_id].vector,
    )
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
