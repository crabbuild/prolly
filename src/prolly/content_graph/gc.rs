use super::{walk_content_graph, ContentGraphLimits, TypedContentRoot};
use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::store::Store;
use std::collections::HashSet;

/// Global typed-content GC plan over an explicit candidate set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContentGcPlan {
    pub live_cids: Vec<Cid>,
    pub live_objects: usize,
    pub live_bytes: usize,
    pub candidate_objects: usize,
    pub reclaimable_cids: Vec<Cid>,
    pub reclaimable_bytes: usize,
    pub missing_candidates: usize,
}

/// Applied typed-content GC result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContentGcSweep {
    pub plan: ContentGcPlan,
    pub deleted_objects: usize,
    pub deleted_bytes: usize,
}

pub fn plan_content_gc<S: Store>(
    store: &S,
    retained_roots: &[TypedContentRoot],
    candidates: &[Cid],
    limits: &ContentGraphLimits,
) -> Result<ContentGcPlan, Error> {
    let walk = walk_content_graph(store, retained_roots, limits)?;
    let mut live_cids: Vec<_> = walk
        .objects
        .iter()
        .map(|object| object.root.cid.clone())
        .collect();
    live_cids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let live: HashSet<_> = live_cids.iter().cloned().collect();
    let mut unique_candidates = candidates.to_vec();
    unique_candidates.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    unique_candidates.dedup();
    let mut reclaimable_cids = Vec::new();
    let mut reclaimable_bytes = 0usize;
    let mut missing_candidates = 0usize;
    for cid in &unique_candidates {
        if live.contains(cid) {
            continue;
        }
        match store
            .get(cid.as_bytes())
            .map_err(|error| Error::Store(Box::new(error)))?
        {
            Some(bytes) => {
                let actual = Cid::from_bytes(&bytes);
                if actual != *cid {
                    return Err(Error::CidMismatch {
                        expected: cid.clone(),
                        actual,
                    });
                }
                reclaimable_bytes += bytes.len();
                reclaimable_cids.push(cid.clone());
            }
            None => missing_candidates += 1,
        }
    }
    Ok(ContentGcPlan {
        live_objects: live_cids.len(),
        live_cids,
        live_bytes: walk.total_bytes,
        candidate_objects: unique_candidates.len(),
        reclaimable_cids,
        reclaimable_bytes,
        missing_candidates,
    })
}

pub fn sweep_content_gc<S: Store>(
    store: &S,
    retained_roots: &[TypedContentRoot],
    candidates: &[Cid],
    limits: &ContentGraphLimits,
) -> Result<ContentGcSweep, Error> {
    sweep_content_gc_with_invalidator(store, retained_roots, candidates, limits, |_| {})
}

/// Sweep unreachable candidates and invalidate process-local consumers after
/// each successful deletion. Per-object notification keeps caches correct even
/// when a later store deletion fails.
pub fn sweep_content_gc_with_invalidator<S, F>(
    store: &S,
    retained_roots: &[TypedContentRoot],
    candidates: &[Cid],
    limits: &ContentGraphLimits,
    mut invalidate: F,
) -> Result<ContentGcSweep, Error>
where
    S: Store,
    F: FnMut(&Cid),
{
    let plan = plan_content_gc(store, retained_roots, candidates, limits)?;
    for cid in &plan.reclaimable_cids {
        store
            .delete(cid.as_bytes())
            .map_err(|error| Error::Store(Box::new(error)))?;
        invalidate(cid);
    }
    Ok(ContentGcSweep {
        deleted_objects: plan.reclaimable_cids.len(),
        deleted_bytes: plan.reclaimable_bytes,
        plan,
    })
}
