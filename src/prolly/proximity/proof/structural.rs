use super::invalid;
use crate::prolly::cid::Cid;
use crate::prolly::content_graph::{
    walk_content_graph, ContentGraphLimits, ContentObjectKind, TypedContentObject, TypedContentRoot,
};
use crate::prolly::error::Error;
use crate::prolly::proximity::{ProximityMap, ProximityVerification};
use crate::prolly::store::{MemStore, Store};
use std::collections::HashSet;
use std::sync::Arc;

/// Complete typed PRXI closure sufficient for store-independent verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProximityStructuralProof {
    pub descriptor: Cid,
    pub objects: Vec<TypedContentObject>,
}

/// Authenticated structural summary recomputed from proof objects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProximityStructuralVerification {
    pub descriptor: Cid,
    pub object_count: usize,
    pub summary: ProximityVerification,
}

impl ProximityStructuralProof {
    /// Verify against a descriptor CID already trusted by the caller.
    pub fn verify_for(
        &self,
        expected_descriptor: &Cid,
        limits: &ContentGraphLimits,
    ) -> Result<ProximityStructuralVerification, Error> {
        if &self.descriptor != expected_descriptor {
            return Err(invalid("structural proof targets an unexpected descriptor"));
        }
        self.verify(limits)
    }

    /// Rebuild an isolated content store, check exact closure, then run every
    /// proximity routing, radius, summary, vector, and directory invariant.
    pub fn verify(
        &self,
        limits: &ContentGraphLimits,
    ) -> Result<ProximityStructuralVerification, Error> {
        let (map, object_count) = self.verified_map(limits)?;
        let summary = map.verify()?;
        Ok(ProximityStructuralVerification {
            descriptor: self.descriptor.clone(),
            object_count,
            summary,
        })
    }

    pub(super) fn verified_map(
        &self,
        limits: &ContentGraphLimits,
    ) -> Result<(ProximityMap<Arc<MemStore>>, usize), Error> {
        let store = Arc::new(MemStore::new());
        let mut supplied_cids = HashSet::new();
        let mut supplied_shape = HashSet::new();
        for object in &self.objects {
            if Cid::from_bytes(&object.bytes) != object.root.cid
                || !supplied_cids.insert(object.root.cid.clone())
                || !supplied_shape.insert((object.root.clone(), object.depth))
            {
                return Err(invalid("duplicate or CID-invalid structural proof object"));
            }
            Store::put(&store, object.root.cid.as_bytes(), &object.bytes)
                .map_err(|error| Error::Store(Box::new(error)))?;
        }
        let root = TypedContentRoot::proximity_descriptor(self.descriptor.clone());
        let walked = walk_content_graph(&store, &[root], limits)?;
        let reached: HashSet<_> = walked
            .objects
            .iter()
            .map(|object| (object.root.clone(), object.depth))
            .collect();
        if supplied_shape != reached
            || walked.objects.last().map(|object| object.root.kind)
                != Some(ContentObjectKind::ProximityDescriptor)
        {
            return Err(invalid(
                "structural proof is not the exact descriptor closure",
            ));
        }
        let map = ProximityMap::load(store, self.descriptor.clone())?;
        Ok((map, walked.objects.len()))
    }
}

impl<S> ProximityMap<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Capture the authenticated typed closure of this immutable snapshot.
    pub fn prove_structure(
        &self,
        limits: &ContentGraphLimits,
    ) -> Result<ProximityStructuralProof, Error> {
        let root = TypedContentRoot::proximity_descriptor(self.tree().descriptor.clone());
        let walk = walk_content_graph(&self.store_clone(), &[root], limits)?;
        Ok(ProximityStructuralProof {
            descriptor: self.tree().descriptor.clone(),
            objects: walk.objects,
        })
    }
}
