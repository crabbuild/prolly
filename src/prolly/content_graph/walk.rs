use super::kind::{ContentObjectKind, TypedContentObject, TypedContentRoot};
use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::node::Node;
use crate::prolly::proximity::accelerator::hnsw::storage::{GraphNode, Manifest as HnswManifest};
use crate::prolly::proximity::accelerator::pq::Manifest as PqManifest;
use crate::prolly::proximity::storage::quantized::ScalarQuantized;
use crate::prolly::proximity::storage::vector::ExternalVector;
use crate::prolly::proximity::storage::{Descriptor, PhysicalNodeKind, ProximityNode, VectorRef};
use crate::prolly::store::Store;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Hard limits for untrusted typed graph traversal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentGraphLimits {
    pub max_objects: usize,
    pub max_depth: usize,
    pub max_bytes: usize,
    pub max_references_per_object: usize,
}

impl Default for ContentGraphLimits {
    fn default() -> Self {
        Self {
            max_objects: 10_000_000,
            max_depth: 256,
            max_bytes: usize::try_from(64_u64 * 1024 * 1024 * 1024).unwrap_or(usize::MAX),
            max_references_per_object: 16_777_216,
        }
    }
}

/// Deterministic descendant-first traversal report.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContentGraphWalk {
    pub objects: Vec<TypedContentObject>,
    pub total_bytes: usize,
    pub maximum_depth: usize,
    pub objects_by_kind: BTreeMap<ContentObjectKind, usize>,
}

pub fn walk_content_graph<S: Store>(
    store: &S,
    roots: &[TypedContentRoot],
    limits: &ContentGraphLimits,
) -> Result<ContentGraphWalk, Error> {
    validate_limits(limits)?;
    let mut ordered_roots = roots.to_vec();
    ordered_roots.sort_by(compare_root);
    ordered_roots.dedup();
    let mut stack: Vec<_> = ordered_roots
        .into_iter()
        .rev()
        .map(|root| Frame::Enter(root, 0))
        .collect();
    let mut visiting = HashSet::<Cid>::new();
    let mut seen = HashSet::<Cid>::new();
    let mut contexts = HashMap::<Cid, (ContentObjectKind, Option<u32>)>::new();
    let mut pending = HashMap::<Cid, TypedContentObject>::new();
    let mut walk = ContentGraphWalk::default();
    let mut loaded_bytes = 0usize;

    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter(root, depth) => {
                if let Some((kind, dimensions)) = contexts.get(&root.cid) {
                    if *dimensions != root.dimensions || !compatible_kinds(*kind, root.kind) {
                        return Err(invalid(
                            "the same CID was referenced with conflicting type context",
                        ));
                    }
                } else {
                    contexts.insert(root.cid.clone(), (root.kind, root.dimensions));
                    if contexts.len() > limits.max_objects {
                        return Err(limit("objects", limits.max_objects, contexts.len()));
                    }
                }
                if seen.contains(&root.cid) {
                    continue;
                }
                if !visiting.insert(root.cid.clone()) {
                    return Err(invalid("cycle detected in typed content graph"));
                }
                if depth > limits.max_depth {
                    return Err(limit("depth", limits.max_depth, depth));
                }
                let bytes = load_content(store, &root.cid)?;
                let next_bytes = loaded_bytes.saturating_add(bytes.len());
                if next_bytes > limits.max_bytes {
                    return Err(limit("bytes", limits.max_bytes, next_bytes));
                }
                loaded_bytes = next_bytes;
                let (actual_kind, mut references) = references(&root, &bytes)?;
                if references.len() > limits.max_references_per_object {
                    return Err(limit(
                        "references",
                        limits.max_references_per_object,
                        references.len(),
                    ));
                }
                references.sort_by(compare_root);
                references.dedup();
                let object = TypedContentObject {
                    root: TypedContentRoot {
                        kind: actual_kind,
                        cid: root.cid.clone(),
                        dimensions: root.dimensions,
                    },
                    bytes,
                    depth,
                };
                pending.insert(root.cid.clone(), object);
                stack.push(Frame::Exit(root.cid));
                for reference in references.into_iter().rev() {
                    stack.push(Frame::Enter(reference, depth + 1));
                }
            }
            Frame::Exit(cid) => {
                visiting.remove(&cid);
                if seen.insert(cid.clone()) {
                    let object = pending.remove(&cid).expect("entered content object");
                    walk.total_bytes = walk.total_bytes.saturating_add(object.bytes.len());
                    walk.maximum_depth = walk.maximum_depth.max(object.depth);
                    *walk.objects_by_kind.entry(object.root.kind).or_default() += 1;
                    walk.objects.push(object);
                }
            }
        }
    }
    Ok(walk)
}

/// Load one authenticated object and return its exact, deterministic typed
/// reference list without recursively walking descendants.
pub fn content_references<S: Store>(
    store: &S,
    root: &TypedContentRoot,
) -> Result<Vec<TypedContentRoot>, Error> {
    let bytes = load_content(store, &root.cid)?;
    let (_, mut output) = references(root, &bytes)?;
    output.sort_by(compare_root);
    output.dedup();
    Ok(output)
}

fn compatible_kinds(left: ContentObjectKind, right: ContentObjectKind) -> bool {
    left == right || (is_proximity_node_kind(left) && is_proximity_node_kind(right))
}

fn is_proximity_node_kind(kind: ContentObjectKind) -> bool {
    matches!(
        kind,
        ContentObjectKind::ProximityNode
            | ContentObjectKind::OverflowDirectory
            | ContentObjectKind::OverflowPage
    )
}

enum Frame {
    Enter(TypedContentRoot, usize),
    Exit(Cid),
}

fn references(
    root: &TypedContentRoot,
    bytes: &[u8],
) -> Result<(ContentObjectKind, Vec<TypedContentRoot>), Error> {
    validate_root_context(root)?;
    let mut output = Vec::new();
    let actual = match root.kind {
        ContentObjectKind::OrderedNode => {
            let node = Node::from_bytes(bytes)?;
            if node.keys.len() != node.vals.len() {
                return Err(invalid("ordered node key/value count mismatch"));
            }
            if !node.leaf {
                for value in node.vals {
                    let cid = Cid(value
                        .try_into()
                        .map_err(|_| invalid("ordered internal value is not a CID"))?);
                    output.push(TypedContentRoot::new(ContentObjectKind::OrderedNode, cid));
                }
            }
            ContentObjectKind::OrderedNode
        }
        ContentObjectKind::ProximityDescriptor => {
            let descriptor = Descriptor::decode(bytes)?;
            if let Some(cid) = descriptor.directory.root {
                output.push(TypedContentRoot::new(ContentObjectKind::OrderedNode, cid));
            }
            output.push(
                TypedContentRoot::new(ContentObjectKind::ProximityNode, descriptor.proximity_root)
                    .with_dimensions(descriptor.config.dimensions),
            );
            ContentObjectKind::ProximityDescriptor
        }
        ContentObjectKind::ProximityNode
        | ContentObjectKind::OverflowDirectory
        | ContentObjectKind::OverflowPage => {
            let dimensions = root
                .dimensions
                .ok_or_else(|| invalid("PRXN traversal requires dimensions"))?;
            let node = ProximityNode::decode(bytes, dimensions)?;
            let actual = match node.kind {
                PhysicalNodeKind::OverflowDirectory => ContentObjectKind::OverflowDirectory,
                PhysicalNodeKind::OverflowPage => ContentObjectKind::OverflowPage,
                PhysicalNodeKind::Leaf | PhysicalNodeKind::Route => {
                    ContentObjectKind::ProximityNode
                }
            };
            if root.kind != ContentObjectKind::ProximityNode && root.kind != actual {
                return Err(invalid("PRXN physical kind disagrees with typed reference"));
            }
            if let Some(cid) = node.quantizer {
                output.push(TypedContentRoot::new(
                    ContentObjectKind::ScalarQuantization,
                    cid,
                ));
            }
            for entry in node.entries {
                if let VectorRef::External(cid) = entry.vector {
                    output.push(TypedContentRoot::new(
                        ContentObjectKind::ExternalVector,
                        cid,
                    ));
                }
                if let Some(cid) = entry.child {
                    output.push(
                        TypedContentRoot::new(ContentObjectKind::ProximityNode, cid)
                            .with_dimensions(dimensions),
                    );
                }
            }
            actual
        }
        ContentObjectKind::ExternalVector => {
            ExternalVector::decode(bytes)?;
            ContentObjectKind::ExternalVector
        }
        ContentObjectKind::ScalarQuantization => {
            ScalarQuantized::decode(bytes)?;
            ContentObjectKind::ScalarQuantization
        }
        ContentObjectKind::ProductQuantization => {
            let manifest = PqManifest::decode(bytes)?;
            manifest.config.validate(
                manifest.dimensions,
                usize::from(manifest.config.centroids_per_subquantizer),
            )?;
            output.push(TypedContentRoot::proximity_descriptor(manifest.source));
            output.push(TypedContentRoot::new(
                ContentObjectKind::OrderedNode,
                manifest.code_root,
            ));
            ContentObjectKind::ProductQuantization
        }
        ContentObjectKind::HnswManifest => {
            let manifest = HnswManifest::decode(bytes)?;
            manifest.config.validate()?;
            output.push(TypedContentRoot::proximity_descriptor(manifest.source));
            output.push(TypedContentRoot::new(
                ContentObjectKind::OrderedNode,
                manifest.graph_root,
            ));
            ContentObjectKind::HnswManifest
        }
        ContentObjectKind::HnswPage => {
            GraphNode::decode(bytes)?;
            ContentObjectKind::HnswPage
        }
    };
    Ok((actual, output))
}

fn validate_root_context(root: &TypedContentRoot) -> Result<(), Error> {
    if is_proximity_node_kind(root.kind) != root.dimensions.is_some() {
        return Err(invalid(
            "dimensions must be present exactly for PRXN-family objects",
        ));
    }
    Ok(())
}

fn compare_root(left: &TypedContentRoot, right: &TypedContentRoot) -> std::cmp::Ordering {
    left.kind
        .cmp(&right.kind)
        .then_with(|| left.cid.as_bytes().cmp(right.cid.as_bytes()))
        .then_with(|| left.dimensions.cmp(&right.dimensions))
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

fn validate_limits(limits: &ContentGraphLimits) -> Result<(), Error> {
    if limits.max_objects == 0 || limits.max_bytes == 0 || limits.max_references_per_object == 0 {
        return Err(invalid("content graph limits must be greater than zero"));
    }
    Ok(())
}

fn limit(resource: &'static str, limit: usize, actual: usize) -> Error {
    Error::ContentGraphResourceLimitExceeded {
        resource,
        limit,
        actual,
    }
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "content graph",
        reason: reason.into(),
    }
}
