use super::{put_named_content_root_with_limits, ContentRootManifest, ContentRootPublication};
use super::{walk_content_graph, ContentGraphLimits, TypedContentRoot};
use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::manifest::ManifestStore;
use crate::prolly::store::Store;

/// Descendant-first replication result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContentGraphCopy {
    pub required_objects: usize,
    pub required_bytes: usize,
    pub copied_objects: usize,
    pub copied_bytes: usize,
    pub reused_objects: usize,
    pub publication_root: Option<TypedContentRoot>,
}

/// Replicate a closed graph, then publish its immutable typed-root manifest by name.
pub fn copy_and_publish_content_graph<S, D>(
    source: &S,
    destination: &D,
    name: &[u8],
    manifest: ContentRootManifest,
    limits: &ContentGraphLimits,
) -> Result<(ContentGraphCopy, ContentRootPublication), Error>
where
    S: Store,
    D: Store + ManifestStore,
{
    let copy = copy_content_graph(source, destination, manifest.root.clone(), limits)?;
    let publication = put_named_content_root_with_limits(destination, name, manifest, limits)?;
    Ok((copy, publication))
}

pub fn copy_content_graph<S: Store, D: Store>(
    source: &S,
    destination: &D,
    root: TypedContentRoot,
    limits: &ContentGraphLimits,
) -> Result<ContentGraphCopy, Error> {
    let walk = walk_content_graph(source, std::slice::from_ref(&root), limits)?;
    let keys: Vec<_> = walk
        .objects
        .iter()
        .map(|object| object.root.cid.as_bytes())
        .collect();
    let existing = destination
        .batch_get_ordered_unique(&keys)
        .map_err(|error| Error::Store(Box::new(error)))?;
    let mut missing = Vec::new();
    let mut reused = 0usize;
    for (object, present) in walk.objects.iter().zip(existing) {
        if let Some(bytes) = present {
            verify_cid(&object.root.cid, &bytes)?;
            reused += 1;
        } else {
            missing.push(object);
        }
    }
    let mut copied_bytes = 0usize;
    // `walk.objects` and therefore `missing` are descendant-first. Publishing
    // each immutable object in this order keeps every visible parent closed.
    for object in &missing {
        destination
            .put(object.root.cid.as_bytes(), &object.bytes)
            .map_err(|error| Error::Store(Box::new(error)))?;
        copied_bytes += object.bytes.len();
    }
    Ok(ContentGraphCopy {
        required_objects: walk.objects.len(),
        required_bytes: walk.total_bytes,
        copied_objects: missing.len(),
        copied_bytes,
        reused_objects: reused,
        publication_root: Some(root),
    })
}

fn verify_cid(expected: &Cid, bytes: &[u8]) -> Result<(), Error> {
    let actual = Cid::from_bytes(bytes);
    if actual != *expected {
        return Err(Error::CidMismatch {
            expected: expected.clone(),
            actual,
        });
    }
    Ok(())
}
