#![no_main]

use libfuzzer_sys::fuzz_target;
use prolly::{
    walk_content_graph, Cid, ContentGraphLimits, ContentObjectKind, MemStore, Store,
    TurboQuantizer, TypedContentRoot,
};
use std::sync::Arc;

const MAX_INPUT_BYTES: usize = 4 * 1024;

fuzz_target!(|input: &[u8]| {
    let bytes = &input[..input.len().min(MAX_INPUT_BYTES)];
    let store = Arc::new(MemStore::new());
    let manifest = Cid::from_bytes(bytes);
    Store::put(&store, manifest.as_bytes(), bytes).expect("in-memory publication");

    let _ = TurboQuantizer::load(store.clone(), manifest.clone());
    let limits = ContentGraphLimits {
        max_objects: 64,
        max_depth: 16,
        max_bytes: 4 * MAX_INPUT_BYTES,
        max_references_per_object: 64,
    };
    let root = TypedContentRoot::new(ContentObjectKind::TurboQuantization, manifest);
    let _ = walk_content_graph(&store, &[root], &limits);
});
