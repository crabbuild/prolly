use std::sync::Arc;

use prolly::{AsyncProlly, AsyncSortedBatchBuilder, Cid, Config};
use prolly_s3_core::{encode_canonical, ContentChunkRef, MemoryObjectPlane, ProllyObjectStore};

#[tokio::test]
async fn million_chunk_index_builds_incrementally_and_seeks_by_offset() {
    if std::env::var("PROLLY_S3_MILLION_CHUNK_TEST").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_MILLION_CHUNK_TEST=1 to run the million-chunk qualification");
        return;
    }
    const COUNT: u64 = 1_000_000;
    const CHUNK_BYTES: u64 = 8 * 1_024 * 1_024;
    let plane = Arc::new(MemoryObjectPlane::new(false));
    let store = ProllyObjectStore::new(plane, "million-chunk-index");
    let config = Config::default();
    let mut builder = AsyncSortedBatchBuilder::new(store.clone(), config.clone());
    for index in 0..COUNT {
        let seed = index.to_be_bytes();
        builder
            .add(
                (index * CHUNK_BYTES).to_be_bytes().to_vec(),
                encode_canonical(&ContentChunkRef {
                    cid: Cid::from_bytes(&seed),
                    len: CHUNK_BYTES as u32,
                })
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let tree = builder.build().await.unwrap();
    let engine = AsyncProlly::new(store, config);
    for index in [0, 1, COUNT / 2, COUNT - 2, COUNT - 1] {
        let offset = index * CHUNK_BYTES;
        let mut range = engine
            .range(&tree, &offset.to_be_bytes(), None)
            .await
            .unwrap();
        let (key, value) = range.next().await.unwrap().unwrap();
        assert_eq!(key, offset.to_be_bytes());
        let chunk: ContentChunkRef = prolly_s3_core::decode_canonical(&value).unwrap();
        assert_eq!(chunk.len, CHUNK_BYTES as u32);
        assert_eq!(chunk.cid, Cid::from_bytes(&index.to_be_bytes()));
    }
}
