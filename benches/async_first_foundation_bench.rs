use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use prolly::{AsyncProlly, Config, MemStore, Prolly, SortedBatchBuilder, SyncStoreAsAsync};

fn main() {
    let records = env_usize("PROLLY_FOUNDATION_RECORDS", 100_000);
    let lookups = env_usize("PROLLY_FOUNDATION_LOOKUPS", 1_000).min(records);
    let samples = env_usize("PROLLY_FOUNDATION_SAMPLES", 30).max(3);
    let store = Arc::new(MemStore::new());
    let config = Config::default();
    let mut builder = SortedBatchBuilder::new(store.clone(), config.clone());
    for index in 0..records {
        builder
            .add(key(index), format!("value-{index:016x}").into_bytes())
            .expect("sorted fixture");
    }
    let tree = builder.build().expect("fixture tree");
    let keys = (0..lookups)
        .map(|offset| key((offset * 104_729) % records))
        .collect::<Vec<_>>();
    let sync = Prolly::new(store.clone(), config.clone());
    let asynchronous = AsyncProlly::new(SyncStoreAsAsync::new(store), config);

    assert_eq!(
        sync.get_many(&tree, &keys).expect("sync warmup"),
        block_on(asynchronous.get_many(&tree, &keys)).expect("async warmup")
    );

    println!("facade,api,records,items_per_sample,samples,median_ns,p95_ns,throughput_items_per_sec,peak_rss_bytes");
    emit(
        "sync_ready",
        "get",
        records,
        lookups,
        sample(samples, || {
            for key in &keys {
                black_box(sync.get(&tree, black_box(key)).expect("sync get"));
            }
        }),
    );
    emit(
        "async_adapted",
        "get",
        records,
        lookups,
        sample(samples, || {
            for key in &keys {
                black_box(block_on(asynchronous.get(&tree, black_box(key))).expect("async get"));
            }
        }),
    );
    emit(
        "sync_ready",
        "get_many",
        records,
        lookups,
        sample(samples, || {
            black_box(
                sync.get_many(&tree, black_box(&keys))
                    .expect("sync get_many"),
            );
        }),
    );
    emit(
        "async_adapted",
        "get_many",
        records,
        lookups,
        sample(samples, || {
            black_box(
                block_on(asynchronous.get_many(&tree, black_box(&keys))).expect("async get_many"),
            );
        }),
    );
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:016x}").into_bytes()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn sample(mut samples: usize, mut operation: impl FnMut()) -> Vec<u128> {
    let mut elapsed = Vec::with_capacity(samples);
    while samples > 0 {
        let start = Instant::now();
        operation();
        elapsed.push(start.elapsed().as_nanos());
        samples -= 1;
    }
    elapsed.sort_unstable();
    elapsed
}

fn emit(facade: &str, api: &str, records: usize, items: usize, elapsed: Vec<u128>) {
    let median = elapsed[elapsed.len() / 2];
    let p95 = elapsed[(elapsed.len() * 95).div_ceil(100).saturating_sub(1)];
    let throughput = items as f64 / (median as f64 / 1_000_000_000.0);
    println!(
        "{facade},{api},{records},{items},{},{median},{p95},{throughput:.0},{}",
        elapsed.len(),
        peak_rss_bytes()
    );
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("SyncStoreAsAsync benchmark future returned Pending"),
    }
}

#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the provided rusage pointer on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return 0;
    }
    // macOS reports bytes; other supported Unix platforms report KiB.
    let rss = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        rss
    } else {
        rss.saturating_mul(1024)
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> u64 {
    0
}
