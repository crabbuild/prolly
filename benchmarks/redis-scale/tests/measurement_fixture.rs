use prolly_redis_scale_bench::fixture::FixtureLayout;
use prolly_redis_scale_bench::measurement::nearest_rank;
use prolly_redis_scale_bench::model::{CacheState, CellSpec, Operation, Pattern};

#[test]
fn nearest_rank_uses_one_based_ceiling() {
    assert_eq!(nearest_rank(&[10, 20, 30, 40, 50], 0.50), Some(30));
    assert_eq!(nearest_rank(&[10, 20, 30, 40, 50], 0.95), Some(50));
    assert_eq!(nearest_rank(&[], 0.99), None);
}

#[test]
fn fixture_namespaces_are_isolated_and_stable() {
    let layout = FixtureLayout::new("redis://127.0.0.1:6379/".to_string(), 100, 2);
    let spec = CellSpec {
        records: 100,
        repetition: 2,
        operation: Operation::GetCold,
        pattern: Pattern::Random,
        cache_state: CacheState::ColdManager,
        changes: 10,
        read_samples: 10,
        revision: "test".to_string(),
        dirty: false,
    };
    assert_eq!(
        layout.source_prefix(),
        b"prolly:redis-scale:100:run:2:source:".to_vec()
    );
    assert_eq!(
        layout.cell_prefix(&spec),
        b"prolly:redis-scale:100:run:2:cell:get_cold:random:cold-manager:".to_vec()
    );
    assert_ne!(layout.source_prefix(), layout.cell_prefix(&spec));
}
