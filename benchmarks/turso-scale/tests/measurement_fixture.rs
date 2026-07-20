use prolly_turso_scale_bench::fixture::{clone_fixture, directory_bytes};
use prolly_turso_scale_bench::measurement::nearest_rank;

#[test]
fn nearest_rank_uses_one_based_ceiling() {
    assert_eq!(nearest_rank(&[10, 20, 30, 40, 50], 0.50), Some(30));
    assert_eq!(nearest_rank(&[10, 20, 30, 40, 50], 0.95), Some(50));
    assert_eq!(nearest_rank(&[], 0.99), None);
}

#[test]
fn closed_fixture_clone_copies_all_regular_files() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let destination = temp.path().join("destination");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("prolly.db"), b"database").unwrap();
    std::fs::write(source.join("prolly.db-wal"), b"wal").unwrap();
    clone_fixture(&source, &destination).unwrap();
    assert_eq!(directory_bytes(&destination).unwrap(), 11);
}
