use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{Config, FileNodeStore, Prolly};

fn temp_store_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let path = std::env::temp_dir().join(format!(
        "prolly-inspect-cli-{name}-{}-{nanos}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

fn config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(42)
        .build()
}

fn build_fixture(path: &Path) {
    let store = Arc::new(FileNodeStore::open(path).unwrap());
    let prolly = Prolly::new(store, config());
    let mut main = prolly.create();

    for idx in 0..32 {
        main = prolly
            .put(
                &main,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut feature = main.clone();
    feature = prolly
        .put(&feature, b"k010".to_vec(), b"feature-010".to_vec())
        .unwrap();
    feature = prolly
        .put(&feature, b"k999".to_vec(), b"feature-999".to_vec())
        .unwrap();

    prolly
        .publish_named_root_at_millis(b"main", &main, 1_000)
        .unwrap();
    prolly
        .publish_named_root_at_millis(b"feature", &feature, 2_000)
        .unwrap();
}

fn run_inspect(path: &Path, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_prolly-inspect"))
        .arg(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "command failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn inspect_cli_reports_named_roots_stats_walk_changes_and_reachability() {
    let path = temp_store_dir("fixture");
    build_fixture(&path);

    let roots = run_inspect(&path, &["roots"]);
    assert!(roots.contains("roots=2"));
    assert!(roots.contains("name=\"feature\""));
    assert!(roots.contains("name=\"main\""));

    let stats = run_inspect(&path, &["stats", "main"]);
    assert!(stats.contains("root_name=\"main\""));
    assert!(stats.contains("Tree Structure Statistics"));
    assert!(stats.contains("Key-value pairs:    32"));

    let walk = run_inspect(&path, &["walk", "main"]);
    assert!(walk.contains("level"));
    assert!(walk.contains("fill="));

    let compare = run_inspect(&path, &["compare", "main", "feature"]);
    assert!(compare.contains("shared="));
    assert!(compare.contains("right_only="));

    let changed = run_inspect(&path, &["changed", "main", "feature", "--span-size", "1"]);
    assert!(changed.contains("changes=2"));
    assert!(changed.contains("span=1"));
    assert!(changed.contains("changed=1"));
    assert!(changed.contains("added=1"));

    let verify = run_inspect(&path, &["verify", "--all"]);
    assert!(verify.contains("status=ok"));
    assert!(verify.contains("retained_roots=2"));
    assert!(verify.contains("missing_candidates=0"));

    let _ = fs::remove_dir_all(path);
}
