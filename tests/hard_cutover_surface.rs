use std::fs;
use std::path::{Path, PathBuf};

fn source_files(root: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            source_files(&path, out);
        } else if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("rs" | "py" | "go" | "ts" | "kt" | "java" | "swift" | "rb")
        ) {
            out.push(path);
        }
    }
}

#[test]
fn legacy_chunking_and_rebalancing_surfaces_are_absent() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!manifest.join("src/prolly/rebalance.rs").exists());

    let forbidden = [
        ["is_boundary", "_config"].concat(),
        ["is_hash_boundary", "_config"].concat(),
        ["Parallel", "Rebalancer"].concat(),
        ["DefaultParallel", "Rebalancer"].concat(),
        ["rebalance", "::"].concat(),
        ["BatchWriter", "Config"].concat(),
    ];
    let mut files = Vec::new();
    for directory in ["src", "bindings"] {
        source_files(&manifest.join(directory), &mut files);
    }

    for path in files {
        let source = fs::read_to_string(&path).unwrap();
        for identifier in &forbidden {
            assert!(
                !source.contains(identifier),
                "legacy identifier {identifier:?} remains in {}",
                path.display()
            );
        }
    }
}
