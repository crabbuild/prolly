use std::path::PathBuf;
use std::sync::Arc;

use prolly::{debug_key, Config, Diff, FileNodeStore, Prolly, Tree};

type CliResult<T> = Result<T, String>;
type FileProlly = Prolly<Arc<FileNodeStore>>;

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        eprintln!();
        eprintln!("{}", usage());
        std::process::exit(2);
    }
}

fn run() -> CliResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{}", usage());
        return Ok(());
    }

    if args.len() < 2 {
        return Err("missing <file-store-dir> and <command>".to_string());
    }

    let store_dir = PathBuf::from(&args[0]);
    let command = args[1].as_str();
    let rest = &args[2..];
    let store = Arc::new(
        FileNodeStore::open(&store_dir)
            .map_err(|err| format!("failed to open file node store: {err}"))?,
    );
    let prolly = Prolly::new(store, Config::default());

    match command {
        "roots" => roots(&prolly, rest),
        "stats" => stats(&prolly, rest),
        "walk" => walk(&prolly, rest),
        "compare" => compare(&prolly, rest),
        "changed" => changed(&prolly, rest),
        "verify" => verify(&prolly, rest),
        other => Err(format!("unknown command: {other}")),
    }
}

fn roots(prolly: &FileProlly, args: &[String]) -> CliResult<()> {
    expect_arg_count("roots", args, 0)?;
    let roots = prolly
        .list_named_root_manifests()
        .map_err(|err| format!("failed to list named roots: {err}"))?;

    println!("roots={}", roots.len());
    for root in roots {
        println!(
            "name={} root={} created_at_millis={} updated_at_millis={} min_chunk_size={} max_chunk_size={} chunking_factor={}",
            debug_key(&root.name),
            format_optional_cid(root.manifest.root.as_ref()),
            format_optional_u64(root.manifest.created_at_millis),
            format_optional_u64(root.manifest.updated_at_millis),
            root.manifest.config.min_chunk_size,
            root.manifest.config.max_chunk_size,
            root.manifest.config.chunking_factor
        );
    }
    Ok(())
}

fn stats(prolly: &FileProlly, args: &[String]) -> CliResult<()> {
    expect_arg_count("stats", args, 1)?;
    let tree = load_named_root(prolly, &args[0])?;
    println!("root_name={}", debug_key(args[0].as_bytes()));
    println!("root={}", format_optional_cid(tree.root.as_ref()));
    println!("{}", prolly.collect_stats(&tree).map_err(format_error)?);
    Ok(())
}

fn walk(prolly: &FileProlly, args: &[String]) -> CliResult<()> {
    expect_arg_count("walk", args, 1)?;
    let tree = load_named_root(prolly, &args[0])?;
    println!("root_name={}", debug_key(args[0].as_bytes()));
    println!("root={}", format_optional_cid(tree.root.as_ref()));
    println!(
        "{}",
        prolly.debug_tree(&tree).map_err(format_error)?.to_text()
    );
    Ok(())
}

fn compare(prolly: &FileProlly, args: &[String]) -> CliResult<()> {
    expect_arg_count("compare", args, 2)?;
    let left = load_named_root(prolly, &args[0])?;
    let right = load_named_root(prolly, &args[1])?;
    println!("left_name={}", debug_key(args[0].as_bytes()));
    println!("left_root={}", format_optional_cid(left.root.as_ref()));
    println!("right_name={}", debug_key(args[1].as_bytes()));
    println!("right_root={}", format_optional_cid(right.root.as_ref()));
    println!(
        "{}",
        prolly
            .debug_compare_trees(&left, &right)
            .map_err(format_error)?
            .to_text()
    );
    Ok(())
}

fn changed(prolly: &FileProlly, args: &[String]) -> CliResult<()> {
    if args.len() != 2 && args.len() != 4 {
        return Err(
            "changed expects <base-root-name> <other-root-name> [--span-size N]".to_string(),
        );
    }
    let span_size = parse_span_size(&args[2..])?;
    let base = load_named_root(prolly, &args[0])?;
    let other = load_named_root(prolly, &args[1])?;
    let mut diffs = prolly.diff(&base, &other).map_err(format_error)?;
    diffs.sort_by(|left, right| left.key().cmp(right.key()));

    println!("base_name={}", debug_key(args[0].as_bytes()));
    println!("base_root={}", format_optional_cid(base.root.as_ref()));
    println!("other_name={}", debug_key(args[1].as_bytes()));
    println!("other_root={}", format_optional_cid(other.root.as_ref()));
    println!(
        "changes={} span_size={} spans={}",
        diffs.len(),
        span_size,
        diffs.len().div_ceil(span_size)
    );

    for (idx, chunk) in diffs.chunks(span_size).enumerate() {
        let counts = DiffCounts::from_diffs(chunk);
        let first = chunk
            .first()
            .map(|diff| debug_key(diff.key()))
            .unwrap_or_else(|| "-".to_string());
        let last = chunk
            .last()
            .map(|diff| debug_key(diff.key()))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "span={} first={} last={} changes={} added={} removed={} changed={}",
            idx + 1,
            first,
            last,
            chunk.len(),
            counts.added,
            counts.removed,
            counts.changed
        );
    }

    Ok(())
}

fn verify(prolly: &FileProlly, args: &[String]) -> CliResult<()> {
    expect_arg_count("verify", args, 1)?;
    let named_roots = if args[0] == "--all" {
        prolly
            .list_named_roots()
            .map_err(|err| format!("failed to list named roots: {err}"))?
    } else {
        vec![prolly::NamedRoot::new(
            args[0].as_bytes().to_vec(),
            load_named_root(prolly, &args[0])?,
        )]
    };

    let roots = named_roots
        .iter()
        .map(|root| root.tree.clone())
        .collect::<Vec<_>>();
    let plan = prolly
        .plan_store_gc(&roots)
        .map_err(|err| format!("store reachability verification failed: {err}"))?;
    let status = if named_roots.is_empty() {
        "no_roots"
    } else {
        "ok"
    };

    println!("status={status}");
    println!("retained_roots={}", named_roots.len());
    for root in &named_roots {
        println!(
            "retained name={} root={}",
            debug_key(&root.name),
            format_optional_cid(root.tree.root.as_ref())
        );
    }
    println!("stored_nodes={}", plan.candidate_nodes);
    println!("reachable_nodes={}", plan.reachability.live_nodes);
    println!("reachable_bytes={}", plan.reachability.live_bytes);
    println!("leaf_nodes={}", plan.reachability.leaf_nodes);
    println!("internal_nodes={}", plan.reachability.internal_nodes);
    println!("unreachable_nodes={}", plan.reclaimable_nodes);
    println!("unreachable_bytes={}", plan.reclaimable_bytes);
    println!("missing_candidates={}", plan.missing_candidates);
    Ok(())
}

#[derive(Default)]
struct DiffCounts {
    added: usize,
    removed: usize,
    changed: usize,
}

impl DiffCounts {
    fn from_diffs(diffs: &[Diff]) -> Self {
        let mut counts = Self::default();
        for diff in diffs {
            match diff {
                Diff::Added { .. } => counts.added += 1,
                Diff::Removed { .. } => counts.removed += 1,
                Diff::Changed { .. } => counts.changed += 1,
            }
        }
        counts
    }
}

fn load_named_root(prolly: &FileProlly, name: &str) -> CliResult<Tree> {
    prolly
        .load_named_root(name.as_bytes())
        .map_err(|err| format!("failed to load named root {name:?}: {err}"))?
        .ok_or_else(|| format!("named root not found: {name:?}"))
}

fn parse_span_size(args: &[String]) -> CliResult<usize> {
    if args.is_empty() {
        return Ok(64);
    }
    if args.len() == 2 && args[0] == "--span-size" {
        let parsed = args[1]
            .parse::<usize>()
            .map_err(|_| format!("invalid --span-size value: {:?}", args[1]))?;
        return if parsed == 0 {
            Err("--span-size must be greater than zero".to_string())
        } else {
            Ok(parsed)
        };
    }
    Err("changed supports only the optional form: --span-size N".to_string())
}

fn expect_arg_count(command: &str, args: &[String], expected: usize) -> CliResult<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(format!(
            "{command} expects {expected} argument(s), got {}",
            args.len()
        ))
    }
}

fn format_optional_cid(cid: Option<&prolly::Cid>) -> String {
    cid.map(cid_hex).unwrap_or_else(|| "empty".to_string())
}

fn cid_hex(cid: &prolly::Cid) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in cid.as_bytes() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn format_error(error: prolly::Error) -> String {
    error.to_string()
}

fn usage() -> &'static str {
    "usage:
  prolly-inspect <file-store-dir> roots
  prolly-inspect <file-store-dir> stats <root-name>
  prolly-inspect <file-store-dir> walk <root-name>
  prolly-inspect <file-store-dir> compare <left-root-name> <right-root-name>
  prolly-inspect <file-store-dir> changed <base-root-name> <other-root-name> [--span-size N]
  prolly-inspect <file-store-dir> verify <root-name|--all>

commands:
  roots     List named root manifests in a FileNodeStore.
  stats     Print TreeStats for a named root.
  walk      Render tree levels, node fill factors, and key ranges.
  compare   Show shared, left-only, and right-only subtrees between roots.
  changed   Show coalesced changed-key spans between roots.
  verify    Dry-run reachability from one root or all named roots."
}
