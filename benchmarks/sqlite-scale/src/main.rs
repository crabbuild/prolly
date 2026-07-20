use prolly_sqlite_scale_bench::cli::{parse_args, USAGE};
use prolly_sqlite_scale_bench::harness::run_matrix;

fn main() {
    let config = match parse_args(std::env::args()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            if !error.contains(USAGE) {
                eprintln!("{USAGE}");
            }
            std::process::exit(2);
        }
    };
    match run_matrix(config) {
        Ok(stats) => println!(
            "complete: {} fixtures built, {} cells measured, {} cells skipped",
            stats.fixtures, stats.measured, stats.skipped
        ),
        Err(error) => {
            eprintln!("benchmark failed: {error}");
            std::process::exit(1);
        }
    }
}
