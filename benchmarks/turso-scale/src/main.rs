use prolly_turso_scale_bench::cli::{parse_args, USAGE};
use prolly_turso_scale_bench::harness::run_matrix;

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
    let workers = config.tokio_workers;
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to create Tokio runtime: {error}");
            std::process::exit(1);
        }
    };
    match runtime.block_on(run_matrix(config)) {
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
