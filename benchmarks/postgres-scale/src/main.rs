use prolly_postgres_scale_bench::cli::parse_args;
use prolly_postgres_scale_bench::harness::run_matrix;

fn main() {
    let config = match parse_args(std::env::args()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to create Tokio runtime: {error}");
            std::process::exit(2);
        }
    };
    match runtime.block_on(run_matrix(config)) {
        Ok(stats) => println!(
            "benchmark complete: {} rows measured, {} rows skipped",
            stats.measured, stats.skipped
        ),
        Err(error) => {
            eprintln!("benchmark failed: {error}");
            std::process::exit(1);
        }
    }
}
