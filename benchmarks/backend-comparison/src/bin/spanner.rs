use prolly_backend_comparison::{
    parse_binary_args, run_spanner, run_spanner_service, write_rows_new, write_service_rows_new,
    Backend, ConnectionConfig, Suite,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Spanner comparison failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = parse_binary_args(Backend::Spanner, std::env::args().collect())?;
    let ConnectionConfig::Spanner { database } = &config.connection else {
        return Err("Spanner binary received a non-Spanner connection".to_string());
    };
    match config.suite {
        Suite::EndToEnd => {
            let rows = run_spanner(&config.run, database).await?;
            write_rows_new(&config.run.output, &rows)
        }
        Suite::Service => {
            let rows = run_spanner_service(&config.run, database).await?;
            write_service_rows_new(&config.run.output, &rows)
        }
    }
}
