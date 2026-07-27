use prolly_backend_comparison::{
    parse_binary_args, run_postgres, write_rows_new, Backend, ConnectionConfig,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("PostgreSQL comparison failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = parse_binary_args(Backend::Postgres, std::env::args().collect())?;
    let ConnectionConfig::Postgres { url } = &config.connection else {
        return Err("PostgreSQL binary received a non-PostgreSQL connection".to_string());
    };
    let rows = run_postgres(&config.run, url).await?;
    write_rows_new(&config.run.output, &rows)
}
