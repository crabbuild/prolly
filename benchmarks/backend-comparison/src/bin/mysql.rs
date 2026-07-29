use prolly_backend_comparison::{
    parse_binary_args, run_mysql, run_mysql_service, write_rows_new, write_service_rows_new,
    Backend, ConnectionConfig, Suite,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("MySQL comparison failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = parse_binary_args(Backend::MySql, std::env::args().collect())?;
    let ConnectionConfig::MySql { url } = &config.connection else {
        return Err("MySQL binary received a non-MySQL connection".to_string());
    };
    match config.suite {
        Suite::EndToEnd => {
            let rows = run_mysql(&config.run, url).await?;
            write_rows_new(&config.run.output, &rows)
        }
        Suite::Service => {
            let rows = run_mysql_service(&config.run, url).await?;
            write_service_rows_new(&config.run.output, &rows)
        }
    }
}
