use prolly_backend_comparison::{
    parse_binary_args, run_dynamodb, write_rows_new, Backend, ConnectionConfig,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("DynamoDB Local comparison failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = parse_binary_args(Backend::DynamoDbLocal, std::env::args().collect())?;
    let ConnectionConfig::DynamoDb(connection) = &config.connection else {
        return Err("DynamoDB binary received a non-DynamoDB connection".to_string());
    };
    let rows = run_dynamodb(&config.run, connection).await?;
    write_rows_new(&config.run.output, &rows)
}
