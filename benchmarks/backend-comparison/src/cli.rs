use std::path::PathBuf;

use prolly_backend_workload_contract::WorkloadSpec;

use crate::Backend;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DynamoDbConnection {
    pub endpoint: String,
    pub table: String,
    pub read_parallelism: usize,
    pub batch_get_parallelism: usize,
    pub batch_write_parallelism: usize,
    pub scan_parallelism: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionConfig {
    Postgres { url: String },
    MySql { url: String },
    DynamoDb(DynamoDbConnection),
    Spanner { database: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryConfig {
    pub run: RunConfig,
    pub connection: ConnectionConfig,
    pub suite: Suite,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Suite {
    #[default]
    EndToEnd,
    Service,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunConfig {
    pub backend: Backend,
    pub output: PathBuf,
    pub run_id: String,
    pub repetition: u32,
    pub revision: String,
    pub tree_hash: String,
    pub binary_sha256: String,
    pub pool_size: u32,
    pub adapter_batch_items: usize,
    pub workload: WorkloadSpec,
}

impl RunConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.run_id.is_empty() {
            return Err("run ID cannot be empty".to_string());
        }
        if self.repetition == 0 {
            return Err("repetition must be positive".to_string());
        }
        if !is_hex(&self.revision, 40) {
            return Err("revision must be a 40-character hexadecimal commit".to_string());
        }
        if !is_hex(&self.tree_hash, 40) {
            return Err("tree hash must be a 40-character hexadecimal value".to_string());
        }
        if !is_hex(&self.binary_sha256, 64) {
            return Err("binary hash must be a 64-character SHA-256 value".to_string());
        }
        if self.output.as_os_str().is_empty() {
            return Err("output path cannot be empty".to_string());
        }
        if self.pool_size == 0 || self.adapter_batch_items == 0 {
            return Err("pool size and adapter batch items must be positive".to_string());
        }
        Ok(())
    }
}

pub fn parse_binary_args(backend: Backend, values: Vec<String>) -> Result<BinaryConfig, String> {
    let mut output = None;
    let mut run_id = None;
    let mut repetition = None;
    let mut revision = None;
    let mut tree_hash = None;
    let mut binary_sha256 = None;
    let mut records = None;
    let mut value_bytes = None;
    let mut changes = None;
    let mut samples = None;
    let mut concurrency = None;
    let mut seed = None;
    let mut pool_size = 10;
    let mut adapter_batch_items = 1_000;
    let mut suite = Suite::EndToEnd;
    let mut url = match backend {
        Backend::Postgres => "postgres://prolly:prolly@127.0.0.1:55433/prolly",
        Backend::MySql => "mysql://prolly:prolly@127.0.0.1:53307/prolly",
        Backend::DynamoDbLocal => "",
        Backend::Spanner => "",
    }
    .to_string();
    let mut dynamodb = DynamoDbConnection {
        endpoint: "http://127.0.0.1:58000".to_string(),
        table: "prolly_backend_comparison".to_string(),
        read_parallelism: 16,
        batch_get_parallelism: 16,
        batch_write_parallelism: 16,
        scan_parallelism: 8,
    };
    let mut spanner_database = String::new();

    let mut index = 1;
    while index < values.len() {
        let flag = values[index].as_str();
        let value = |index: &mut usize| take(&values, index, flag);
        match flag {
            "--output" => output = Some(PathBuf::from(value(&mut index)?)),
            "--run-id" => run_id = Some(value(&mut index)?),
            "--repetition" => repetition = Some(number(&value(&mut index)?, flag)?),
            "--revision" => revision = Some(value(&mut index)?),
            "--tree-hash" => tree_hash = Some(value(&mut index)?),
            "--binary-sha256" => binary_sha256 = Some(value(&mut index)?),
            "--records" => records = Some(number(&value(&mut index)?, flag)?),
            "--value-bytes" => value_bytes = Some(number(&value(&mut index)?, flag)?),
            "--changes" => changes = Some(number(&value(&mut index)?, flag)?),
            "--samples" => samples = Some(number(&value(&mut index)?, flag)?),
            "--concurrency" => concurrency = Some(number(&value(&mut index)?, flag)?),
            "--seed" => seed = Some(parse_seed(&value(&mut index)?)?),
            "--pool-size" => pool_size = number(&value(&mut index)?, flag)?,
            "--adapter-batch-items" => adapter_batch_items = number(&value(&mut index)?, flag)?,
            "--suite" => {
                suite = match value(&mut index)?.as_str() {
                    "end-to-end" => Suite::EndToEnd,
                    "service" => Suite::Service,
                    value => return Err(format!("invalid --suite: {value}")),
                }
            }
            "--url" => url = value(&mut index)?,
            "--endpoint" => dynamodb.endpoint = value(&mut index)?,
            "--table" => dynamodb.table = value(&mut index)?,
            "--read-parallelism" => dynamodb.read_parallelism = number(&value(&mut index)?, flag)?,
            "--batch-get-parallelism" => {
                dynamodb.batch_get_parallelism = number(&value(&mut index)?, flag)?
            }
            "--batch-write-parallelism" => {
                dynamodb.batch_write_parallelism = number(&value(&mut index)?, flag)?
            }
            "--scan-parallelism" => dynamodb.scan_parallelism = number(&value(&mut index)?, flag)?,
            "--database" => spanner_database = value(&mut index)?,
            "--help" | "-h" => return Err(usage(backend).to_string()),
            _ => return Err(format!("unknown option for {backend}: {flag}")),
        }
        index += 1;
    }
    let run = RunConfig {
        backend,
        output: required(output, "--output")?,
        run_id: required(run_id, "--run-id")?,
        repetition: required(repetition, "--repetition")?,
        revision: required(revision, "--revision")?,
        tree_hash: required(tree_hash, "--tree-hash")?,
        binary_sha256: required(binary_sha256, "--binary-sha256")?,
        pool_size,
        adapter_batch_items,
        workload: WorkloadSpec {
            records: required(records, "--records")?,
            value_bytes: required(value_bytes, "--value-bytes")?,
            changes: required(changes, "--changes")?,
            samples: required(samples, "--samples")?,
            concurrency: required(concurrency, "--concurrency")?,
            seed: required(seed, "--seed")?,
        },
    };
    run.validate()?;
    let connection = match backend {
        Backend::Postgres => ConnectionConfig::Postgres { url },
        Backend::MySql => ConnectionConfig::MySql { url },
        Backend::DynamoDbLocal => {
            if dynamodb.endpoint.is_empty()
                || dynamodb.table.is_empty()
                || dynamodb.read_parallelism == 0
                || dynamodb.batch_get_parallelism == 0
                || dynamodb.batch_write_parallelism == 0
                || dynamodb.scan_parallelism == 0
            {
                return Err("DynamoDB Local connection values must be positive".to_string());
            }
            ConnectionConfig::DynamoDb(dynamodb)
        }
        Backend::Spanner => {
            if spanner_database.is_empty() {
                return Err("Spanner database resource name must be set".to_string());
            }
            ConnectionConfig::Spanner {
                database: spanner_database,
            }
        }
    };
    if backend == Backend::DynamoDbLocal && suite == Suite::Service {
        return Err("the service suite supports MySQL, PostgreSQL, and Spanner".to_string());
    }
    Ok(BinaryConfig {
        run,
        connection,
        suite,
    })
}

pub(crate) fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn take(values: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    values
        .get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn number<T: std::str::FromStr>(value: &str, flag: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {flag}: {error}"))
}

fn parse_seed(value: &str) -> Result<u64, String> {
    value
        .strip_prefix("0x")
        .map(|hex| u64::from_str_radix(hex, 16))
        .unwrap_or_else(|| value.parse())
        .map_err(|error| format!("invalid --seed: {error}"))
}

fn required<T>(value: Option<T>, flag: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("{flag} is required"))
}

fn usage(backend: Backend) -> &'static str {
    match backend {
        Backend::Postgres => {
            "usage: prolly-backend-postgres --output PATH --run-id ID --repetition N --revision SHA --tree-hash SHA --binary-sha256 SHA --records N --value-bytes N --changes N --samples N --concurrency N --seed N [--pool-size N] [--adapter-batch-items N] [--url URL]"
        }
        Backend::MySql => {
            "usage: prolly-backend-mysql --output PATH --run-id ID --repetition N --revision SHA --tree-hash SHA --binary-sha256 SHA --records N --value-bytes N --changes N --samples N --concurrency N --seed N [--pool-size N] [--adapter-batch-items N] [--url URL]"
        }
        Backend::DynamoDbLocal => {
            "usage: prolly-backend-dynamodb --output PATH --run-id ID --repetition N --revision SHA --tree-hash SHA --binary-sha256 SHA --records N --value-bytes N --changes N --samples N --concurrency N --seed N [--endpoint URL] [--table NAME] [--read-parallelism N] [--batch-get-parallelism N] [--batch-write-parallelism N] [--scan-parallelism N]"
        }
        Backend::Spanner => {
            "usage: prolly-backend-spanner --output PATH --run-id ID --repetition N --revision SHA --tree-hash SHA --binary-sha256 SHA --records N --value-bytes N --changes N --samples N --concurrency N --seed N --database RESOURCE [--adapter-batch-items N]"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prolly_backend_workload_contract::DEFAULT_SEED;

    #[test]
    fn run_config_rejects_unattributable_provenance() {
        let mut config = RunConfig {
            backend: Backend::Postgres,
            output: PathBuf::from("row.csv"),
            run_id: "run-1".to_string(),
            repetition: 1,
            revision: "a".repeat(40),
            tree_hash: "b".repeat(40),
            binary_sha256: "c".repeat(64),
            pool_size: 10,
            adapter_batch_items: 1_000,
            workload: WorkloadSpec {
                records: 100,
                value_bytes: 27,
                changes: 10,
                samples: 10,
                concurrency: 4,
                seed: DEFAULT_SEED,
            },
        };
        config.validate().unwrap();
        config.binary_sha256 = "unknown".to_string();
        assert!(config.validate().unwrap_err().contains("binary hash"));
    }

    #[test]
    fn binary_arguments_share_workload_and_provenance_parsing() {
        let args = vec![
            "runner".to_string(),
            "--output".to_string(),
            "row.csv".to_string(),
            "--run-id".to_string(),
            "run-1".to_string(),
            "--repetition".to_string(),
            "3".to_string(),
            "--revision".to_string(),
            "a".repeat(40),
            "--tree-hash".to_string(),
            "b".repeat(40),
            "--binary-sha256".to_string(),
            "c".repeat(64),
            "--records".to_string(),
            "100".to_string(),
            "--value-bytes".to_string(),
            "27".to_string(),
            "--changes".to_string(),
            "10".to_string(),
            "--samples".to_string(),
            "8".to_string(),
            "--concurrency".to_string(),
            "4".to_string(),
            "--seed".to_string(),
            "0x6a09e667f3bcc909".to_string(),
            "--url".to_string(),
            "postgres://local".to_string(),
        ];

        let parsed = parse_binary_args(Backend::Postgres, args).unwrap();
        assert_eq!(parsed.run.repetition, 3);
        assert_eq!(parsed.run.workload.records, 100);
        assert_eq!(parsed.run.pool_size, 10);
        assert_eq!(
            parsed.connection,
            ConnectionConfig::Postgres {
                url: "postgres://local".to_string()
            }
        );
    }

    #[test]
    fn spanner_arguments_require_and_preserve_database_resource() {
        let mut args = vec![
            "runner".to_string(),
            "--output".to_string(),
            "row.csv".to_string(),
            "--run-id".to_string(),
            "run-1".to_string(),
            "--repetition".to_string(),
            "3".to_string(),
            "--revision".to_string(),
            "a".repeat(40),
            "--tree-hash".to_string(),
            "b".repeat(40),
            "--binary-sha256".to_string(),
            "c".repeat(64),
            "--records".to_string(),
            "100".to_string(),
            "--value-bytes".to_string(),
            "27".to_string(),
            "--changes".to_string(),
            "10".to_string(),
            "--samples".to_string(),
            "8".to_string(),
            "--concurrency".to_string(),
            "4".to_string(),
            "--seed".to_string(),
            "0x6a09e667f3bcc909".to_string(),
        ];
        assert!(parse_binary_args(Backend::Spanner, args.clone())
            .unwrap_err()
            .contains("database resource"));

        args.extend([
            "--database".to_string(),
            "projects/p/instances/i/databases/d".to_string(),
            "--suite".to_string(),
            "service".to_string(),
        ]);
        let parsed = parse_binary_args(Backend::Spanner, args).unwrap();
        assert_eq!(
            parsed.connection,
            ConnectionConfig::Spanner {
                database: "projects/p/instances/i/databases/d".to_string()
            }
        );
        assert_eq!(parsed.suite, Suite::Service);
    }
}
