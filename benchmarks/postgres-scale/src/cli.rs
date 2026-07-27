use std::path::PathBuf;
use std::str::FromStr;

use crate::config::{CommandConfig, SuiteSelection, WorkloadConfig};
use crate::model::{Operation, Pattern};

pub const USAGE: &str = "usage: prolly-postgres-scale-bench [--profile smoke|full] [--url URL] [--output PATH] [--revision REV] [--dirty|--clean] [--sizes LIST] [--runs N] [--operations LIST] [--patterns LIST] [--changes N|auto] [--read-samples N] [--concurrency N] [--min-free-gb N]";
pub const COMMAND_USAGE: &str = "usage: prolly-postgres-scale-bench [--config PATH] [--suite service|scale|both] [--url URL] [--output PATH] [--revision REV] [--dirty|--clean] [--baseline PATH] [--allow-environment-mismatch] [--clients LIST] [--pool-sizes LIST] [--warmup-ms N] [--duration-ms N] [--adapter-batch-items N] [--service-records N] [--service-value-bytes N] [--sizes LIST] [--runs N] [--operations LIST] [--patterns LIST] [--changes N|auto] [--read-samples N] [--concurrency N] [--scale-value-bytes N] [--min-free-gb N]";

#[derive(Clone, Debug)]
pub enum ParsedCommand {
    LegacyScale(RunConfig),
    Unified(Box<CommandConfig>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunConfig {
    pub url: String,
    pub output: PathBuf,
    pub revision: String,
    pub dirty: bool,
    pub sizes: Vec<usize>,
    pub value_bytes: usize,
    pub runs: u32,
    pub operations: Vec<Operation>,
    pub patterns: Vec<Pattern>,
    pub changes: Option<usize>,
    pub read_samples: usize,
    pub concurrency: usize,
    pub min_free_bytes: u64,
}

impl RunConfig {
    pub fn smoke() -> Self {
        Self {
            url: "postgres://prolly:prolly@127.0.0.1:55433/prolly".to_string(),
            output: PathBuf::from("performance-results/postgres-scale-smoke"),
            revision: "unknown".to_string(),
            dirty: true,
            sizes: vec![1_000],
            value_bytes: 27,
            runs: 1,
            operations: Operation::ALL.to_vec(),
            patterns: Pattern::ALL.to_vec(),
            changes: Some(100),
            read_samples: 100,
            concurrency: 32,
            min_free_bytes: 0,
        }
    }

    pub fn full() -> Self {
        Self {
            sizes: vec![1_000_000, 10_000_000],
            runs: 3,
            changes: None,
            read_samples: 10_000,
            min_free_bytes: 3 * 1024 * 1024 * 1024,
            output: PathBuf::from("performance-results/postgres-scale"),
            ..Self::smoke()
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.url.is_empty() || self.revision.is_empty() {
            return Err("URL and revision must be non-empty".to_string());
        }
        if self.sizes.is_empty()
            || self.sizes.contains(&0)
            || self.value_bytes == 0
            || self.runs == 0
        {
            return Err("sizes and runs must be positive".to_string());
        }
        if self.operations.is_empty() || self.patterns.is_empty() {
            return Err("operation and pattern filters must be non-empty".to_string());
        }
        if self.changes == Some(0) || self.read_samples == 0 || self.concurrency == 0 {
            return Err("changes, read samples, and concurrency must be positive".to_string());
        }
        if self.operations.contains(&Operation::Merge)
            && self.changes.is_some_and(|changes| changes % 2 != 0)
        {
            return Err("merge requires an even total change count".to_string());
        }
        Ok(())
    }
}

pub fn parse_args<I, S>(args: I) -> Result<RunConfig, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let values = args
        .into_iter()
        .map(|value| value.as_ref().to_string())
        .collect::<Vec<_>>();
    let mut profile = "full".to_string();
    let mut overrides = Vec::new();
    let mut index = 1;
    while index < values.len() {
        let flag = values[index].clone();
        if flag == "--profile" {
            index += 1;
            profile = values
                .get(index)
                .cloned()
                .ok_or_else(|| "--profile requires a value".to_string())?;
        } else {
            overrides.push(flag);
            if !matches!(values[index].as_str(), "--dirty" | "--clean") {
                index += 1;
                overrides.push(
                    values
                        .get(index)
                        .cloned()
                        .ok_or_else(|| "option requires a value".to_string())?,
                );
            }
        }
        index += 1;
    }
    let mut config = match profile.as_str() {
        "smoke" => RunConfig::smoke(),
        "full" => RunConfig::full(),
        _ => return Err(format!("unknown profile: {profile}\n{USAGE}")),
    };
    let mut index = 0;
    while index < overrides.len() {
        let flag = overrides[index].as_str();
        match flag {
            "--dirty" => config.dirty = true,
            "--clean" => config.dirty = false,
            "--url" => config.url = take(&overrides, &mut index, flag)?,
            "--output" => config.output = PathBuf::from(take(&overrides, &mut index, flag)?),
            "--revision" => config.revision = take(&overrides, &mut index, flag)?,
            "--sizes" => config.sizes = parse_list(&take(&overrides, &mut index, flag)?)?,
            "--value-bytes" => {
                config.value_bytes = parse_number(&take(&overrides, &mut index, flag)?, flag)?;
            }
            "--runs" => config.runs = parse_number(&take(&overrides, &mut index, flag)?, flag)?,
            "--operations" => config.operations = parse_list(&take(&overrides, &mut index, flag)?)?,
            "--patterns" => config.patterns = parse_list(&take(&overrides, &mut index, flag)?)?,
            "--changes" => {
                let value = take(&overrides, &mut index, flag)?;
                config.changes = if value == "auto" {
                    None
                } else {
                    Some(parse_number(&value, flag)?)
                };
            }
            "--read-samples" => {
                config.read_samples = parse_number(&take(&overrides, &mut index, flag)?, flag)?;
            }
            "--concurrency" => {
                config.concurrency = parse_number(&take(&overrides, &mut index, flag)?, flag)?;
            }
            "--min-free-gb" => {
                let gib: u64 = parse_number(&take(&overrides, &mut index, flag)?, flag)?;
                config.min_free_bytes = gib.saturating_mul(1024 * 1024 * 1024);
            }
            "--help" | "-h" => return Err(USAGE.to_string()),
            _ => return Err(format!("unknown option: {flag}\n{USAGE}")),
        }
        index += 1;
    }
    config.validate()?;
    Ok(config)
}

pub fn parse_command_args<I, S>(args: I) -> Result<ParsedCommand, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let values = args
        .into_iter()
        .map(|value| value.as_ref().to_string())
        .collect::<Vec<_>>();
    if values.iter().any(|value| value == "--profile") {
        return parse_args(values).map(ParsedCommand::LegacyScale);
    }

    let mut workload_path = WorkloadConfig::default_path();
    let mut index = 1;
    while index < values.len() {
        if values[index] == "--config" {
            workload_path = PathBuf::from(
                values
                    .get(index + 1)
                    .ok_or_else(|| "--config requires a value".to_string())?,
            );
            break;
        }
        index += 1;
    }
    let mut workload = WorkloadConfig::load(&workload_path)?;
    let mut suites = match (workload.service.enabled, workload.scale.enabled) {
        (true, true) => SuiteSelection::Both,
        (true, false) => SuiteSelection::Service,
        (false, true) => SuiteSelection::Scale,
        (false, false) => return Err("workload enables no suites".to_string()),
    };
    let mut url = "postgres://prolly:prolly@127.0.0.1:55433/prolly".to_string();
    let mut output = PathBuf::from("performance-results/postgres-service");
    let mut revision = "unknown".to_string();
    let mut dirty = true;
    let mut baseline = None;
    let mut allow_environment_mismatch = false;

    index = 1;
    while index < values.len() {
        let flag = values[index].as_str();
        match flag {
            "--config" => {
                index += 1;
            }
            "--suite" => {
                suites = SuiteSelection::parse(&command_value(&values, &mut index, flag)?)?;
            }
            "--url" => url = command_value(&values, &mut index, flag)?,
            "--output" => {
                output = PathBuf::from(command_value(&values, &mut index, flag)?);
            }
            "--revision" => revision = command_value(&values, &mut index, flag)?,
            "--dirty" => dirty = true,
            "--clean" => dirty = false,
            "--baseline" => {
                baseline = Some(PathBuf::from(command_value(&values, &mut index, flag)?));
            }
            "--allow-environment-mismatch" => allow_environment_mismatch = true,
            "--clients" => {
                workload.service.clients = parse_list(&command_value(&values, &mut index, flag)?)?;
            }
            "--pool-sizes" => {
                workload.service.pool_sizes =
                    parse_list(&command_value(&values, &mut index, flag)?)?;
            }
            "--warmup-ms" => {
                workload.service.warmup_ms =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--duration-ms" => {
                workload.service.duration_ms =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--adapter-batch-items" => {
                workload.service.adapter_batch_items =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--service-records" => {
                workload.service.records =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--service-value-bytes" => {
                workload.service.value_bytes =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--sizes" => {
                workload.scale.sizes = parse_list(&command_value(&values, &mut index, flag)?)?;
            }
            "--runs" => {
                workload.scale.runs =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--operations" => {
                workload.scale.operations = parse_list(&command_value(&values, &mut index, flag)?)?;
            }
            "--patterns" => {
                workload.scale.patterns = parse_list(&command_value(&values, &mut index, flag)?)?;
            }
            "--changes" => {
                let value = command_value(&values, &mut index, flag)?;
                workload.scale.changes = if value == "auto" {
                    None
                } else {
                    Some(parse_number(&value, flag)?)
                };
            }
            "--read-samples" => {
                workload.scale.read_samples =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--concurrency" => {
                workload.scale.concurrency =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--scale-value-bytes" => {
                workload.scale.value_bytes =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--min-free-gb" => {
                workload.scale.min_free_gb =
                    parse_number(&command_value(&values, &mut index, flag)?, flag)?;
            }
            "--help" | "-h" => return Err(COMMAND_USAGE.to_string()),
            _ => return Err(format!("unknown option: {flag}\n{COMMAND_USAGE}")),
        }
        index += 1;
    }
    workload.validate()?;
    Ok(ParsedCommand::Unified(Box::new(CommandConfig {
        workload,
        workload_path,
        suites,
        url,
        output,
        revision,
        dirty,
        baseline,
        allow_environment_mismatch,
    })))
}

fn command_value(values: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    values
        .get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn take(values: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    values
        .get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_number<T: FromStr>(value: &str, flag: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {flag} value {value}: {error}"))
}

fn parse_list<T: FromStr>(value: &str) -> Result<Vec<T>, String>
where
    T::Err: std::fmt::Display,
{
    value
        .split(',')
        .map(|item| item.parse::<T>().map_err(|error| error.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Operation, Pattern};

    #[test]
    fn full_profile_has_requested_scale_and_repetitions() {
        let config = parse_args(["bench", "--profile", "full"]).unwrap();
        assert_eq!(config.sizes, vec![1_000_000, 10_000_000]);
        assert_eq!(config.runs, 3);
        assert_eq!(config.changes, None);
        assert_eq!(config.read_samples, 10_000);
        assert!(config.operations.contains(&Operation::Merge));
        assert_eq!(config.patterns, Pattern::ALL);
    }

    #[test]
    fn smoke_profile_and_filters_are_explicit() {
        let config = parse_args([
            "bench",
            "--profile",
            "smoke",
            "--sizes",
            "500,1000",
            "--runs",
            "2",
            "--operations",
            "get_cold,query",
            "--patterns",
            "random,clustered",
            "--changes",
            "25",
            "--read-samples",
            "10",
            "--concurrency",
            "7",
        ])
        .unwrap();
        assert_eq!(config.sizes, vec![500, 1_000]);
        assert_eq!(config.runs, 2);
        assert_eq!(config.changes, Some(25));
        assert_eq!(config.read_samples, 10);
        assert_eq!(config.concurrency, 7);
        assert_eq!(
            config.operations,
            vec![Operation::GetCold, Operation::Query]
        );
        assert_eq!(config.patterns, vec![Pattern::Random, Pattern::Clustered]);
    }

    #[test]
    fn unified_command_loads_toml_and_applies_overrides() {
        let path = WorkloadConfig::default_path()
            .parent()
            .unwrap()
            .join("smoke.toml");
        let parsed = parse_command_args([
            "bench".to_string(),
            "--config".to_string(),
            path.display().to_string(),
            "--suite".to_string(),
            "both".to_string(),
            "--clients".to_string(),
            "2,4".to_string(),
            "--pool-sizes".to_string(),
            "2".to_string(),
            "--sizes".to_string(),
            "500,1000".to_string(),
            "--operations".to_string(),
            "batch,query".to_string(),
            "--patterns".to_string(),
            "random".to_string(),
            "--concurrency".to_string(),
            "6".to_string(),
        ])
        .unwrap();
        let ParsedCommand::Unified(command) = parsed else {
            panic!("expected unified command");
        };
        assert_eq!(command.workload.service.clients, vec![2, 4]);
        assert_eq!(command.workload.service.pool_sizes, vec![2]);
        assert_eq!(command.workload.scale.sizes, vec![500, 1_000]);
        assert_eq!(
            command.workload.scale.operations,
            vec![Operation::Batch, Operation::Query]
        );
        assert_eq!(command.workload.scale.patterns, vec![Pattern::Random]);
        assert_eq!(command.workload.scale.concurrency, 6);
        assert_eq!(command.suites, SuiteSelection::Both);
    }
}
