use std::path::PathBuf;
use std::str::FromStr;

use crate::model::{Operation, Pattern};

pub const USAGE: &str = "usage: prolly-postgres-scale-bench [--profile smoke|full] [--url URL] [--output PATH] [--revision REV] [--dirty|--clean] [--sizes LIST] [--runs N] [--operations LIST] [--patterns LIST] [--changes N|auto] [--read-samples N] [--min-free-gb N]";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunConfig {
    pub url: String,
    pub output: PathBuf,
    pub revision: String,
    pub dirty: bool,
    pub sizes: Vec<usize>,
    pub runs: u32,
    pub operations: Vec<Operation>,
    pub patterns: Vec<Pattern>,
    pub changes: Option<usize>,
    pub read_samples: usize,
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
            runs: 1,
            operations: Operation::ALL.to_vec(),
            patterns: Pattern::ALL.to_vec(),
            changes: Some(100),
            read_samples: 100,
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
        if self.sizes.is_empty() || self.sizes.contains(&0) || self.runs == 0 {
            return Err("sizes and runs must be positive".to_string());
        }
        if self.operations.is_empty() || self.patterns.is_empty() {
            return Err("operation and pattern filters must be non-empty".to_string());
        }
        if self.changes == Some(0) || self.read_samples == 0 {
            return Err("changes and read samples must be positive".to_string());
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
        ])
        .unwrap();
        assert_eq!(config.sizes, vec![500, 1_000]);
        assert_eq!(config.runs, 2);
        assert_eq!(config.changes, Some(25));
        assert_eq!(config.read_samples, 10);
        assert_eq!(
            config.operations,
            vec![Operation::GetCold, Operation::Query]
        );
        assert_eq!(config.patterns, vec![Pattern::Random, Pattern::Clustered]);
    }
}
