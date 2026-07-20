//! Command-line parsing for reproducible benchmark profiles and filters.

use std::path::PathBuf;
use std::str::FromStr;

use crate::model::{Adapter, Api, Pattern, RunConfig};

pub const USAGE: &str = "usage: prolly-sqlite-turso-local-bench [--profile smoke|full] [--output PATH] [--revision REV] [--dirty|--clean] [--adapters LIST] [--sizes LIST] [--runs N] [--apis LIST] [--patterns LIST] [--changes N|auto] [--measurement-samples N] [--max-seconds N] [--min-free-gb N] [--keep-fixtures] [--tokio-workers N] [--build-batch-size N]";

pub fn parse_args<I, S>(args: I) -> Result<RunConfig, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let values: Vec<String> = args
        .into_iter()
        .map(|value| value.as_ref().to_string())
        .collect();
    let mut profile = "full".to_string();
    let mut output = PathBuf::from("performance-results/sqlite-turso-local");
    let mut revision = std::env::var("BENCH_REVISION").unwrap_or_else(|_| "unknown".to_string());
    let mut dirty = std::env::var("BENCH_DIRTY").map_or(true, |value| value != "false");
    let mut adapters = None;
    let mut sizes = None;
    let mut runs = None;
    let mut apis = None;
    let mut patterns = None;
    let mut changes = None;
    let mut max_seconds = None;
    let mut min_free_bytes = None;
    let mut keep_fixtures = false;
    let mut tokio_workers = None;
    let mut build_batch_size = None;
    let mut measurement_samples = None;

    let mut index = 1;
    while index < values.len() {
        let flag = values[index].as_str();
        match flag {
            "--profile" => profile = take_value(&values, &mut index, flag)?,
            "--output" => output = PathBuf::from(take_value(&values, &mut index, flag)?),
            "--revision" => revision = take_value(&values, &mut index, flag)?,
            "--dirty" => dirty = true,
            "--clean" => dirty = false,
            "--adapters" => {
                adapters = Some(parse_list::<Adapter>(&take_value(
                    &values, &mut index, flag,
                )?)?)
            }
            "--sizes" => {
                sizes = Some(parse_list::<usize>(&take_value(
                    &values, &mut index, flag,
                )?)?)
            }
            "--runs" => runs = Some(parse_number(&take_value(&values, &mut index, flag)?, flag)?),
            "--apis" => apis = Some(parse_list::<Api>(&take_value(&values, &mut index, flag)?)?),
            "--patterns" => {
                patterns = Some(parse_list::<Pattern>(&take_value(
                    &values, &mut index, flag,
                )?)?)
            }
            "--changes" => {
                let value = take_value(&values, &mut index, flag)?;
                changes = Some(if value == "auto" {
                    None
                } else {
                    Some(parse_number(&value, flag)?)
                });
            }
            "--measurement-samples" => {
                measurement_samples =
                    Some(parse_number(&take_value(&values, &mut index, flag)?, flag)?)
            }
            "--max-seconds" => {
                max_seconds = Some(parse_number(&take_value(&values, &mut index, flag)?, flag)?)
            }
            "--min-free-gb" => {
                let gib: u64 = parse_number(&take_value(&values, &mut index, flag)?, flag)?;
                min_free_bytes = Some(gib.saturating_mul(1024 * 1024 * 1024));
            }
            "--keep-fixtures" => keep_fixtures = true,
            "--tokio-workers" => {
                tokio_workers = Some(parse_number(&take_value(&values, &mut index, flag)?, flag)?)
            }
            "--build-batch-size" => {
                build_batch_size =
                    Some(parse_number(&take_value(&values, &mut index, flag)?, flag)?)
            }
            "--help" | "-h" => return Err(USAGE.to_string()),
            _ => return Err(format!("unknown option: {flag}\n{USAGE}")),
        }
        index += 1;
    }

    let mut config = match profile.as_str() {
        "smoke" => RunConfig::smoke(output),
        "full" => RunConfig::full(output, revision.clone(), dirty),
        _ => return Err(format!("unknown profile: {profile}\n{USAGE}")),
    };
    config.revision = revision;
    config.dirty = dirty;
    if let Some(value) = adapters {
        config.adapters = value;
    }
    if let Some(value) = sizes {
        config.sizes = value;
    }
    if let Some(value) = runs {
        config.runs = value;
    }
    if let Some(value) = apis {
        config.apis = value;
    }
    if let Some(value) = patterns {
        config.patterns = value;
    }
    if let Some(value) = changes {
        config.explicit_changes = value;
    }
    if let Some(value) = max_seconds {
        config.max_seconds = Some(value);
    }
    if let Some(value) = min_free_bytes {
        config.min_free_bytes = value;
    }
    config.keep_fixtures = keep_fixtures;
    if let Some(value) = tokio_workers {
        config.tokio_workers = value;
    }
    if let Some(value) = build_batch_size {
        config.build_batch_size = value;
    }
    if let Some(value) = measurement_samples {
        config.measurement_samples = value;
    }
    config.validate()?;
    Ok(config)
}

fn take_value(values: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    values
        .get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_number<T>(value: &str, flag: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid value for {flag}: {value}: {error}"))
}

fn parse_list<T>(value: &str) -> Result<Vec<T>, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    if value.is_empty() {
        return Err("list must not be empty".to_string());
    }
    value
        .split(',')
        .map(|item| {
            item.parse()
                .map_err(|error| format!("invalid list item {item}: {error}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Adapter, Api, Pattern};

    #[test]
    fn parses_smoke_profile_and_explicit_filters() {
        let config = parse_args([
            "bench",
            "--profile",
            "smoke",
            "--output",
            "results",
            "--revision",
            "abc123",
            "--clean",
            "--adapters",
            "turso-async",
            "--apis",
            "put,diff",
            "--patterns",
            "random",
            "--tokio-workers",
            "2",
            "--measurement-samples",
            "7",
        ])
        .unwrap();
        assert_eq!(config.revision, "abc123");
        assert!(!config.dirty);
        assert_eq!(config.adapters, vec![Adapter::TursoAsync]);
        assert_eq!(config.apis, vec![Api::Put, Api::Diff]);
        assert_eq!(config.patterns, vec![Pattern::Random]);
        assert_eq!(config.tokio_workers, 2);
        assert_eq!(config.measurement_samples, 7);
    }

    #[test]
    fn rejects_unknown_flags_and_profiles() {
        assert!(parse_args(["bench", "--profile", "tiny"]).is_err());
        assert!(parse_args(["bench", "--mystery"]).is_err());
    }
}
