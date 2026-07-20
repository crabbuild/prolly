use std::path::PathBuf;
use std::str::FromStr;

use crate::model::RunConfig;

pub const USAGE: &str = "usage: prolly-redis-scale-bench [--profile smoke|full] [--redis-url URL] [--output PATH] [--sizes LIST] [--runs N] [--operations LIST] [--patterns LIST] [--changes N|auto] [--read-samples N] [--min-free-gb N] [--tokio-workers N] [--keep-fixtures] [--revision REV] [--dirty|--clean]";

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
            if !matches!(
                values[index].as_str(),
                "--dirty" | "--clean" | "--keep-fixtures"
            ) {
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
        "smoke" => RunConfig::smoke(PathBuf::from("performance-results/redis/baseline/smoke")),
        "full" => RunConfig::full(
            PathBuf::from("performance-results/redis/baseline"),
            "unknown".to_string(),
            true,
        ),
        _ => return Err(format!("unknown profile: {profile}\n{USAGE}")),
    };

    let mut index = 0;
    while index < overrides.len() {
        let flag = overrides[index].as_str();
        match flag {
            "--dirty" => config.dirty = true,
            "--clean" => config.dirty = false,
            "--keep-fixtures" => config.keep_fixtures = true,
            "--redis-url" => config.redis_url = take(&overrides, &mut index, flag)?,
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
                config.read_samples = parse_number(&take(&overrides, &mut index, flag)?, flag)?
            }
            "--min-free-gb" => {
                let gib: u64 = parse_number(&take(&overrides, &mut index, flag)?, flag)?;
                config.min_free_bytes = gib.saturating_mul(1024 * 1024 * 1024);
            }
            "--tokio-workers" => {
                config.tokio_workers = parse_number(&take(&overrides, &mut index, flag)?, flag)?
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
    if value.is_empty() {
        return Err("list must not be empty".to_string());
    }
    value
        .split(',')
        .map(|item| item.parse::<T>().map_err(|error| error.to_string()))
        .collect()
}
