use std::path::PathBuf;

use crate::model::RunConfig;

pub const USAGE: &str = "usage: prolly-sqlite-pattern-bench [--profile smoke|full] [--output PATH] [--sizes LIST] [--runs N] [--operations N|auto] [--keep-fixtures] [--revision REV] [--dirty|--clean]";

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
    let mut output = PathBuf::from("performance-results/sqlite-prolly-patterns");
    let mut revision = "unknown".to_string();
    let mut dirty = true;
    let mut sizes = None;
    let mut runs = None;
    let mut operations = None;
    let mut keep_fixtures = false;

    let mut index = 1;
    while index < values.len() {
        let flag = values[index].as_str();
        match flag {
            "--profile" => profile = take_value(&values, &mut index, flag)?,
            "--output" => output = PathBuf::from(take_value(&values, &mut index, flag)?),
            "--sizes" => {
                sizes = Some(
                    take_value(&values, &mut index, flag)?
                        .split(',')
                        .map(|part| parse_number(part, flag))
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            "--runs" => runs = Some(parse_number(&take_value(&values, &mut index, flag)?, flag)?),
            "--operations" => {
                let value = take_value(&values, &mut index, flag)?;
                operations = if value == "auto" {
                    Some(None)
                } else {
                    Some(Some(parse_number(&value, flag)?))
                };
            }
            "--keep-fixtures" => keep_fixtures = true,
            "--revision" => revision = take_value(&values, &mut index, flag)?,
            "--dirty" => dirty = true,
            "--clean" => dirty = false,
            "--help" | "-h" => return Err(USAGE.to_string()),
            _ => return Err(format!("unknown argument: {flag}\n{USAGE}")),
        }
        index += 1;
    }

    let mut config = match profile.as_str() {
        "smoke" => RunConfig::smoke(output),
        "full" => RunConfig::full(output, revision.clone(), dirty),
        _ => return Err(format!("unknown profile: {profile}")),
    };
    config.revision = revision;
    config.dirty = dirty;
    if let Some(value) = sizes {
        config.sizes = value;
    }
    if let Some(value) = runs {
        config.runs = value;
    }
    if let Some(value) = operations {
        config.explicit_operations = value;
    }
    config.keep_fixtures = keep_fixtures;
    config.validate()?;
    Ok(config)
}

fn take_value(values: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    values
        .get(*index)
        .cloned()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_number(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid value for {flag}: {error}"))
}
