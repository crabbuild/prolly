use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::RunConfig;
use crate::config::CommandConfig;
use crate::harness::{run_matrix, RunStats};
use crate::service_harness::run_service_suite;

pub async fn run_benchmark(command: CommandConfig) -> Result<RunStats, String> {
    std::fs::create_dir_all(&command.output)
        .map_err(|error| format!("failed to create {}: {error}", command.output.display()))?;
    let is_resume = has_existing_results(&command.output);
    validate_existing_manifest(&command)?;
    if !is_resume {
        let original = std::fs::read_to_string(&command.workload_path).map_err(|error| {
            format!(
                "failed to read workload {}: {error}",
                command.workload_path.display()
            )
        })?;
        std::fs::write(command.output.join("workload.toml"), original)
            .map_err(|error| format!("failed to write original workload: {error}"))?;
        std::fs::write(
            command.output.join("resolved-workload.toml"),
            command.workload.canonical_toml()?,
        )
        .map_err(|error| format!("failed to write resolved workload: {error}"))?;
        write_manifest(&command)?;
    }

    let mut total = RunStats::default();
    if command.suites.runs_service() {
        add_stats(&mut total, run_service_suite(&command).await?);
    }
    if command.suites.runs_scale() {
        let scale = &command.workload.scale;
        let config = RunConfig {
            url: command.url.clone(),
            output: command.output.clone(),
            revision: command.revision.clone(),
            dirty: command.dirty,
            sizes: scale.sizes.clone(),
            value_bytes: scale.value_bytes,
            runs: scale.runs,
            operations: scale.operations.clone(),
            patterns: scale.patterns.clone(),
            changes: scale.changes,
            read_samples: scale.read_samples,
            concurrency: scale.concurrency,
            min_free_bytes: scale.min_free_gb.saturating_mul(1024 * 1024 * 1024),
        };
        add_stats(&mut total, run_matrix(config).await?);
        let raw = command.output.join("raw-results.csv");
        if raw.exists() {
            std::fs::copy(&raw, command.output.join("scale-raw.csv"))
                .map_err(|error| format!("failed to copy scale raw results: {error}"))?;
        }
    }
    Ok(total)
}

fn validate_existing_manifest(command: &CommandConfig) -> Result<(), String> {
    let manifest_path = command.output.join("run-manifest.txt");
    let has_results = has_existing_results(&command.output);
    if !has_results {
        return Ok(());
    }
    if !manifest_path.exists() {
        return Err("existing results have no run manifest; refusing unsafe resume".to_string());
    }
    let contents = std::fs::read_to_string(&manifest_path)
        .map_err(|error| format!("failed to read {}: {error}", manifest_path.display()))?;
    let values = contents
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<std::collections::BTreeMap<_, _>>();
    let expected_hash = command.workload.configuration_hash()?;
    for (name, expected) in [
        ("schema", "postgres-service-scale-v1"),
        ("config_hash", expected_hash.as_str()),
        ("revision", command.revision.as_str()),
        ("dirty", if command.dirty { "true" } else { "false" }),
    ] {
        if values.get(name).copied() != Some(expected) {
            return Err(format!(
                "existing run manifest {name} differs; refusing to overwrite resumable results"
            ));
        }
    }
    Ok(())
}

fn has_existing_results(output: &std::path::Path) -> bool {
    ["service-raw.csv", "raw-results.csv", "scale-raw.csv"]
        .iter()
        .any(|name| output.join(name).exists())
}

fn add_stats(total: &mut RunStats, next: RunStats) {
    total.measured += next.measured;
    total.skipped += next.skipped;
    total.fixtures_built += next.fixtures_built;
}

fn write_manifest(command: &CommandConfig) -> Result<(), String> {
    let mut manifest = String::new();
    writeln!(&mut manifest, "schema=postgres-service-scale-v1").unwrap();
    writeln!(
        &mut manifest,
        "started_unix_ms={}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    )
    .unwrap();
    writeln!(
        &mut manifest,
        "config_hash={}",
        command.workload.configuration_hash()?
    )
    .unwrap();
    writeln!(&mut manifest, "revision={}", command.revision).unwrap();
    writeln!(&mut manifest, "dirty={}", command.dirty).unwrap();
    writeln!(&mut manifest, "seed={}", command.workload.seed).unwrap();
    writeln!(&mut manifest, "suites={:?}", command.suites).unwrap();
    writeln!(
        &mut manifest,
        "sizes={}",
        command
            .workload
            .scale
            .sizes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
    .unwrap();
    writeln!(&mut manifest, "runs={}", command.workload.scale.runs).unwrap();
    writeln!(
        &mut manifest,
        "operations={}",
        command
            .workload
            .scale
            .operations
            .iter()
            .map(|operation| operation.as_str())
            .collect::<Vec<_>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        &mut manifest,
        "patterns={}",
        command
            .workload
            .scale
            .patterns
            .iter()
            .map(|pattern| pattern.as_str())
            .collect::<Vec<_>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        &mut manifest,
        "changes={}",
        command
            .workload
            .scale
            .changes
            .map(|changes| changes.to_string())
            .unwrap_or_else(|| "auto".to_string())
    )
    .unwrap();
    writeln!(
        &mut manifest,
        "read_samples={}",
        command.workload.scale.read_samples
    )
    .unwrap();
    writeln!(&mut manifest, "merge_changes_semantics=total_split_evenly").unwrap();
    writeln!(
        &mut manifest,
        "baseline={}",
        command
            .baseline
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default()
    )
    .unwrap();
    writeln!(
        &mut manifest,
        "allow_environment_mismatch={}",
        command.allow_environment_mismatch
    )
    .unwrap();
    std::fs::write(command.output.join("run-manifest.txt"), manifest)
        .map_err(|error| format!("failed to write run manifest: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SuiteSelection, WorkloadConfig};

    #[test]
    fn mismatched_resume_manifest_is_not_overwritten() {
        let temp = tempfile::tempdir().unwrap();
        let workload_path = WorkloadConfig::default_path()
            .parent()
            .unwrap()
            .join("smoke.toml");
        let command = CommandConfig {
            workload: WorkloadConfig::load(&workload_path).unwrap(),
            workload_path,
            suites: SuiteSelection::Service,
            url: "postgres://unused".to_string(),
            output: temp.path().to_path_buf(),
            revision: "new".to_string(),
            dirty: true,
            baseline: None,
            allow_environment_mismatch: false,
        };
        let original =
            "schema=postgres-service-scale-v1\nconfig_hash=old\nrevision=old\ndirty=false\n";
        std::fs::write(temp.path().join("run-manifest.txt"), original).unwrap();
        std::fs::write(temp.path().join("service-raw.csv"), "results").unwrap();

        assert!(validate_existing_manifest(&command).is_err());
        assert_eq!(
            std::fs::read_to_string(temp.path().join("run-manifest.txt")).unwrap(),
            original
        );
    }
}
