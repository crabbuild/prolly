use std::path::PathBuf;

use prolly_backend_comparison::summary::summarize_run;

fn main() {
    if let Err(error) = run() {
        eprintln!("backend comparison summary failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let values = std::env::args().collect::<Vec<_>>();
    let mut input = None;
    let mut manifest = None;
    let mut output = None;
    let mut index = 1;
    while index < values.len() {
        let flag = values[index].as_str();
        index += 1;
        let value = values
            .get(index)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag {
            "--input" => input = Some(PathBuf::from(value)),
            "--manifest" => manifest = Some(PathBuf::from(value)),
            "--output-dir" => output = Some(PathBuf::from(value)),
            _ => return Err(format!("unknown option: {flag}")),
        }
        index += 1;
    }
    summarize_run(
        &input.ok_or_else(|| "--input is required".to_string())?,
        &manifest.ok_or_else(|| "--manifest is required".to_string())?,
        &output.ok_or_else(|| "--output-dir is required".to_string())?,
    )
}
