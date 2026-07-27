use std::fmt;

use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ConfidenceInterval {
    pub low: f64,
    pub high: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Winner {
    Postgres,
    DynamoDbLocal,
    Inconclusive,
}

impl fmt::Display for Winner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Postgres => "PostgreSQL",
            Self::DynamoDbLocal => "DynamoDB Local",
            Self::Inconclusive => "inconclusive",
        })
    }
}

pub fn median(values: &[f64]) -> f64 {
    assert!(!values.is_empty(), "median requires at least one value");
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

pub fn median_absolute_deviation(values: &[f64]) -> f64 {
    let center = median(values);
    let deviations = values
        .iter()
        .map(|value| (value - center).abs())
        .collect::<Vec<_>>();
    median(&deviations)
}

pub fn coefficient_of_variation(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if mean == 0.0 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.sqrt() / mean.abs()
}

pub fn paired_bootstrap_ratio_ci(
    postgres: &[f64],
    dynamodb: &[f64],
    resamples: usize,
    seed: u64,
) -> Result<ConfidenceInterval, String> {
    if postgres.len() != dynamodb.len() || postgres.len() < 2 {
        return Err("paired bootstrap requires equal samples with at least two pairs".to_string());
    }
    if resamples < 100 {
        return Err("paired bootstrap requires at least 100 resamples".to_string());
    }
    if postgres
        .iter()
        .chain(dynamodb)
        .any(|value| !value.is_finite() || *value <= 0.0)
    {
        return Err("paired bootstrap samples must be finite and positive".to_string());
    }
    let mut state = seed;
    let mut ratios = Vec::with_capacity(resamples);
    let mut postgres_sample = Vec::with_capacity(postgres.len());
    let mut dynamodb_sample = Vec::with_capacity(dynamodb.len());
    for _ in 0..resamples {
        postgres_sample.clear();
        dynamodb_sample.clear();
        for _ in 0..postgres.len() {
            state = splitmix64(state);
            let index = (state as usize) % postgres.len();
            postgres_sample.push(postgres[index]);
            dynamodb_sample.push(dynamodb[index]);
        }
        ratios.push(median(&dynamodb_sample) / median(&postgres_sample));
    }
    ratios.sort_by(f64::total_cmp);
    Ok(ConfidenceInterval {
        low: percentile(&ratios, 0.025),
        high: percentile(&ratios, 0.975),
    })
}

pub fn classify_winner(ratio: f64, interval: ConfidenceInterval) -> Winner {
    if interval.low > 1.0 && ratio > 1.05 {
        Winner::Postgres
    } else if interval.high < 1.0 && ratio < 1.0 / 1.05 {
        Winner::DynamoDbLocal
    } else {
        Winner::Inconclusive
    }
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    let rank = (quantile * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptive_statistics_have_pinned_values() {
        let values = [10.0, 11.0, 12.0, 13.0, 54.0];
        assert_eq!(median(&values), 12.0);
        assert_eq!(median_absolute_deviation(&values), 1.0);
        assert!((coefficient_of_variation(&values) - 0.951_971_6).abs() < 1e-6);
    }

    #[test]
    fn winner_requires_confidence_and_five_percent_effect() {
        let postgres = [100.0, 101.0, 99.0, 102.0, 98.0, 100.0, 101.0];
        let dynamodb = [120.0, 121.0, 119.0, 122.0, 118.0, 120.0, 121.0];
        let interval = paired_bootstrap_ratio_ci(&postgres, &dynamodb, 10_000, 7).unwrap();
        assert!(interval.low > 1.0);
        assert_eq!(classify_winner(1.2, interval), Winner::Postgres);

        assert_eq!(
            classify_winner(
                1.04,
                ConfidenceInterval {
                    low: 1.01,
                    high: 1.07
                }
            ),
            Winner::Inconclusive
        );
    }
}
