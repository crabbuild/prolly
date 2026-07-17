use super::scalar::score;
use crate::prolly::proximity::{DistanceMetric, QueryKernel};

const PRODUCT_SLOTS: usize = 64;

#[cfg(test)]
thread_local! {
    static QUERY_KERNEL_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(crate) fn query_score(
    kernel: QueryKernel,
    metric: DistanceMetric,
    left: &[f32],
    right: &[f32],
) -> f64 {
    #[cfg(test)]
    QUERY_KERNEL_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));

    match kernel {
        QueryKernel::ScalarDeterministic => score(metric, left, right),
        QueryKernel::SimdDeterministic | QueryKernel::AutoDeterministic => {
            simd_score(metric, left, right).unwrap_or_else(|| score(metric, left, right))
        }
    }
}

fn simd_score(metric: DistanceMetric, left: &[f32], right: &[f32]) -> Option<f64> {
    debug_assert_eq!(left.len(), right.len());
    let mut products = [0.0f64; PRODUCT_SLOTS];
    let mut reduced = 0.0;
    for (left, right) in left.chunks(PRODUCT_SLOTS).zip(right.chunks(PRODUCT_SLOTS)) {
        let output = &mut products[..left.len()];
        if !fill_products(metric, left, right, output) {
            return None;
        }
        for &product in output.iter() {
            reduced += product;
        }
    }
    let result = match metric {
        DistanceMetric::L2Squared => reduced,
        DistanceMetric::Cosine => 1.0 - reduced.clamp(-1.0, 1.0),
        DistanceMetric::InnerProduct => -reduced,
    };
    Some(if result == 0.0 { 0.0 } else { result })
}

#[cfg(test)]
pub(crate) fn reset_query_kernel_calls() {
    QUERY_KERNEL_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn query_kernel_calls() -> usize {
    QUERY_KERNEL_CALLS.with(std::cell::Cell::get)
}

#[cfg_attr(
    not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")),
    allow(unused_variables)
)]
fn fill_products(metric: DistanceMetric, left: &[f32], right: &[f32], output: &mut [f64]) -> bool {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if std::arch::is_x86_feature_detected!("sse2") {
        // SAFETY: runtime feature detection guards the target-feature function;
        // pointers are bounded by the shared slice length.
        unsafe { fill_x86_sse2(metric, left, right, output) };
        return true;
    }
    #[cfg(target_arch = "aarch64")]
    {
        // AArch64 guarantees Advanced SIMD. The function uses unaligned two-lane
        // loads only inside the validated slice bounds.
        unsafe { fill_aarch64_neon(metric, left, right, output) };
        return true;
    }
    #[allow(unreachable_code)]
    false
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse2")]
unsafe fn fill_x86_sse2(metric: DistanceMetric, left: &[f32], right: &[f32], output: &mut [f64]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let mut index = 0usize;
    while index + 4 <= left.len() {
        let a = _mm_loadu_ps(left.as_ptr().add(index));
        let b = _mm_loadu_ps(right.as_ptr().add(index));
        let a_low = _mm_cvtps_pd(a);
        let b_low = _mm_cvtps_pd(b);
        let a_high = _mm_cvtps_pd(_mm_movehl_ps(a, a));
        let b_high = _mm_cvtps_pd(_mm_movehl_ps(b, b));
        let low = if metric == DistanceMetric::L2Squared {
            let delta = _mm_sub_pd(a_low, b_low);
            _mm_mul_pd(delta, delta)
        } else {
            _mm_mul_pd(a_low, b_low)
        };
        let high = if metric == DistanceMetric::L2Squared {
            let delta = _mm_sub_pd(a_high, b_high);
            _mm_mul_pd(delta, delta)
        } else {
            _mm_mul_pd(a_high, b_high)
        };
        _mm_storeu_pd(output.as_mut_ptr().add(index), low);
        _mm_storeu_pd(output.as_mut_ptr().add(index + 2), high);
        index += 4;
    }
    fill_tail(metric, left, right, output, index);
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn fill_aarch64_neon(
    metric: DistanceMetric,
    left: &[f32],
    right: &[f32],
    output: &mut [f64],
) {
    use std::arch::aarch64::*;

    let mut index = 0usize;
    while index + 2 <= left.len() {
        let a = vcvt_f64_f32(vld1_f32(left.as_ptr().add(index)));
        let b = vcvt_f64_f32(vld1_f32(right.as_ptr().add(index)));
        let product = if metric == DistanceMetric::L2Squared {
            let delta = vsubq_f64(a, b);
            vmulq_f64(delta, delta)
        } else {
            vmulq_f64(a, b)
        };
        vst1q_f64(output.as_mut_ptr().add(index), product);
        index += 2;
    }
    fill_tail(metric, left, right, output, index);
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))]
fn fill_tail(
    metric: DistanceMetric,
    left: &[f32],
    right: &[f32],
    output: &mut [f64],
    start: usize,
) {
    for index in start..left.len() {
        let a = f64::from(left[index]);
        let b = f64::from(right[index]);
        output[index] = if metric == DistanceMetric::L2Squared {
            let delta = a - b;
            delta * delta
        } else {
            a * b
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simd_and_scalar_scores_are_bit_identical_for_adversarial_lengths() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for dimensions in 1..=129 {
            let mut left = Vec::with_capacity(dimensions);
            let mut right = Vec::with_capacity(dimensions);
            for _ in 0..dimensions {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                left.push((state as i32) as f32 / 65_536.0);
                state = state.rotate_left(31).wrapping_mul(0xd6e8_feb8_6659_fd93);
                right.push((state as i32) as f32 / 65_536.0);
            }
            for metric in [
                DistanceMetric::L2Squared,
                DistanceMetric::Cosine,
                DistanceMetric::InnerProduct,
            ] {
                assert_eq!(
                    query_score(QueryKernel::SimdDeterministic, metric, &left, &right).to_bits(),
                    score(metric, &left, &right).to_bits(),
                    "metric={metric:?} dimensions={dimensions}"
                );
            }
        }
    }

    #[test]
    fn simd_and_scalar_scores_are_bit_identical_for_extreme_finite_values() {
        let smallest = f32::from_bits(1);
        let vectors = [
            (
                vec![smallest, -smallest, 0.0, -0.0, f32::MIN_POSITIVE],
                vec![-smallest, smallest, -0.0, 0.0, -f32::MIN_POSITIVE],
            ),
            (
                vec![f32::MAX, -f32::MAX, 1.0, -1.0, 0.5, -0.5, 3.0],
                vec![f32::MAX, f32::MAX, 1.0 + f32::EPSILON, -1.0, -0.5, 0.5, 3.0],
            ),
            (
                vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                vec![1.0, 1.0, 1.0, 1.0 + f32::EPSILON, 1.0, 1.0, 1.0, 1.0, 1.0],
            ),
        ];
        for (left, right) in vectors {
            for metric in [
                DistanceMetric::L2Squared,
                DistanceMetric::Cosine,
                DistanceMetric::InnerProduct,
            ] {
                assert_eq!(
                    query_score(QueryKernel::SimdDeterministic, metric, &left, &right).to_bits(),
                    score(metric, &left, &right).to_bits(),
                    "metric={metric:?} dimensions={}",
                    left.len()
                );
            }
        }
    }
}
