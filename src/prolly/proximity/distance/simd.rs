use super::scalar::{score, score_encoded as score_encoded_scalar};
use crate::prolly::proximity::{DistanceMetric, QueryKernel};
use std::sync::OnceLock;

const PRODUCT_SLOTS: usize = 64;

type FillProducts = unsafe fn(&[f32], &[f32], &mut [f64]);

static FILL_L2: OnceLock<Option<FillProducts>> = OnceLock::new();
static FILL_DOT: OnceLock<Option<FillProducts>> = OnceLock::new();

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

pub(crate) fn query_score_encoded(
    kernel: QueryKernel,
    metric: DistanceMetric,
    left: &[f32],
    right: &[u8],
) -> f64 {
    match kernel {
        QueryKernel::ScalarDeterministic => score_encoded_scalar(metric, left, right),
        QueryKernel::SimdDeterministic | QueryKernel::AutoDeterministic => {
            simd_score_encoded(metric, left, right)
                .unwrap_or_else(|| score_encoded_scalar(metric, left, right))
        }
    }
}

fn simd_score(metric: DistanceMetric, left: &[f32], right: &[f32]) -> Option<f64> {
    debug_assert_eq!(left.len(), right.len());
    let fill = match metric {
        DistanceMetric::L2Squared => *FILL_L2.get_or_init(detect_fill::<true>),
        DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
            *FILL_DOT.get_or_init(detect_fill::<false>)
        }
    }?;
    let mut products = [0.0f64; PRODUCT_SLOTS];
    let mut reduced = 0.0f64;
    for (left, right) in left.chunks(PRODUCT_SLOTS).zip(right.chunks(PRODUCT_SLOTS)) {
        let output = &mut products[..left.len()];
        // SAFETY: `detect_fill` only returns a target-feature function after
        // checking the corresponding runtime CPU feature. Each implementation
        // writes exactly `left.len()` products into the bounded output slice.
        unsafe { fill(left, right, output) };
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

type FillEncoded = unsafe fn(&[f32], &[u8], &mut [f64]);

static FILL_ENCODED_L2: OnceLock<Option<FillEncoded>> = OnceLock::new();
static FILL_ENCODED_DOT: OnceLock<Option<FillEncoded>> = OnceLock::new();

fn simd_score_encoded(metric: DistanceMetric, left: &[f32], right: &[u8]) -> Option<f64> {
    let expected_bytes = left.len().checked_mul(4)?;
    if expected_bytes != right.len() {
        return None;
    }
    let fill = match metric {
        DistanceMetric::L2Squared => *FILL_ENCODED_L2.get_or_init(detect_encoded_fill::<true>),
        DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
            *FILL_ENCODED_DOT.get_or_init(detect_encoded_fill::<false>)
        }
    }?;
    let mut products = [0.0f64; PRODUCT_SLOTS];
    let mut reduced = 0.0f64;
    let mut index = 0usize;
    while index < left.len() {
        let end = index.saturating_add(PRODUCT_SLOTS).min(left.len());
        let output = &mut products[..end - index];
        // SAFETY: `StoredRecordRef::decode` validates every encoded component
        // before this query-only scorer is called. The target-specific loader
        // reads exactly the corresponding four-byte f32 components.
        unsafe { fill(&left[index..end], &right[index * 4..end * 4], output) };
        for &product in output.iter() {
            reduced += product;
        }
        index = end;
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

fn detect_fill<const L2: bool>() -> Option<FillProducts> {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            return Some(fill_x86_avx512::<L2>);
        }
        // The wider floating-point kernel only needs AVX. AVX2 CPUs also take
        // this path; AVX2-specific integer instructions are reserved for the
        // quantized kernels.
        if std::arch::is_x86_feature_detected!("avx2") || std::arch::is_x86_feature_detected!("avx")
        {
            return Some(fill_x86_avx::<L2>);
        }
        if std::arch::is_x86_feature_detected!("sse2") {
            return Some(fill_x86_sse2::<L2>);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        return Some(fill_aarch64_neon::<L2>);
    }
    #[allow(unreachable_code)]
    None
}

fn detect_encoded_fill<const L2: bool>() -> Option<FillEncoded> {
    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_endian = "little"
    ))]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            return Some(fill_encoded_x86_avx512::<L2>);
        }
        if std::arch::is_x86_feature_detected!("avx2") || std::arch::is_x86_feature_detected!("avx")
        {
            return Some(fill_encoded_x86_avx::<L2>);
        }
        if std::arch::is_x86_feature_detected!("sse2") {
            return Some(fill_encoded_x86_sse2::<L2>);
        }
    }
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        return Some(fill_encoded_aarch64_neon::<L2>);
    }
    #[allow(unreachable_code)]
    None
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse2")]
unsafe fn fill_x86_sse2<const L2: bool>(left: &[f32], right: &[f32], output: &mut [f64]) {
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
        let low = if L2 {
            let delta = _mm_sub_pd(a_low, b_low);
            _mm_mul_pd(delta, delta)
        } else {
            _mm_mul_pd(a_low, b_low)
        };
        let high = if L2 {
            let delta = _mm_sub_pd(a_high, b_high);
            _mm_mul_pd(delta, delta)
        } else {
            _mm_mul_pd(a_high, b_high)
        };
        _mm_storeu_pd(output.as_mut_ptr().add(index), low);
        _mm_storeu_pd(output.as_mut_ptr().add(index + 2), high);
        index += 4;
    }
    fill_tail::<L2>(left, right, output, index);
}

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_endian = "little"
))]
#[target_feature(enable = "sse2")]
unsafe fn fill_encoded_x86_sse2<const L2: bool>(left: &[f32], right: &[u8], output: &mut [f64]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let mut index = 0usize;
    while index + 4 <= left.len() {
        let a = _mm_loadu_ps(left.as_ptr().add(index));
        let b = _mm_loadu_ps(right.as_ptr().add(index * 4).cast::<f32>());
        let a_low = _mm_cvtps_pd(a);
        let b_low = _mm_cvtps_pd(b);
        let a_high = _mm_cvtps_pd(_mm_movehl_ps(a, a));
        let b_high = _mm_cvtps_pd(_mm_movehl_ps(b, b));
        let low = if L2 {
            let delta = _mm_sub_pd(a_low, b_low);
            _mm_mul_pd(delta, delta)
        } else {
            _mm_mul_pd(a_low, b_low)
        };
        let high = if L2 {
            let delta = _mm_sub_pd(a_high, b_high);
            _mm_mul_pd(delta, delta)
        } else {
            _mm_mul_pd(a_high, b_high)
        };
        _mm_storeu_pd(output.as_mut_ptr().add(index), low);
        _mm_storeu_pd(output.as_mut_ptr().add(index + 2), high);
        index += 4;
    }
    fill_encoded_tail::<L2>(&left[index..], &right[index * 4..], &mut output[index..]);
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx")]
unsafe fn fill_x86_avx<const L2: bool>(left: &[f32], right: &[f32], output: &mut [f64]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let mut index = 0usize;
    while index + 8 <= left.len() {
        let a_low = _mm256_cvtps_pd(_mm_loadu_ps(left.as_ptr().add(index)));
        let b_low = _mm256_cvtps_pd(_mm_loadu_ps(right.as_ptr().add(index)));
        let a_high = _mm256_cvtps_pd(_mm_loadu_ps(left.as_ptr().add(index + 4)));
        let b_high = _mm256_cvtps_pd(_mm_loadu_ps(right.as_ptr().add(index + 4)));
        let low = if L2 {
            let delta = _mm256_sub_pd(a_low, b_low);
            _mm256_mul_pd(delta, delta)
        } else {
            _mm256_mul_pd(a_low, b_low)
        };
        let high = if L2 {
            let delta = _mm256_sub_pd(a_high, b_high);
            _mm256_mul_pd(delta, delta)
        } else {
            _mm256_mul_pd(a_high, b_high)
        };
        _mm256_storeu_pd(output.as_mut_ptr().add(index), low);
        _mm256_storeu_pd(output.as_mut_ptr().add(index + 4), high);
        index += 8;
    }
    fill_tail::<L2>(left, right, output, index);
}

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_endian = "little"
))]
#[target_feature(enable = "avx")]
unsafe fn fill_encoded_x86_avx<const L2: bool>(left: &[f32], right: &[u8], output: &mut [f64]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let mut index = 0usize;
    while index + 8 <= left.len() {
        let a_low = _mm256_cvtps_pd(_mm_loadu_ps(left.as_ptr().add(index)));
        let b_low = _mm256_cvtps_pd(_mm_loadu_ps(right.as_ptr().add(index * 4).cast::<f32>()));
        let a_high = _mm256_cvtps_pd(_mm_loadu_ps(left.as_ptr().add(index + 4)));
        let b_high = _mm256_cvtps_pd(_mm_loadu_ps(
            right.as_ptr().add((index + 4) * 4).cast::<f32>(),
        ));
        let low = if L2 {
            let delta = _mm256_sub_pd(a_low, b_low);
            _mm256_mul_pd(delta, delta)
        } else {
            _mm256_mul_pd(a_low, b_low)
        };
        let high = if L2 {
            let delta = _mm256_sub_pd(a_high, b_high);
            _mm256_mul_pd(delta, delta)
        } else {
            _mm256_mul_pd(a_high, b_high)
        };
        _mm256_storeu_pd(output.as_mut_ptr().add(index), low);
        _mm256_storeu_pd(output.as_mut_ptr().add(index + 4), high);
        index += 8;
    }
    fill_encoded_tail::<L2>(&left[index..], &right[index * 4..], &mut output[index..]);
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx,avx512f")]
unsafe fn fill_x86_avx512<const L2: bool>(left: &[f32], right: &[f32], output: &mut [f64]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let mut index = 0usize;
    while index + 8 <= left.len() {
        let a = _mm512_cvtps_pd(_mm256_loadu_ps(left.as_ptr().add(index)));
        let b = _mm512_cvtps_pd(_mm256_loadu_ps(right.as_ptr().add(index)));
        let product = if L2 {
            let delta = _mm512_sub_pd(a, b);
            _mm512_mul_pd(delta, delta)
        } else {
            _mm512_mul_pd(a, b)
        };
        _mm512_storeu_pd(output.as_mut_ptr().add(index), product);
        index += 8;
    }
    fill_tail::<L2>(left, right, output, index);
}

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_endian = "little"
))]
#[target_feature(enable = "avx,avx512f")]
unsafe fn fill_encoded_x86_avx512<const L2: bool>(left: &[f32], right: &[u8], output: &mut [f64]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let mut index = 0usize;
    while index + 8 <= left.len() {
        let a = _mm512_cvtps_pd(_mm256_loadu_ps(left.as_ptr().add(index)));
        let b = _mm512_cvtps_pd(_mm256_loadu_ps(right.as_ptr().add(index * 4).cast::<f32>()));
        let product = if L2 {
            let delta = _mm512_sub_pd(a, b);
            _mm512_mul_pd(delta, delta)
        } else {
            _mm512_mul_pd(a, b)
        };
        _mm512_storeu_pd(output.as_mut_ptr().add(index), product);
        index += 8;
    }
    fill_encoded_tail::<L2>(&left[index..], &right[index * 4..], &mut output[index..]);
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn fill_aarch64_neon<const L2: bool>(left: &[f32], right: &[f32], output: &mut [f64]) {
    use std::arch::aarch64::*;

    let mut index = 0usize;
    while index + 2 <= left.len() {
        let a = vcvt_f64_f32(vld1_f32(left.as_ptr().add(index)));
        let b = vcvt_f64_f32(vld1_f32(right.as_ptr().add(index)));
        let product = if L2 {
            let delta = vsubq_f64(a, b);
            vmulq_f64(delta, delta)
        } else {
            vmulq_f64(a, b)
        };
        vst1q_f64(output.as_mut_ptr().add(index), product);
        index += 2;
    }
    fill_tail::<L2>(left, right, output, index);
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
unsafe fn fill_encoded_aarch64_neon<const L2: bool>(
    left: &[f32],
    right: &[u8],
    output: &mut [f64],
) {
    use std::arch::aarch64::*;

    let mut index = 0usize;
    while index + 2 <= left.len() {
        let a = vcvt_f64_f32(vld1_f32(left.as_ptr().add(index)));
        let b = vcvt_f64_f32(vld1_f32(right.as_ptr().add(index * 4).cast::<f32>()));
        let product = if L2 {
            let delta = vsubq_f64(a, b);
            vmulq_f64(delta, delta)
        } else {
            vmulq_f64(a, b)
        };
        vst1q_f64(output.as_mut_ptr().add(index), product);
        index += 2;
    }
    fill_encoded_tail::<L2>(&left[index..], &right[index * 4..], &mut output[index..]);
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))]
fn fill_tail<const L2: bool>(left: &[f32], right: &[f32], output: &mut [f64], start: usize) {
    for index in start..left.len() {
        let a = f64::from(left[index]);
        let b = f64::from(right[index]);
        output[index] = if L2 {
            let delta = a - b;
            delta * delta
        } else {
            a * b
        };
    }
}

#[cfg(any(
    all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_endian = "little"
    ),
    all(target_arch = "aarch64", target_endian = "little")
))]
fn fill_encoded_tail<const L2: bool>(left: &[f32], right: &[u8], output: &mut [f64]) {
    for (index, (&a, bytes)) in left.iter().zip(right.as_chunks::<4>().0).enumerate() {
        let b = f32::from_bits(u32::from_le_bytes(*bytes));
        output[index] = if L2 {
            let delta = f64::from(a) - f64::from(b);
            delta * delta
        } else {
            f64::from(a) * f64::from(b)
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

    #[test]
    fn encoded_simd_scores_are_bit_identical_to_decoded_scores() {
        let mut state = 0x243f_6a88_85a3_08d3u64;
        for dimensions in 1..=129 {
            let mut left = Vec::with_capacity(dimensions);
            let mut right = Vec::with_capacity(dimensions);
            for _ in 0..dimensions {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                left.push((state as i32) as f32 / 65_536.0);
                state = state.rotate_left(29).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                right.push((state as i32) as f32 / 65_536.0);
            }
            let encoded: Vec<u8> = right
                .iter()
                .flat_map(|component| component.to_bits().to_le_bytes())
                .collect();
            for metric in [
                DistanceMetric::L2Squared,
                DistanceMetric::Cosine,
                DistanceMetric::InnerProduct,
            ] {
                assert_eq!(
                    query_score_encoded(QueryKernel::SimdDeterministic, metric, &left, &encoded,)
                        .to_bits(),
                    score(metric, &left, &right).to_bits(),
                    "metric={metric:?} dimensions={dimensions}"
                );
            }
        }
    }
}
