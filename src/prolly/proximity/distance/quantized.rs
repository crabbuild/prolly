use crate::prolly::proximity::DistanceMetric;
use std::sync::OnceLock;

const PRODUCT_SLOTS: usize = 64;

type FillQuantized = unsafe fn(&[f32], &[i8], f64, &mut [f64]);

static FILL_L2: OnceLock<Option<FillQuantized>> = OnceLock::new();
static FILL_DOT: OnceLock<Option<FillQuantized>> = OnceLock::new();

/// Score one scalar-quantized vector while preserving the canonical scalar
/// f64 reduction order. Target-specific code only computes independent
/// per-component products; the final reduction remains sequential.
pub(crate) fn score(
    metric: DistanceMetric,
    query: &[f32],
    values: &[i8],
    scales: &[f32],
    group_size: usize,
) -> f64 {
    debug_assert_eq!(query.len(), values.len());
    debug_assert!(group_size > 0);
    debug_assert_eq!(scales.len(), query.len().div_ceil(group_size));

    let fill = match metric {
        DistanceMetric::L2Squared => *FILL_L2.get_or_init(detect_fill::<true>),
        DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
            *FILL_DOT.get_or_init(detect_fill::<false>)
        }
    };
    let mut products = [0.0f64; PRODUCT_SLOTS];
    let mut reduced = 0.0f64;
    let mut group_start = 0usize;
    while group_start < query.len() {
        let group = group_start / group_size;
        let group_end = group_start.saturating_add(group_size).min(query.len());
        let scale = f64::from(scales[group]);
        let mut index = group_start;
        while index < group_end {
            let end = index.saturating_add(PRODUCT_SLOTS).min(group_end);
            let output = &mut products[..end - index];
            if let Some(fill) = fill {
                // SAFETY: the function pointer is selected only after its
                // target feature is detected, and all slices are bounded by
                // the validated vector length.
                unsafe { fill(&query[index..end], &values[index..end], scale, output) };
            } else {
                match metric {
                    DistanceMetric::L2Squared => {
                        fill_tail::<true>(&query[index..end], &values[index..end], scale, output)
                    }
                    DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
                        fill_tail::<false>(&query[index..end], &values[index..end], scale, output)
                    }
                }
            }
            for &product in output.iter() {
                reduced += product;
            }
            index = end;
        }
        group_start = group_end;
    }

    let result = match metric {
        DistanceMetric::L2Squared => reduced,
        DistanceMetric::Cosine => 1.0 - reduced.clamp(-1.0, 1.0),
        DistanceMetric::InnerProduct => -reduced,
    };
    if result == 0.0 {
        0.0
    } else {
        result
    }
}

fn detect_fill<const L2: bool>() -> Option<FillQuantized> {
    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_endian = "little"
    ))]
    {
        if std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx2")
        {
            return Some(fill_x86_avx512::<L2>);
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            return Some(fill_x86_avx2::<L2>);
        }
    }
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        return Some(fill_aarch64_neon::<L2>);
    }
    #[allow(unreachable_code)]
    None
}

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_endian = "little"
))]
#[target_feature(enable = "avx2")]
unsafe fn fill_x86_avx2<const L2: bool>(
    query: &[f32],
    values: &[i8],
    scale: f64,
    output: &mut [f64],
) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let scalar_scale = scale;
    let scale = _mm256_set1_pd(scale);
    let mut index = 0usize;
    while index + 8 <= query.len() {
        let query_low = _mm256_cvtps_pd(_mm_loadu_ps(query.as_ptr().add(index)));
        let query_high = _mm256_cvtps_pd(_mm_loadu_ps(query.as_ptr().add(index + 4)));
        let ints = _mm256_cvtepi8_epi32(_mm_loadl_epi64(
            values.as_ptr().add(index).cast::<__m128i>(),
        ));
        let values_low = _mm256_cvtepi32_pd(_mm256_castsi256_si128(ints));
        let values_high = _mm256_cvtepi32_pd(_mm256_extracti128_si256(ints, 1));
        let reconstructed_low = _mm256_mul_pd(values_low, scale);
        let reconstructed_high = _mm256_mul_pd(values_high, scale);
        let product_low = if L2 {
            let delta = _mm256_sub_pd(query_low, reconstructed_low);
            _mm256_mul_pd(delta, delta)
        } else {
            _mm256_mul_pd(query_low, reconstructed_low)
        };
        let product_high = if L2 {
            let delta = _mm256_sub_pd(query_high, reconstructed_high);
            _mm256_mul_pd(delta, delta)
        } else {
            _mm256_mul_pd(query_high, reconstructed_high)
        };
        _mm256_storeu_pd(output.as_mut_ptr().add(index), product_low);
        _mm256_storeu_pd(output.as_mut_ptr().add(index + 4), product_high);
        index += 8;
    }
    fill_tail::<L2>(
        &query[index..],
        &values[index..],
        scalar_scale,
        &mut output[index..],
    );
}

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_endian = "little"
))]
#[target_feature(enable = "avx2,avx512f")]
unsafe fn fill_x86_avx512<const L2: bool>(
    query: &[f32],
    values: &[i8],
    scale: f64,
    output: &mut [f64],
) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let scalar_scale = scale;
    let scale = _mm512_set1_pd(scale);
    let mut index = 0usize;
    while index + 8 <= query.len() {
        let query_f64 = _mm512_cvtps_pd(_mm256_loadu_ps(query.as_ptr().add(index)));
        let ints = _mm256_cvtepi8_epi32(_mm_loadl_epi64(
            values.as_ptr().add(index).cast::<__m128i>(),
        ));
        // Every i8 is exactly representable as f32, so this conversion keeps
        // the same exact value as the scalar i8 -> f64 conversion.
        let values_f64 = _mm512_cvtps_pd(_mm256_cvtepi32_ps(ints));
        let reconstructed = _mm512_mul_pd(values_f64, scale);
        let product = if L2 {
            let delta = _mm512_sub_pd(query_f64, reconstructed);
            _mm512_mul_pd(delta, delta)
        } else {
            _mm512_mul_pd(query_f64, reconstructed)
        };
        _mm512_storeu_pd(output.as_mut_ptr().add(index), product);
        index += 8;
    }
    fill_tail::<L2>(
        &query[index..],
        &values[index..],
        scalar_scale,
        &mut output[index..],
    );
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
unsafe fn fill_aarch64_neon<const L2: bool>(
    query: &[f32],
    values: &[i8],
    scale: f64,
    output: &mut [f64],
) {
    use std::arch::aarch64::*;

    let scalar_scale = scale;
    let scale = vdupq_n_f64(scale);
    let mut index = 0usize;
    while index + 8 <= query.len() {
        let query_low = vcvt_f64_f32(vld1_f32(query.as_ptr().add(index)));
        let query_high = vcvt_f64_f32(vld1_f32(query.as_ptr().add(index + 2)));
        let query_next_low = vcvt_f64_f32(vld1_f32(query.as_ptr().add(index + 4)));
        let query_next_high = vcvt_f64_f32(vld1_f32(query.as_ptr().add(index + 6)));

        let values_i16 = vmovl_s8(vld1_s8(values.as_ptr().add(index)));
        let values_low = vcvtq_f32_s32(vmovl_s16(vget_low_s16(values_i16)));
        let values_high = vcvtq_f32_s32(vmovl_s16(vget_high_s16(values_i16)));
        let values_low_low = vcvt_f64_f32(vget_low_f32(values_low));
        let values_low_high = vcvt_f64_f32(vget_high_f32(values_low));
        let values_high_low = vcvt_f64_f32(vget_low_f32(values_high));
        let values_high_high = vcvt_f64_f32(vget_high_f32(values_high));

        let product = |query, values| {
            let reconstructed = vmulq_f64(values, scale);
            if L2 {
                let delta = vsubq_f64(query, reconstructed);
                vmulq_f64(delta, delta)
            } else {
                vmulq_f64(query, reconstructed)
            }
        };
        vst1q_f64(
            output.as_mut_ptr().add(index),
            product(query_low, values_low_low),
        );
        vst1q_f64(
            output.as_mut_ptr().add(index + 2),
            product(query_high, values_low_high),
        );
        vst1q_f64(
            output.as_mut_ptr().add(index + 4),
            product(query_next_low, values_high_low),
        );
        vst1q_f64(
            output.as_mut_ptr().add(index + 6),
            product(query_next_high, values_high_high),
        );
        index += 8;
    }
    fill_tail::<L2>(
        &query[index..],
        &values[index..],
        scalar_scale,
        &mut output[index..],
    );
}

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_endian = "little"
))]
#[inline]
fn fill_tail<const L2: bool>(query: &[f32], values: &[i8], scale: f64, output: &mut [f64]) {
    for (index, (&query, &value)) in query.iter().zip(values).enumerate() {
        let reconstructed = f64::from(value) * scale;
        output[index] = if L2 {
            let delta = f64::from(query) - reconstructed;
            delta * delta
        } else {
            f64::from(query) * reconstructed
        };
    }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[inline]
fn fill_tail<const L2: bool>(query: &[f32], values: &[i8], scale: f64, output: &mut [f64]) {
    for (index, (&query, &value)) in query.iter().zip(values).enumerate() {
        let reconstructed = f64::from(value) * scale;
        output[index] = if L2 {
            let delta = f64::from(query) - reconstructed;
            delta * delta
        } else {
            f64::from(query) * reconstructed
        };
    }
}

#[cfg(not(any(
    all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_endian = "little"
    ),
    all(target_arch = "aarch64", target_endian = "little")
)))]
#[inline]
fn fill_tail<const L2: bool>(query: &[f32], values: &[i8], scale: f64, output: &mut [f64]) {
    for (index, (&query, &value)) in query.iter().zip(values).enumerate() {
        let reconstructed = f64::from(value) * scale;
        output[index] = if L2 {
            let delta = f64::from(query) - reconstructed;
            delta * delta
        } else {
            f64::from(query) * reconstructed
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantized_scores_preserve_scalar_bits_for_group_boundaries() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for dimensions in 1..=129 {
            let mut query = Vec::with_capacity(dimensions);
            let mut values = Vec::with_capacity(dimensions);
            for _ in 0..dimensions {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                query.push((state as i32) as f32 / 65_536.0);
                values.push((state as i8).clamp(-127, 127));
            }
            for group_size in [1, 2, 3, 4, 7, 8, 16, 32] {
                let scales: Vec<_> = (0..dimensions.div_ceil(group_size))
                    .map(|index| (index as f32 + 1.0) / 127.0)
                    .collect();
                for metric in [
                    DistanceMetric::L2Squared,
                    DistanceMetric::Cosine,
                    DistanceMetric::InnerProduct,
                ] {
                    let expected = query.iter().zip(&values).enumerate().fold(
                        0.0f64,
                        |sum, (index, (&query, &value))| {
                            let reconstructed =
                                f64::from(value) * f64::from(scales[index / group_size]);
                            if metric == DistanceMetric::L2Squared {
                                let delta = f64::from(query) - reconstructed;
                                sum + delta * delta
                            } else {
                                sum + f64::from(query) * reconstructed
                            }
                        },
                    );
                    let expected = match metric {
                        DistanceMetric::L2Squared => expected,
                        DistanceMetric::Cosine => 1.0 - expected.clamp(-1.0, 1.0),
                        DistanceMetric::InnerProduct => -expected,
                    };
                    assert_eq!(
                        score(metric, &query, &values, &scales, group_size).to_bits(),
                        (if expected == 0.0 { 0.0 } else { expected }).to_bits(),
                        "metric={metric:?} dimensions={dimensions} group_size={group_size}"
                    );
                }
            }
        }
    }
}
