pub(crate) mod canonical;
mod quantized;
mod scalar;
mod simd;

pub(crate) use canonical::euclidean_radius_up;
pub(crate) use quantized::score as score_quantized;
pub(crate) use scalar::{prepare_vector, score};
#[cfg(test)]
pub(crate) use simd::{query_kernel_calls, reset_query_kernel_calls};
pub(crate) use simd::{query_score, query_score_encoded};
