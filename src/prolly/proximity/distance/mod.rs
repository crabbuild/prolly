pub(crate) mod canonical;
mod scalar;
mod simd;

pub(crate) use canonical::euclidean_radius_up;
pub(crate) use scalar::{prepare_vector, score};
pub(crate) use simd::query_score;
#[cfg(test)]
pub(crate) use simd::{query_kernel_calls, reset_query_kernel_calls};
