pub(crate) mod canonical;
mod scalar;

pub(crate) use canonical::euclidean_radius_up;
pub(crate) use scalar::{prepare_vector, score};
