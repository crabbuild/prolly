use std::cmp::Ordering;

/// Largest finite `f64` strictly below `value` (or negative zero below +0).
pub(super) fn next_down(value: f64) -> f64 {
    if value.is_nan() || value == f64::NEG_INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(0x8000_0000_0000_0001);
    }
    let bits = value.to_bits();
    f64::from_bits(if value.is_sign_positive() {
        bits - 1
    } else {
        bits + 1
    })
}

/// Smallest finite `f64` strictly above `value` (or positive minimum below -0).
pub(super) fn next_up(value: f64) -> f64 {
    if value.is_nan() || value == f64::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(1);
    }
    let bits = value.to_bits();
    f64::from_bits(if value.is_sign_positive() {
        bits + 1
    } else {
        bits - 1
    })
}

/// Deterministic square root rounded toward negative infinity.
///
/// Newton iteration supplies a nearby candidate. An exact integer comparison
/// of IEEE significands then selects the greatest representable value whose
/// square does not exceed the input. No target `sqrt` instruction is used.
pub(super) fn sqrt_down(value: f64) -> f64 {
    debug_assert!(value.is_finite() && value >= 0.0);
    if value == 0.0 {
        return 0.0;
    }

    let exponent = floor_log2(value);
    let guess_exponent = exponent.div_euclid(2);
    let mut guess = f64::from_bits(((guess_exponent + 1023) as u64) << 52);
    for _ in 0..8 {
        guess = (guess + value / guess) * 0.5;
    }

    while square_cmp(guess, value).is_gt() {
        guess = next_down(guess);
    }
    loop {
        let following = next_up(guess);
        if square_cmp(following, value).is_le() {
            guess = following;
        } else {
            return guess;
        }
    }
}

pub(super) fn reciprocal_sqrt(value: f64) -> f64 {
    1.0 / sqrt_down(value)
}

/// Conservative Euclidean covering radius, rounded upward.
#[allow(dead_code)] // Wired into persisted child summaries in the v2 storage slice.
pub(crate) fn euclidean_radius_up(distance_squared: f64, child_radius: f64) -> f64 {
    debug_assert!(distance_squared.is_finite() && distance_squared >= 0.0);
    debug_assert!(child_radius.is_finite() && child_radius >= 0.0);
    if distance_squared == 0.0 {
        return child_radius;
    }
    let root_up = next_up(sqrt_down(distance_squared));
    next_up(root_up + child_radius)
}

/// Conservative squared-L2 lower bound, rounded downward.
#[allow(dead_code)] // Wired into best-first traversal in the v2 search slice.
pub(crate) fn l2_lower_bound_down(distance_squared: f64, radius: f64) -> f64 {
    debug_assert!(distance_squared.is_finite() && distance_squared >= 0.0);
    debug_assert!(radius.is_finite() && radius >= 0.0);
    let separation = sqrt_down(distance_squared) - radius;
    if separation <= 0.0 {
        0.0
    } else {
        next_down(separation * separation).max(0.0)
    }
}

fn floor_log2(value: f64) -> i32 {
    let bits = value.to_bits();
    let encoded = ((bits >> 52) & 0x7ff) as i32;
    if encoded != 0 {
        encoded - 1023
    } else {
        let fraction = bits & ((1u64 << 52) - 1);
        -1074 + (63 - fraction.leading_zeros() as i32)
    }
}

fn square_cmp(candidate: f64, value: f64) -> Ordering {
    if candidate == 0.0 {
        return 0.0f64.total_cmp(&value);
    }
    let (candidate_significand, candidate_exponent) = decompose(candidate);
    let (value_significand, value_exponent) = decompose(value);
    compare_scaled(
        u128::from(candidate_significand) * u128::from(candidate_significand),
        candidate_exponent * 2,
        u128::from(value_significand),
        value_exponent,
    )
}

fn decompose(value: f64) -> (u64, i32) {
    let bits = value.to_bits();
    let fraction = bits & ((1u64 << 52) - 1);
    let encoded = ((bits >> 52) & 0x7ff) as i32;
    if encoded == 0 {
        (fraction, -1074)
    } else {
        ((1u64 << 52) | fraction, encoded - 1023 - 52)
    }
}

fn compare_scaled(left: u128, left_exponent: i32, right: u128, right_exponent: i32) -> Ordering {
    let left_top = (127 - left.leading_zeros() as i32) + left_exponent;
    let right_top = (127 - right.leading_zeros() as i32) + right_exponent;
    match left_top.cmp(&right_top) {
        Ordering::Equal => {
            let common = left_exponent.min(right_exponent);
            let left = left << u32::try_from(left_exponent - common).expect("non-negative shift");
            let right =
                right << u32::try_from(right_exponent - common).expect("non-negative shift");
            left.cmp(&right)
        }
        ordering => ordering,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn software_square_root_has_frozen_directed_bits() {
        assert_eq!(sqrt_down(4.0).to_bits(), 2.0f64.to_bits());
        assert_eq!(sqrt_down(2.0).to_bits(), 0x3ff6_a09e_667f_3bcc);
        assert_eq!(
            sqrt_down(f64::from_bits(1)).to_bits(),
            0x1e60_0000_0000_0000
        );
    }

    #[test]
    fn radius_rounding_is_conservative() {
        let radius = euclidean_radius_up(2.0, 0.0);
        assert_eq!(radius.to_bits(), 0x3ff6_a09e_667f_3bce);
        let bound = l2_lower_bound_down(25.0, 2.0);
        assert!(bound <= 9.0);
        assert!(bound >= next_down(9.0));
    }

    #[test]
    fn software_square_root_brackets_adversarial_finite_inputs() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..10_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let exponent = (state >> 52) % 0x7ff;
            let bits = (exponent << 52) | (state & ((1u64 << 52) - 1));
            let value = f64::from_bits(bits);
            if value == 0.0 {
                continue;
            }
            let root = sqrt_down(value);
            assert!(square_cmp(root, value).is_le(), "input bits {bits:016x}");
            assert!(
                square_cmp(next_up(root), value).is_gt(),
                "input bits {bits:016x}"
            );
        }
    }
}
