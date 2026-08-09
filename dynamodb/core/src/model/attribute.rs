use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use num_bigint::BigInt;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, Result};

/// Deterministic item representation. Attribute names are ordered by UTF-8 bytes.
pub type Item = BTreeMap<String, AttributeValue>;

/// Exact normalized DynamoDB decimal number.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DynamoNumber(String);

impl DynamoNumber {
    pub const MAX_PRECISION: usize = 38;
    pub const MIN_ADJUSTED_EXPONENT: i32 = -130;
    pub const MAX_ADJUSTED_EXPONENT: i32 = 125;

    /// Parse and normalize a finite base-10 number without binary floating point.
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            return Err(Error::Validation("number must not be empty".into()));
        }
        let (negative, unsigned) = match input.as_bytes()[0] {
            b'-' => (true, &input[1..]),
            b'+' => (false, &input[1..]),
            _ => (false, input),
        };
        if unsigned.is_empty() {
            return Err(Error::Validation(format!("invalid number {input:?}")));
        }
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(index) => {
                if unsigned[index + 1..].contains(['e', 'E']) {
                    return Err(Error::Validation(format!("invalid number {input:?}")));
                }
                let exponent = unsigned[index + 1..]
                    .parse::<i32>()
                    .map_err(|_| Error::Validation(format!("invalid exponent in {input:?}")))?;
                (&unsigned[..index], exponent)
            }
            None => (unsigned, 0),
        };
        let mut seen_dot = false;
        let mut fractional_digits = 0_i32;
        let mut digits = String::with_capacity(mantissa.len());
        for byte in mantissa.bytes() {
            match byte {
                b'0'..=b'9' => {
                    digits.push(byte as char);
                    if seen_dot {
                        fractional_digits = fractional_digits
                            .checked_add(1)
                            .ok_or_else(|| Error::Validation("number exponent overflow".into()))?;
                    }
                }
                b'.' if !seen_dot => seen_dot = true,
                _ => return Err(Error::Validation(format!("invalid number {input:?}"))),
            }
        }
        if digits.is_empty() {
            return Err(Error::Validation(format!("invalid number {input:?}")));
        }
        let first_nonzero = digits.find(|character| character != '0');
        let Some(first_nonzero) = first_nonzero else {
            return Ok(Self("0".into()));
        };
        let mut digits = digits[first_nonzero..].to_string();
        let mut scale = fractional_digits
            .checked_sub(exponent)
            .ok_or_else(|| Error::Validation("number exponent overflow".into()))?;
        while digits.ends_with('0') {
            digits.pop();
            scale = scale
                .checked_sub(1)
                .ok_or_else(|| Error::Validation("number exponent overflow".into()))?;
        }
        if digits.len() > Self::MAX_PRECISION {
            return Err(Error::Validation(format!(
                "number exceeds {} significant digits",
                Self::MAX_PRECISION
            )));
        }
        let adjusted = i32::try_from(digits.len())
            .map_err(|_| Error::Validation("number precision overflow".into()))?
            .checked_sub(scale)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| Error::Validation("number exponent overflow".into()))?;
        if !(Self::MIN_ADJUSTED_EXPONENT..=Self::MAX_ADJUSTED_EXPONENT).contains(&adjusted) {
            return Err(Error::Validation(format!(
                "number adjusted exponent {adjusted} is outside {}..={}",
                Self::MIN_ADJUSTED_EXPONENT,
                Self::MAX_ADJUSTED_EXPONENT
            )));
        }

        let mut canonical = String::new();
        if negative {
            canonical.push('-');
        }
        if scale <= 0 {
            canonical.push_str(&digits);
            canonical.extend(std::iter::repeat_n('0', (-scale) as usize));
        } else if scale < digits.len() as i32 {
            let split = digits.len() - scale as usize;
            canonical.push_str(&digits[..split]);
            canonical.push('.');
            canonical.push_str(&digits[split..]);
        } else {
            canonical.push_str("0.");
            canonical.extend(std::iter::repeat_n('0', scale as usize - digits.len()));
            canonical.push_str(&digits);
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// DynamoDB's documented internal size estimate: one byte per two
    /// significant digits, rounded up, plus one byte.
    pub fn storage_size(&self) -> usize {
        let significant_digits = self.0.bytes().filter(u8::is_ascii_digit).count();
        significant_digits.div_ceil(2) + 1
    }

    /// Add two exact DynamoDB decimals without binary floating-point.
    pub fn checked_add(&self, other: &Self) -> Result<Self> {
        let (left, left_scale) = self.coefficient_and_scale()?;
        let (right, right_scale) = other.coefficient_and_scale()?;
        let scale = left_scale.max(right_scale);
        let left = left * pow10(scale - left_scale)?;
        let right = right * pow10(scale - right_scale)?;
        Self::from_coefficient(left + right, scale)
    }

    /// Subtract two exact DynamoDB decimals without binary floating-point.
    pub fn checked_sub(&self, other: &Self) -> Result<Self> {
        let (left, left_scale) = self.coefficient_and_scale()?;
        let (right, right_scale) = other.coefficient_and_scale()?;
        let scale = left_scale.max(right_scale);
        let left = left * pow10(scale - left_scale)?;
        let right = right * pow10(scale - right_scale)?;
        Self::from_coefficient(left - right, scale)
    }

    /// Compare exact decimal values numerically without binary floating point.
    pub fn numeric_cmp(&self, other: &Self) -> Result<Ordering> {
        let (left, left_scale) = self.coefficient_and_scale()?;
        let (right, right_scale) = other.coefficient_and_scale()?;
        let scale = left_scale.max(right_scale);
        Ok((left * pow10(scale - left_scale)?).cmp(&(right * pow10(scale - right_scale)?)))
    }

    fn coefficient_and_scale(&self) -> Result<(BigInt, usize)> {
        let (negative, magnitude) = self
            .0
            .strip_prefix('-')
            .map_or((false, self.0.as_str()), |value| (true, value));
        let scale = magnitude
            .find('.')
            .map_or(0, |index| magnitude.len() - index - 1);
        let digits = magnitude.replace('.', "");
        let mut coefficient = BigInt::from_str(&digits)
            .map_err(|_| Error::CorruptData("validated number has invalid digits".into()))?;
        if negative {
            coefficient = -coefficient;
        }
        Ok((coefficient, scale))
    }

    fn from_coefficient(coefficient: BigInt, scale: usize) -> Result<Self> {
        let negative = coefficient.sign() == num_bigint::Sign::Minus;
        let mut digits = coefficient.magnitude().to_str_radix(10);
        if scale > 0 {
            if digits.len() <= scale {
                digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
            }
            digits.insert(digits.len() - scale, '.');
        }
        if negative {
            digits.insert(0, '-');
        }
        Self::parse(&digits)
    }

    pub(crate) fn ordered_bytes(&self) -> Vec<u8> {
        if self.0 == "0" {
            return vec![1];
        }
        let (negative, magnitude) = self
            .0
            .strip_prefix('-')
            .map_or((false, self.0.as_str()), |value| (true, value));
        let decimal = magnitude.find('.');
        let integer_digits = decimal.unwrap_or(magnitude.len());
        let digits = magnitude
            .bytes()
            .filter(|byte| *byte != b'.')
            .collect::<Vec<_>>();
        let leading_zeros = digits.iter().take_while(|byte| **byte == b'0').count();
        let adjusted = integer_digits as i32 - leading_zeros as i32 - 1;
        let biased = u16::try_from(adjusted - Self::MIN_ADJUSTED_EXPONENT)
            .expect("validated adjusted exponent fits u16");
        let mut payload = Vec::with_capacity(digits.len() + 4);
        payload.extend_from_slice(&biased.to_be_bytes());
        payload.extend_from_slice(&digits[leading_zeros..]);
        payload.push(0);
        if negative {
            let mut encoded = vec![0];
            encoded.extend(payload.into_iter().map(|byte| !byte));
            encoded
        } else {
            let mut encoded = vec![2];
            encoded.extend(payload);
            encoded
        }
    }
}

fn pow10(exponent: usize) -> Result<BigInt> {
    let exponent = u32::try_from(exponent)
        .map_err(|_| Error::Validation("decimal scale exceeds supported range".into()))?;
    Ok(BigInt::from(10_u8).pow(exponent))
}

impl fmt::Display for DynamoNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for DynamoNumber {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl Serialize for DynamoNumber {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DynamoNumber {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

/// Transport-independent DynamoDB attribute value subset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeValue {
    B(Vec<u8>),
    Bool(bool),
    Bs(Vec<Vec<u8>>),
    L(Vec<AttributeValue>),
    M(BTreeMap<String, AttributeValue>),
    N(DynamoNumber),
    Ns(Vec<DynamoNumber>),
    Null(bool),
    S(String),
    Ss(Vec<String>),
}
