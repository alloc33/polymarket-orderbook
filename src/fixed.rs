//! Decimal strings to the fixed-point integers of the SBE schema.

use anyhow::{Context, ensure};

// 1_000_000 is one dollar or one share, as the SBE schema defines.
const DECIMALS: u32 = 6;
pub const SCALE: i64 = 10_i64.pow(DECIMALS);

pub fn parse(text: &str) -> anyhow::Result<i64> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let decimals = u32::try_from(fraction.len())?;
    ensure!(
        decimals <= DECIMALS,
        "more than {DECIMALS} decimals: {text}"
    );

    let whole: i64 = whole
        .parse::<u64>()
        .with_context(|| format!("invalid number: {text}"))?
        .try_into()?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        let digits: u32 = fraction
            .parse()
            .with_context(|| format!("invalid number: {text}"))?;
        i64::from(digits) * 10_i64.pow(DECIMALS - decimals)
    };

    whole
        .checked_mul(SCALE)
        .and_then(|whole| whole.checked_add(fraction))
        .with_context(|| format!("out of range: {text}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_decimal_strings() {
        assert_eq!(parse("0.018").unwrap(), 18_000);
        assert_eq!(parse("1254.57").unwrap(), 1_254_570_000);
        assert_eq!(parse("8275").unwrap(), 8_275_000_000);
        assert_eq!(parse("0").unwrap(), 0);
        assert_eq!(parse("1").unwrap(), SCALE);
        assert_eq!(parse("0.000001").unwrap(), 1);
    }

    #[test]
    fn rejects_malformed_numbers() {
        for text in ["", "abc", "-1", "1.2345678", "1.x", ".5"] {
            assert!(parse(text).is_err(), "{text:?} should be rejected");
        }
    }
}
