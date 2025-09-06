use std::ops::RangeInclusive;

use snafu::{ResultExt, Snafu};

#[derive(Debug, Clone, Snafu)]
pub enum RangeParserError {
    #[snafu(display("Empty range string"))]
    Empty,
    #[snafu(display("Invalid range"))]
    Invalid,
    #[snafu(display("Failed to parse range"))]
    Parse { source: std::num::ParseIntError },
}

pub fn parse_range(range_str: &str) -> Result<RangeInclusive<u64>, RangeParserError> {
    let range_str = range_str.trim();

    match range_str.split("-").collect::<Vec<_>>().as_slice() {
        [] => Err(RangeParserError::Empty),
        [value] => {
            let value = value.trim().parse::<u64>().context(ParseSnafu {})?;

            Ok(value..=value)
        }
        [start, end] => {
            let start = start.trim().parse::<u64>().context(ParseSnafu {})?;
            let end = end.trim().parse::<u64>().context(ParseSnafu {})?;

            if start > end {
                return Err(RangeParserError::Invalid);
            }

            Ok(start..=end)
        }
        _ => Err(RangeParserError::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_value() {
        let range = parse_range("10").unwrap();
        assert_eq!(range, 10..=10);
    }

    #[test]
    fn test_parse_range() {
        let range = parse_range("10-100").unwrap();
        assert_eq!(range, 10..=100);
    }

    #[test]
    fn test_parse_range_with_spaces() {
        let range = parse_range("  5  -  25  ").unwrap();
        assert_eq!(range, 5..=25);
    }

    #[test]
    fn test_parse_invalid_format() {
        assert!(parse_range("10-20-30").is_err());
        assert!(parse_range("abc").is_err());
        assert!(parse_range("10-abc").is_err());
        assert!(parse_range("abc-10").is_err());
    }

    #[test]
    fn test_parse_invalid_range() {
        assert!(parse_range("100-10").is_err());
    }
}
