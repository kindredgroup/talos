use time::{format_description::well_known::Rfc3339, OffsetDateTime};

/// Get the time difference in micro-seconds
pub(crate) fn time_diff_micoseconds(start: OffsetDateTime, end: OffsetDateTime) -> i128 {
    let diff = end - start;
    diff.whole_microseconds()
}

pub(crate) fn parse_header_timestamps(timestamp: &str) -> Option<OffsetDateTime> {
    let timestamp = timestamp.trim();

    // Validate empty timestamp
    if timestamp.is_empty() {
        return None;
    }
    // Parse timestamp in RFC3339 date string (eg. 2026-04-19T20:53:11.407Z)
    if let Ok(ts) = OffsetDateTime::parse(timestamp, &Rfc3339) {
        return Some(ts);
    }
    // Parse timestamp. If the length is less than 13 characters, it doesn't have milliseconds.
    if timestamp.len() == 13 {
        // Parse timestamp in epoch number (eg. 1776631991407)
        if let Ok(ts) = timestamp.parse::<i64>() {
            return OffsetDateTime::from_unix_timestamp_nanos(ts as i128 * 1_000_000).ok();
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use time::{format_description::well_known::Rfc3339, OffsetDateTime};

    use crate::kafka::utils::parse_header_timestamps;

    // Test for valid date string
    #[test]
    fn test_fn_handle_valid_date_string() {
        let date = "2026-04-19T20:53:11.407Z";
        let expected = OffsetDateTime::parse(date, &Rfc3339).unwrap();

        assert!(parse_header_timestamps(date).is_some());
        assert_eq!(parse_header_timestamps(date).unwrap(), expected);
    }
    // Test for invalid date string
    #[test]
    fn test_fn_handle_incorrect_date_string() {
        let date = "2026-049T20:53:11.407Z";

        assert!(parse_header_timestamps(date).is_none());
    }

    // Test for valid epoch date
    #[test]
    fn test_fn_handle_valid_epoch_date() {
        let date = 1776631991407_i128;
        let date_string = date.to_string();
        let expected = OffsetDateTime::from_unix_timestamp_nanos(date * 1_000_000).unwrap();

        let result = parse_header_timestamps(&date_string);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), expected);
    }
    // Test for invalid epoch date
    #[test]
    fn test_fn_handle_incorrect_epoch_date() {
        let date = "1234567";

        assert!(parse_header_timestamps(date).is_none());
    }
    // Tets for empty string
    #[test]
    fn test_fn_handle_empty_string_input_date() {
        let date = "";

        assert!(parse_header_timestamps(date).is_none());
    }
}
