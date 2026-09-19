//! `strict_date_optional_time||epoch_millis` — OpenSearch's default date
//! format (issue #86).
//!
//! rSearch used to accept only RFC 3339, which rejects the two forms a
//! projected SQL column most often takes: a date with no time
//! (`2026-09-16`) and a timestamp with no offset (`2026-09-16T00:00:00`).
//! This module parses the whole grammar, on the write path and in the
//! query DSL, so a value OpenSearch indexes is a value rSearch indexes.
//!
//! The grammar is strict: fixed-width fields, `T` as the only date/time
//! separator, and an optional offset that defaults to UTC.
//!
//! ```text
//! yyyy | yyyy-MM | yyyy-MM-dd [ T HH:mm [ :ss [ .S+ ] ] [ Z | ±HH[:]mm | ±HH ] ]
//! ```

use tantivy::time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use crate::document::{MAX_SAFE_MILLIS, epoch_to_millis};

/// The format name OpenSearch reports in its parse failures.
pub const DEFAULT_DATE_FORMAT: &str = "strict_date_optional_time||epoch_millis";

/// Parse a date literal into epoch milliseconds, accepting every form of
/// `strict_date_optional_time||epoch_millis`. An all-digit string is an
/// epoch stamp, as in OpenSearch.
pub fn parse_date_string(value: &str) -> Option<i64> {
    if value.is_empty() {
        return None;
    }
    // OpenSearch tries `strict_date_optional_time` before `epoch_millis`,
    // so a four-digit string is the year 2026, not 2026 milliseconds,
    // while a longer run of digits is an epoch stamp.
    if let Some(millis) = parse_iso(value) {
        return Some(millis);
    }
    let digits = value.strip_prefix('-').unwrap_or(value);
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        return value.parse::<i64>().ok().map(epoch_to_millis);
    }
    None
}

/// Split at the first occurrence of any byte in `chars`, keeping the
/// separator with the remainder.
fn split_at_any<'a>(value: &'a str, chars: &[u8]) -> (&'a str, &'a str) {
    match value.bytes().position(|b| chars.contains(&b)) {
        Some(at) => value.split_at(at),
        None => (value, ""),
    }
}

fn digits<const N: usize>(value: &str) -> Option<u32> {
    (value.len() == N && value.bytes().all(|b| b.is_ascii_digit()))
        .then(|| value.parse::<u32>().ok())
        .flatten()
}

fn parse_iso(value: &str) -> Option<i64> {
    let (date_part, rest) = match value.find('T') {
        Some(at) => (&value[..at], &value[at + 1..]),
        None => (value, ""),
    };
    let date = parse_date(date_part)?;
    if rest.is_empty() {
        return millis_from(date, Time::MIDNIGHT, UtcOffset::UTC);
    }
    let (time_part, offset_part) = split_at_any(rest, b"Z+-");
    let time = parse_time(time_part)?;
    let offset = parse_offset(offset_part)?;
    millis_from(date, time, offset)
}

fn parse_date(value: &str) -> Option<Date> {
    let mut parts = value.split('-');
    let year = digits::<4>(parts.next()?)? as i32;
    let month = match parts.next() {
        Some(m) => Month::try_from(digits::<2>(m)? as u8).ok()?,
        None => Month::January,
    };
    let day = match parts.next() {
        Some(d) => digits::<2>(d)? as u8,
        None => 1,
    };
    if parts.next().is_some() {
        return None;
    }
    Date::from_calendar_date(year, month, day).ok()
}

fn parse_time(value: &str) -> Option<Time> {
    let (clock, fraction) = match value.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (value, None),
    };
    let mut parts = clock.split(':');
    let hour = digits::<2>(parts.next()?)?;
    let minute = digits::<2>(parts.next()?)?;
    let second = match parts.next() {
        Some(s) => digits::<2>(s)?,
        // A fraction needs seconds to attach to.
        None if fraction.is_some() => return None,
        None => 0,
    };
    if parts.next().is_some() {
        return None;
    }
    let nanos = match fraction {
        Some(fraction) => {
            if fraction.is_empty()
                || fraction.len() > 9
                || !fraction.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            let mut padded = String::with_capacity(9);
            padded.push_str(fraction);
            while padded.len() < 9 {
                padded.push('0');
            }
            padded.parse::<u32>().ok()?
        }
        None => 0,
    };
    Time::from_hms_nano(hour as u8, minute as u8, second as u8, nanos).ok()
}

fn parse_offset(value: &str) -> Option<UtcOffset> {
    if value.is_empty() || value == "Z" {
        return Some(UtcOffset::UTC);
    }
    let (sign, rest) = value.split_at(1);
    let sign = match sign {
        "+" => 1i8,
        "-" => -1i8,
        _ => return None,
    };
    let (hours, minutes) = match rest.split_once(':') {
        Some((h, m)) => (digits::<2>(h)?, digits::<2>(m)?),
        None => match rest.len() {
            2 => (digits::<2>(rest)?, 0),
            4 => (digits::<2>(&rest[..2])?, digits::<2>(&rest[2..])?),
            _ => return None,
        },
    };
    UtcOffset::from_hms(sign * hours as i8, sign * minutes as i8, 0).ok()
}

fn millis_from(date: Date, time: Time, offset: UtcOffset) -> Option<i64> {
    let stamp = PrimitiveDateTime::new(date, time).assume_offset(offset);
    Some(offset_millis(stamp))
}

fn offset_millis(stamp: OffsetDateTime) -> i64 {
    let millis = stamp.unix_timestamp_nanos() / 1_000_000;
    millis.clamp(-(MAX_SAFE_MILLIS as i128), MAX_SAFE_MILLIS as i128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(value: &str) -> Option<i64> {
        parse_date_string(value)
    }

    #[test]
    fn parses_every_strict_date_optional_time_form() {
        let midnight = at("2026-09-16T00:00:00Z").unwrap();
        // The forms rSearch used to reject (issue #86).
        assert_eq!(at("2026-09-16"), Some(midnight));
        assert_eq!(at("2026-09-16T00:00:00"), Some(midnight));
        assert_eq!(at("2026-09-16T00:00"), Some(midnight));
        assert_eq!(at("2026-09-16T00:00:00.000"), Some(midnight));
        // And the ones it already accepted.
        assert_eq!(at("2026-09-16T00:00:00.000Z"), Some(midnight));
        assert_eq!(at("2026-09-16T00:00:00+00:00"), Some(midnight));
        assert_eq!(at("2026-09-16T00:00:00+0000"), Some(midnight));
        assert_eq!(at("2026-09-16T01:00:00+01:00"), Some(midnight));
        assert_eq!(at("2026-09-16T01:00:00+01"), Some(midnight));
        assert_eq!(at("2026-09-16T00:00:00.123456789Z"), Some(midnight + 123));
        // Partial dates round down, as OpenSearch does.
        assert_eq!(at("2026-09"), at("2026-09-01T00:00:00Z"));
        assert_eq!(at("2026"), at("2026-01-01T00:00:00Z"));
    }

    #[test]
    fn epoch_stamps_stay_epoch_stamps() {
        assert_eq!(at("1789578275562"), Some(1789578275562));
        // A four-digit string is a year, the way OpenSearch orders the
        // two parsers; longer digit runs are epoch stamps.
        assert_eq!(at("2026"), at("2026-01-01T00:00:00Z"));
        // Past four digits an all-digit string is an epoch stamp. Unlike
        // OpenSearch, which always reads one as milliseconds, rSearch
        // keeps its unit heuristic (shippers send seconds, millis, micros
        // and nanos) — a superset: a millisecond stamp still reads as
        // milliseconds.
        assert_eq!(at("20260916"), Some(20_260_916_000));
        assert_eq!(at("1789578275562"), at("2026-09-16T17:04:35.562Z"));
    }

    #[test]
    fn rejects_what_opensearch_rejects() {
        for bad in [
            "",
            "nonsense",
            "2026-09-16 00:00:00",
            "+2026-09-16",
            "2026-W38-3",
            " 2026-09-16",
            "2026-13-01",
            "2026-09-16T24:00:00Z",
            "2026-9-16",
            "2026-09-16T00:00:00.",
            "2026-09-16T00:00:00+0:00",
            "2026-09-16T00",
        ] {
            assert_eq!(at(bad), None, "{bad} should not parse");
        }
    }
}
