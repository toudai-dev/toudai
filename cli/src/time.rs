//! Event time as the query service takes it: Unix nanoseconds, written and
//! read as RFC 3339 UTC. No time zone database is involved, so a bucket or a
//! range means the same instant on every machine.

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// `2026-09-01T07:00:00.123456789Z` (fraction optional, `Z` required) or a
/// decimal count of Unix nanoseconds.
pub fn parse_time(text: &str) -> Result<i64, String> {
    let bad = || format!("{text:?} is not RFC 3339 UTC (2026-09-01T07:00:00Z) or Unix nanoseconds");
    if !text.is_empty()
        && text
            .trim_start_matches('-')
            .bytes()
            .all(|b| b.is_ascii_digit())
    {
        return text.parse().map_err(|_| bad());
    }
    let rest = text.strip_suffix('Z').ok_or_else(bad)?;
    let (date, clock) = rest.split_once('T').ok_or_else(bad)?;
    let (clock, fraction) = clock.split_once('.').unwrap_or((clock, ""));
    let field = |part: &str, len: usize| -> Result<i64, String> {
        if part.len() != len || !part.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        part.parse().map_err(|_| bad())
    };
    let date: Vec<&str> = date.split('-').collect();
    let clock: Vec<&str> = clock.split(':').collect();
    let [year, month, day] = date[..] else {
        return Err(bad());
    };
    let [hour, minute, second] = clock[..] else {
        return Err(bad());
    };
    let (year, month, day) = (field(year, 4)?, field(month, 2)?, field(day, 2)?);
    let (hour, minute, second) = (field(hour, 2)?, field(minute, 2)?, field(second, 2)?);
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(bad());
    }
    let nanos = if fraction.is_empty() && !text.contains('.') {
        0
    } else if (1..=9).contains(&fraction.len()) && fraction.bytes().all(|b| b.is_ascii_digit()) {
        format!("{fraction:0<9}")
            .parse::<i64>()
            .map_err(|_| bad())?
    } else {
        return Err(bad());
    };
    // In i128 and range-checked once, so the RFC 3339 spelling of i64::MIN
    // parses although its whole seconds alone do not fit i64 nanoseconds.
    let seconds = i128::from(days_from_civil(year, month, day) * SECONDS_PER_DAY)
        + i128::from(hour * 3600 + minute * 60 + second);
    i64::try_from(seconds * i128::from(NANOS_PER_SECOND) + i128::from(nanos)).map_err(|_| bad())
}

/// RFC 3339 UTC with the fraction trimmed of trailing zeros.
pub fn format_time(ns: i64) -> String {
    let seconds = ns.div_euclid(NANOS_PER_SECOND);
    let nanos = ns.rem_euclid(NANOS_PER_SECOND);
    let (year, month, day) = civil_from_days(seconds.div_euclid(SECONDS_PER_DAY));
    let of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    let mut out = format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60
    );
    if nanos != 0 {
        out.push('.');
        out.push_str(format!("{nanos:09}").trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// A whole number of `ns`, `us`, `ms`, `s`, `m`, or `h`: `250ms`, `1h`.
pub fn parse_duration(text: &str) -> Result<i64, String> {
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(split);
    let scale: i64 = match unit {
        "ns" => 1,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => NANOS_PER_SECOND,
        "m" => 60 * NANOS_PER_SECOND,
        "h" => 3600 * NANOS_PER_SECOND,
        _ => 0,
    };
    digits
        .parse::<i64>()
        .ok()
        .filter(|_| scale > 0)
        .and_then(|n| n.checked_mul(scale))
        .ok_or_else(|| format!("{text:?} is not a duration such as 500ms, 10s, or 1h"))
}

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

// Howard Hinnant's civil-calendar conversions (proleptic Gregorian).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-09-01T07:00:00.123456789Z, the fixture's first span.
    const TS: i64 = 1_788_246_000_123_456_789;

    #[test]
    fn times_round_trip() {
        let cases = [
            ("2026-09-01T07:00:00.123456789Z", TS),
            ("2026-09-01T07:00:00Z", TS - 123_456_789),
            ("2026-09-01T07:00:00.5Z", TS - 123_456_789 + 500_000_000),
            ("1970-01-01T00:00:00Z", 0),
            ("1969-12-31T23:59:59.999999999Z", -1),
            ("2024-02-29T00:00:00Z", 1_709_164_800 * NANOS_PER_SECOND),
            ("2000-03-01T00:00:00Z", 951_868_800 * NANOS_PER_SECOND),
        ];
        for (text, ns) in cases {
            assert_eq!(parse_time(text), Ok(ns), "{text}");
            assert_eq!(format_time(ns), text, "{ns}");
        }
        assert_eq!(parse_time("1788246000123456789"), Ok(TS));
        for ns in [i64::MIN, i64::MAX] {
            assert_eq!(parse_time(&format_time(ns)), Ok(ns), "{ns}");
        }
        assert!(parse_time("1677-09-21T00:12:43.145224191Z").is_err());
        assert_eq!(parse_time("-1"), Ok(-1));
    }

    #[test]
    fn malformed_times_are_rejected() {
        for text in [
            "",
            "2026-09-01T07:00:00",
            "2026-09-01T07:00:00+00:00",
            "2026-09-01 07:00:00Z",
            "2026-9-01T07:00:00Z",
            "2026-02-29T00:00:00Z",
            "2026-09-31T00:00:00Z",
            "2026-09-01T24:00:00Z",
            "2026-09-01T07:00:60Z",
            "2026-09-01T07:00:00.Z",
            "2026-09-01T07:00:00.1234567890Z",
            "9999999999999999999",
        ] {
            assert!(parse_time(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn durations() {
        let cases = [
            ("1ns", Some(1)),
            ("250us", Some(250_000)),
            ("1ms", Some(1_000_000)),
            ("10s", Some(10 * NANOS_PER_SECOND)),
            ("5m", Some(300 * NANOS_PER_SECOND)),
            ("1h", Some(3600 * NANOS_PER_SECOND)),
            ("1", None),
            ("1.5s", None),
            ("ms", None),
            ("-1s", None),
            ("1d", None),
            ("9999999999999h", None),
        ];
        for (text, expected) in cases {
            assert_eq!(parse_duration(text).ok(), expected, "{text}");
        }
    }
}
