// Minimal strftime-compatible formatter.
//
// Implements the strftime specifiers used by FT.AGGREGATE APPLY's timefmt()
// and that real-world Redis queries actually send. We deliberately avoid
// pulling in chrono — the supported set is small enough that hand-rolling
// is clearer and dependency-free.
//
// Supported specifiers (all UTC):
//   %Y  4-digit year
//   %m  2-digit month (01-12)
//   %d  2-digit day-of-month (01-31)
//   %H  2-digit hour (00-23)
//   %M  2-digit minute (00-59)
//   %S  2-digit second (00-59)
//   %j  3-digit day-of-year (001-366)
//   %F  shorthand for %Y-%m-%d
//   %T  shorthand for %H:%M:%S
//   %p  AM or PM
//   %A  full weekday name (Sunday..Saturday)
//   %B  full month name (January..December)
//   %%  literal '%'
//
// Unrecognised specifiers are emitted verbatim ("%X" stays "%X") so users
// can spot typos in their format strings.

/// Format a Unix timestamp (seconds since 1970-01-01 UTC) using a
/// strftime-style format string. Returns None for timestamps outside
/// the representable range (roughly years 1..9999).
pub fn format_unix_seconds(ts: i64, fmt: &str) -> Option<String> {
    let date = unix_seconds_to_civil(ts)?;

    let mut out = String::with_capacity(fmt.len() + 16);
    let bytes = fmt.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 1 < bytes.len() {
            i += 1;
            match bytes[i] {
                b'Y' => out.push_str(&format!("{:04}", date.year)),
                b'm' => out.push_str(&format!("{:02}", date.month)),
                b'd' => out.push_str(&format!("{:02}", date.day)),
                b'H' => out.push_str(&format!("{:02}", date.hour)),
                b'M' => out.push_str(&format!("{:02}", date.minute)),
                b'S' => out.push_str(&format!("{:02}", date.second)),
                b'j' => out.push_str(&format!("{:03}", date.day_of_year)),
                b'F' => out.push_str(&format!(
                    "{:04}-{:02}-{:02}",
                    date.year, date.month, date.day
                )),
                b'T' => out.push_str(&format!(
                    "{:02}:{:02}:{:02}",
                    date.hour, date.minute, date.second
                )),
                b'p' => out.push_str(if date.hour < 12 { "AM" } else { "PM" }),
                b'A' => out.push_str(weekday_name(date.weekday)),
                b'B' => out.push_str(month_name(date.month)),
                b'%' => out.push('%'),
                other => {
                    out.push('%');
                    out.push(other as char);
                }
            }
            i += 1;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    Some(out)
}

#[derive(Debug)]
struct CivilDate {
    year: i32,
    month: u32,       // 1-12
    day: u32,         // 1-31
    hour: u32,        // 0-23
    minute: u32,      // 0-59
    second: u32,      // 0-59
    day_of_year: u32, // 1-366
    weekday: u32,     // 0 = Sunday, 6 = Saturday
}

/// Convert Unix seconds (UTC) to a civil date.
///
/// Algorithm: Howard Hinnant's date.h "civil from days" — the canonical
/// O(1) algorithm for proleptic Gregorian. Cleanly handles negative
/// timestamps (pre-1970) and arbitrary distant dates.
/// Reference: https://howardhinnant.github.io/date_algorithms.html
fn unix_seconds_to_civil(ts: i64) -> Option<CivilDate> {
    // Range guard: anything outside roughly years 1..9999.
    // Below year 1 the proleptic calendar gets dicey; above 9999 the
    // 4-digit year format breaks. Refuse rather than emit garbage.
    const MIN: i64 = -62_135_596_800; // 0001-01-01T00:00:00Z
    const MAX: i64 = 253_402_300_799; // 9999-12-31T23:59:59Z
    if !(MIN..=MAX).contains(&ts) {
        return None;
    }

    // Split into days-since-epoch and seconds-within-day.
    // Rust's integer division truncates toward zero; we want floor.
    // div_euclid / rem_euclid give us floor-division for negatives too.
    let days = ts.div_euclid(86_400);
    let secs_of_day = ts.rem_euclid(86_400) as u32;

    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    // Hinnant's civil_from_days, ported. Input is days-since-1970-01-01.
    let z = days + 719_468; // shift epoch to 0000-03-01
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };

    // Day-of-year: civil_from_days gives March-1-anchored doy; convert
    // to January-1-anchored. We compute directly from (y, m, d) to avoid
    // re-deriving from the era arithmetic.
    let doy_jan = day_of_year_from_civil(y as i32, m, d);

    // Weekday: 1970-01-01 was Thursday (4). Add days mod 7, handle negative.
    let weekday = ((days + 4).rem_euclid(7)) as u32;

    Some(CivilDate {
        year: y as i32,
        month: m,
        day: d,
        hour,
        minute,
        second,
        day_of_year: doy_jan,
        weekday,
    })
}

fn day_of_year_from_civil(year: i32, month: u32, day: u32) -> u32 {
    // Cumulative days at the start of each month for a non-leap year
    const CUM: [u32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let leap_bonus = if month > 2 && is_leap_year(year) {
        1
    } else {
        0
    };
    CUM[(month - 1) as usize] + day + leap_bonus
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn weekday_name(w: u32) -> &'static str {
    match w {
        0 => "Sunday",
        1 => "Monday",
        2 => "Tuesday",
        3 => "Wednesday",
        4 => "Thursday",
        5 => "Friday",
        6 => "Saturday",
        _ => "",
    }
}

fn month_name(m: u32) -> &'static str {
    match m {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        12 => "December",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A handful of dates with known truth values from `date -u -r <ts>`
    // on a Unix system. Covers epoch, recent, leap years, and the boundary
    // cases that have historically broken naive implementations.

    #[test]
    fn epoch() {
        assert_eq!(
            format_unix_seconds(0, "%Y-%m-%d %H:%M:%S").unwrap(),
            "1970-01-01 00:00:00"
        );
    }

    #[test]
    fn iso_default() {
        assert_eq!(
            format_unix_seconds(1_700_000_000, "%FT%TZ").unwrap(),
            "2023-11-14T22:13:20Z"
        );
    }

    #[test]
    fn leap_day() {
        // 2024-02-29 12:00:00 UTC
        assert_eq!(
            format_unix_seconds(1_709_208_000, "%F").unwrap(),
            "2024-02-29"
        );
    }

    #[test]
    fn day_of_year_after_leap() {
        // 2024-03-01 — should be day 61 (because Feb has 29 days)
        assert_eq!(format_unix_seconds(1_709_251_200, "%j").unwrap(), "061");
    }

    #[test]
    fn day_of_year_non_leap() {
        // 2023-03-01 — day 60
        assert_eq!(format_unix_seconds(1_677_628_800, "%j").unwrap(), "060");
    }

    #[test]
    fn weekday_name_test() {
        // 1970-01-01 was Thursday
        assert_eq!(format_unix_seconds(0, "%A").unwrap(), "Thursday");
        // 2024-01-01 was Monday
        assert_eq!(format_unix_seconds(1_704_067_200, "%A").unwrap(), "Monday");
    }

    #[test]
    fn unknown_specifier_passes_through() {
        // %X is unsupported — pass through
        assert_eq!(
            format_unix_seconds(0, "year=%Y other=%X").unwrap(),
            "year=1970 other=%X"
        );
    }

    #[test]
    fn out_of_range_returns_none() {
        assert!(format_unix_seconds(i64::MIN, "%F").is_none());
        assert!(format_unix_seconds(i64::MAX, "%F").is_none());
    }

    #[test]
    fn am_pm() {
        // Midnight, noon, and 1pm
        assert_eq!(format_unix_seconds(0, "%H %p").unwrap(), "00 AM");
        assert_eq!(format_unix_seconds(43_200, "%H %p").unwrap(), "12 PM");
        assert_eq!(format_unix_seconds(46_800, "%H %p").unwrap(), "13 PM");
    }
}
