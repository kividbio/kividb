// The values of a GEO field (#1004): which are points, as Redis 8.10 reads
// them, and how a JSON document's are handed to the postings.
//
// A HASH holds one point a field, `"lon,lat"`. A JSON document holds a
// string of that form, or an array of them. A value that is not a point
// keeps the whole document out of the index (`IndexDef::refuse`), as Redis
// keeps it out: what is checked here is what Redis checks, case by case.

use std::sync::Arc;

use crate::geohash::{GEO_LAT_MAX, GEO_LAT_MIN, GEO_LON_MAX, GEO_LON_MIN};

/// What a JSON document's GEO field is projected to (`project_json`): its
/// points, each `"lon,lat"` as written, joined by NUL, which no point holds;
/// or, for a value that refuses the document, a NUL and then why.
const JSON_SEPARATOR: u8 = 0;

/// The point a HASH field holds, as Redis 8.10.3 reads one: the longitude
/// up to the first comma or space, as it is, and the latitude after it,
/// ASCII whitespace around it allowed (`"2.35,48.85 "` and `"2.35 48.85"`
/// are points; `" 2.35,48.85"` and `"2.35 ,48.85"` are not). Each is read
/// as Rust reads an `f64`, then must be finite and within the coordinates a
/// geohash holds. The error is the one Redis records as the document's
/// indexing error, word for word.
pub(crate) fn parse_point(raw: &[u8]) -> Result<(f64, f64), String> {
    let text =
        std::str::from_utf8(raw).map_err(|_| invalid_literal(&String::from_utf8_lossy(raw)))?;
    let Some(at) = text.find([',', ' ']) else {
        return Err("SEARCH_PARSE_ARGS Invalid geo string: missing separator".to_string());
    };
    let number = |s: &str| -> Result<f64, String> {
        s.parse::<f64>()
            .map_err(|e| format!("SEARCH_PARSE_ARGS Invalid geo string \"{s}\": {e}"))
    };
    let lon = number(&text[..at])?;
    let lat = number(text[at + 1..].trim_matches(|c: char| c.is_ascii_whitespace()))?;
    if !lon.is_finite() || !lat.is_finite() {
        return Err("SEARCH_PARSE_ARGS Geo coordinates must be finite".to_string());
    }
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return Err(format!(
            "SEARCH_GEO_COORDINATES_INVALID Invalid geo coordinates: {lon:.6}, {lat:.6}"
        ));
    }
    Ok((lon, lat))
}

fn invalid_literal(s: &str) -> String {
    format!("SEARCH_PARSE_ARGS Invalid geo string \"{s}\": invalid float literal")
}

/// The points a field's value holds, as the index reads them: on a HASH, the
/// one [`parse_point`] reads; on JSON, those [`json_bytes`] projected, or why
/// the document is refused.
pub(crate) fn points(raw: &[u8], json: bool) -> Result<Vec<(f64, f64)>, String> {
    if !json {
        return parse_point(raw).map(|p| vec![p]);
    }
    if let Some(why) = raw.strip_prefix(&[JSON_SEPARATOR]) {
        return Err(String::from_utf8_lossy(why).into_owned());
    }
    raw.split(|b| *b == JSON_SEPARATOR)
        .map(parse_point)
        .collect()
}

/// A JSON document's GEO field, from what its path selects: a string, or
/// an array of strings, each a point; `null` and an empty array are no value
/// (the document is indexed without one). Anything else, or a string that
/// is not a point, refuses the document, as in Redis.
pub(crate) fn json_bytes(
    values: &[std::borrow::Cow<'_, crate::json_tree::Value>],
) -> Option<Arc<[u8]>> {
    use crate::json_tree::{Array, Value};
    let refused = |why: &str| {
        let mut bytes = vec![JSON_SEPARATOR];
        bytes.extend_from_slice(why.as_bytes());
        Some(Arc::from(bytes))
    };
    let mut out: Vec<u8> = Vec::new();
    let take = |s: &str, out: &mut Vec<u8>| -> Result<(), String> {
        parse_point(s.as_bytes())?;
        if !out.is_empty() {
            out.push(JSON_SEPARATOR);
        }
        out.extend_from_slice(s.as_bytes());
        Ok(())
    };
    for value in values {
        let taken = match value.as_ref() {
            Value::Null => Ok(()),
            Value::String(s) => take(s, &mut out),
            Value::Array(Array::Items(items)) => items.iter().try_for_each(|item| match item {
                Value::Null => Ok(()),
                Value::String(s) => take(s, &mut out),
                _ => Err("SEARCH_VALUE_BAD GEO fields can only contain strings or nulls".to_string()),
            }),
            // A packed array holds numbers only.
            Value::Array(Array::Packed(packed)) if packed.is_empty() => Ok(()),
            Value::Number(_) | Value::Array(Array::Packed(_)) => Err(
                "SEARCH_JSON_TYPE_INVALID Invalid JSON type: Numeric type can represent only NUMERIC field"
                    .to_string(),
            ),
            _ => Err("SEARCH_VALUE_BAD GEO fields can only contain strings or nulls".to_string()),
        };
        if let Err(why) = taken {
            return refused(&why);
        }
    }
    (!out.is_empty()).then(|| Arc::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every case here was checked against Redis 8.10.3: `Ok` the ones it
    /// indexes, `Err` those it refuses the document for.
    #[test]
    fn a_hash_value_is_a_point_as_redis_reads_one() {
        for ok in [
            "2.35,48.85",
            "2.35,48.85 ",
            "2.35,48.85  ",
            "2.35 48.85",
            "2.35  48.85",
            "2.35, 48.85 ",
            "2.35,\t48.85",
            "2.35,  48.85",
            "+2.35,48.85",
            "2.35e0,48.85",
            "180,85.05112878",
            "-180,-85.05112878",
            "1e2,10",
            "2.35,48.85\n",
            "2.35, -48.85",
            "-0,0",
            ".5,.5",
            "5.,5.",
        ] {
            assert!(parse_point(ok.as_bytes()).is_ok(), "{ok:?}");
        }
        for refused in [
            " 2.35,48.85",
            "2.35 ,48.85",
            "2.35 , 48.85",
            "2.35\t48.85",
            "2.35 48.85 1",
            "2.35,48.85 x",
            "0x1p1,10",
            "inf,10",
            "nan,10",
            "2.35,,48.85",
            "2.35,48.85,",
            "180,85.05112879",
            "180.0000001,10",
            "2.35,48.85\0",
            "2,3abc",
            "2.35;48.85",
            "abc",
            "2.35",
            "",
            "1,2,3",
            "2.35,86",
            "181,10",
        ] {
            assert!(parse_point(refused.as_bytes()).is_err(), "{refused:?}");
        }
        assert_eq!(
            parse_point(b"2.35").unwrap_err(),
            "SEARCH_PARSE_ARGS Invalid geo string: missing separator"
        );
        assert_eq!(
            parse_point(b"2,3abc").unwrap_err(),
            "SEARCH_PARSE_ARGS Invalid geo string \"3abc\": invalid float literal"
        );
        assert_eq!(
            parse_point(b" 2.35,48.85").unwrap_err(),
            "SEARCH_PARSE_ARGS Invalid geo string \"\": cannot parse float from empty string"
        );
        assert_eq!(
            parse_point(b"2.35 , 48.85").unwrap_err(),
            "SEARCH_PARSE_ARGS Invalid geo string \", 48.85\": invalid float literal"
        );
        assert_eq!(
            parse_point(b"2.35\t48.85").unwrap_err(),
            "SEARCH_PARSE_ARGS Invalid geo string: missing separator"
        );
        for not_finite in ["inf,1", "nan,1", "1,inf", "1,-nan"] {
            assert_eq!(
                parse_point(not_finite.as_bytes()).unwrap_err(),
                "SEARCH_PARSE_ARGS Geo coordinates must be finite",
                "{not_finite:?}"
            );
        }
        assert_eq!(
            parse_point(b"2.35,86").unwrap_err(),
            "SEARCH_GEO_COORDINATES_INVALID Invalid geo coordinates: 2.350000, 86.000000"
        );
    }

    fn projected(json: &str) -> Option<Result<Vec<(f64, f64)>, String>> {
        let doc: crate::json_tree::Value = json.parse().unwrap();
        let query = crate::commands::json_path::Query::parse("$.loc").unwrap();
        let values = query.values(&doc);
        json_bytes(&values).map(|bytes| points(&bytes, true))
    }

    /// JSON, as Redis 8.10.3 indexes it: a string or an array of strings,
    /// `null` and `[]` no value; a number, a numeric array, a nested array, or
    /// an array with one element that is not a point refuses the document.
    #[test]
    fn a_json_value_is_points_as_redis_reads_them() {
        assert_eq!(
            projected(r#"{"loc":"2.35,48.85"}"#),
            Some(Ok(vec![(2.35, 48.85)]))
        );
        assert_eq!(
            projected(r#"{"loc":["2.35,48.85","10,10"]}"#),
            Some(Ok(vec![(2.35, 48.85), (10.0, 10.0)]))
        );
        assert_eq!(projected(r#"{"loc":null}"#), None);
        assert_eq!(projected(r#"{"loc":[]}"#), None);
        assert_eq!(projected(r#"{"other":1}"#), None);
        for refused in [
            r#"{"loc":[2.35,48.85]}"#,
            r#"{"loc":2.35}"#,
            r#"{"loc":"abc"}"#,
            r#"{"loc":["2.35,48.85","bad"]}"#,
            r#"{"loc":["2.35,48.85",5]}"#,
            r#"{"loc":[["2.35,48.85"]]}"#,
            r#"{"loc":{"lon":2.35}}"#,
            r#"{"loc":true}"#,
        ] {
            assert!(matches!(projected(refused), Some(Err(_))), "{refused}");
        }
    }
}
