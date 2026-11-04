use std::io::Write;

pub fn resp_simple(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() + 3);
    out.extend_from_slice(b"+");
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

/// `n` in decimal at the end of `out`.
///
/// A length goes in front of every bulk string and every array, so this is
/// on the path of nearly every reply; `write!` goes through the formatting
/// machinery (a `Formatter`, padding, a virtual call) to do the same.
#[inline]
pub(crate) fn push_decimal(out: &mut Vec<u8>, mut n: u64) {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&digits[at..]);
}

/// `{prefix}{n}\r\n` at the end of `out`: the line that begins a bulk
/// string, an array, a map.
#[inline]
pub(crate) fn push_header(out: &mut Vec<u8>, prefix: u8, n: usize) {
    out.push(prefix);
    push_decimal(out, n as u64);
    out.extend_from_slice(b"\r\n");
}

pub fn resp_bulk(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 16);
    push_header(&mut out, b'$', data.len());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

pub fn resp_integer(n: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.push(b':');
    if n < 0 {
        out.push(b'-');
    }
    push_decimal(&mut out, n.unsigned_abs());
    out.extend_from_slice(b"\r\n");
    out
}

pub fn resp_array(items: Vec<Vec<u8>>) -> Vec<u8> {
    let estimated: usize = items.iter().map(|i| i.len()).sum::<usize>() + 16;
    let mut out = Vec::with_capacity(estimated);
    push_header(&mut out, b'*', items.len());
    for item in items {
        out.extend_from_slice(&item);
    }
    out
}

/// RESP3 aggregate map (`%{n}`) where `encoded_kv` is alternating encoded RESP values
/// (typically bulk strings), length `2 * n`.
pub fn resp_map(encoded_kv: Vec<Vec<u8>>) -> Vec<u8> {
    debug_assert!(encoded_kv.len().is_multiple_of(2));
    let n = encoded_kv.len() / 2;
    let estimated: usize = encoded_kv.iter().map(|i| i.len()).sum::<usize>() + 16;
    let mut out = Vec::with_capacity(estimated);
    push_header(&mut out, b'%', n);
    for part in encoded_kv {
        out.extend_from_slice(&part);
    }
    out
}

pub fn resp_error(s: &str) -> Vec<u8> {
    let msg = if s.is_empty() { "ERR" } else { s };
    crate::stats::record_error_reply_stats(msg);

    resp_error_wire(msg)
}

/// Same wire format as [`resp_error`] without bumping error INFO stats (nested script wrappers).
pub fn resp_error_wire(msg: &str) -> Vec<u8> {
    let msg = if msg.is_empty() { "ERR" } else { msg };
    let mut out = Vec::with_capacity(msg.len() + 3);
    out.extend_from_slice(b"-");
    out.extend_from_slice(msg.as_bytes());
    // An error is one line. A message with a line break in it — a Lua
    // error with its traceback, say — would end the reply early and leave
    // the rest to be read as the next one. As in Redis, breaks become
    // spaces.
    if memchr::memchr2(b'\r', b'\n', msg.as_bytes()).is_some() {
        for byte in &mut out[1..] {
            if matches!(*byte, b'\r' | b'\n') {
                *byte = b' ';
            }
        }
    }
    out.extend_from_slice(b"\r\n");
    out
}

pub fn resp_null_bulk() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

pub fn resp_bulk_str(s: &str) -> Vec<u8> {
    resp_bulk(s.as_bytes())
}

/// Shortest human-readable double string — matches Redis `d2string` / `addReplyDouble` better
/// than `{:.17}`, which amplifies binary noise (e.g. `3.141` → a 19-char tail and wrong `$` length).
fn format_redis_double(d: f64) -> String {
    if !d.is_finite() {
        return format!("{d}");
    }
    let mut s = format!("{d}");
    if s == "-0" {
        return "0".to_string();
    }
    if s.contains('.') {
        s = s.trim_end_matches('0').trim_end_matches('.').to_string();
    }
    s
}

/// RESP2 encoding for a Lua `{ double = x }` reply (Redis `addReplyDouble` when `client->resp != 3`).
pub fn resp_double_bulk(d: f64) -> Vec<u8> {
    resp_bulk(format_redis_double(d).as_bytes())
}

/// A command as it goes to a replica and into the AOF.
///
/// Every write that anything is listening to comes through here, so it is
/// sized once and its lengths are written without the formatter.
pub fn encode_resp_command<A: AsRef<[u8]>>(cmd: &[A]) -> Vec<u8> {
    let size: usize = cmd.iter().map(|part| part.as_ref().len() + 16).sum();
    let mut out = Vec::with_capacity(size + 16);
    push_header(&mut out, b'*', cmd.len());
    for part in cmd {
        let part = part.as_ref();
        push_header(&mut out, b'$', part.len());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub fn resp_null_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

// ── RESP3 wire helpers (Redis-compatible) ──────────────────────────────────

/// RESP3 null (`_`).
pub fn resp_null_resp3() -> Vec<u8> {
    b"_\r\n".to_vec()
}

/// If `reply` is *exactly* a RESP2 top-level null (`$-1\r\n` or `*-1\r\n`), rewrite it to the
/// RESP3 null type (`_\r\n`); otherwise pass it through unchanged.
///
/// Command handlers may emit `resp_null_bulk()`/`resp_null_array()` — RESP2's legacy null
/// encoding — for a reply that is *entirely* a null, without checking the negotiated protocol.
/// Strict RESP3 parsers (e.g. modern redis-py) don't reliably treat `$-1`/`*-1` as null once
/// RESP3 is negotiated, which silently breaks nil detection for the *entire* reply of any
/// command whose reply is nothing but that marker (GET on a missing key, EXEC after a dirty
/// WATCH, LPOP on an empty list, ...). Calling this once here, on every command's fully-built
/// reply, fixes that whole class of command without touching each of them individually.
///
/// Nulls nested inside an aggregate are a different matter and are handled at their own sites
/// via [`resp_null_for`] — see the note below.
///
/// This is safe to call unconditionally: RESP is a length-prefixed binary protocol, so a
/// value's own payload bytes can never be mistaken for a *different* reply's leading type
/// marker — `reply == b"$-1\r\n"` only matches when the ENTIRE reply is exactly that 5-byte
/// marker, not as a false positive against a bulk string's payload contents.
///
/// It does **not** reach nulls nested inside a larger structure (e.g. one missing key inside
/// MGET's array) — those still need the RESP3 null type substituted at the command site.
pub fn resp3_normalize_null(reply: Vec<u8>) -> Vec<u8> {
    if reply == b"$-1\r\n" || reply == b"*-1\r\n" {
        resp_null_resp3()
    } else {
        reply
    }
}

/// RESP3 boolean (`#t` / `#f`).
pub fn resp_bool_resp3(b: bool) -> Vec<u8> {
    if b {
        b"#t\r\n".to_vec()
    } else {
        b"#f\r\n".to_vec()
    }
}

/// RESP3 double (`, ` prefix).
pub fn resp_double_resp3(d: f64) -> Vec<u8> {
    let s = format_redis_double(d);
    let mut out = Vec::with_capacity(4 + s.len());
    out.push(b',');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

/// RESP3 big number (`(` ... `\r\n`).
pub fn resp_bignum_resp3(digits: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(3 + digits.len());
    out.push(b'(');
    out.extend_from_slice(digits.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

/// RESP3 set (`~` + `n` elements).
pub fn resp_set_resp3(members: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::new();
    use std::io::Write;
    write!(&mut out, "~{}\r\n", members.len()).unwrap();
    for m in members {
        out.extend_from_slice(&m);
    }
    out
}

/// RESP3 verbatim string (`=`) — `ext` is 1–3 chars, padded to 3 (Redis `addReplyVerbatim`).
pub fn resp_verbatim_resp3(ext: &str, payload: &str) -> Vec<u8> {
    let mut extb = [b' '; 3];
    for (i, b) in ext.as_bytes().iter().take(3).enumerate() {
        extb[i] = *b;
    }
    let mut body: Vec<u8> = Vec::with_capacity(4 + payload.len());
    body.extend_from_slice(&extb);
    body.push(b':');
    body.extend_from_slice(payload.as_bytes());
    let n = body.len();
    use std::io::Write;
    let mut out = Vec::new();
    write!(&mut out, "={}\r\n", n).unwrap();
    out.extend_from_slice(&body);
    out.extend_from_slice(b"\r\n");
    out
}

// ── Protocol-shaped constructors ───────────────────────────────────────────
//
// Each takes the reply's effective RESP version (see
// `commands::handler::effective_resp_for_reply`) and emits the right type for
// it. Use these instead of hand-rolling `if resp == 3 { .. } else { .. }` at
// each site, and *especially* instead of hardcoding the RESP2 form.
//
// These matter for values nested inside a larger reply. `resp3_normalize_null`
// only rewrites a reply that IS, in its entirety, a top-level null; a null
// sitting in one slot of an MGET array is invisible to it and has to be
// emitted correctly here.

/// Null shaped for `resp` — RESP3's `_` or RESP2's `$-1`.
#[inline]
pub fn resp_null_for(resp: u8) -> Vec<u8> {
    if resp >= 3 {
        resp_null_resp3()
    } else {
        resp_null_bulk()
    }
}

/// Double shaped for `resp` — RESP3's `,` or RESP2's bulk string.
///
/// RESP2 has no double type, so scores/distances have always gone out as bulk
/// strings; under RESP3 a client (redis-py included) expects a real double and
/// hands back a native float.
///
/// Not currently called: sorted sets and geo each format their RESP2 branch
/// with their own existing helper (`format_score`, `format_dist`) so their
/// RESP2 bytes stay identical, and route only the RESP3 branch through
/// [`resp_double_resp3`]. Kept as the canonical form for any new site that has
/// no such legacy formatting to preserve.
#[allow(dead_code)]
#[inline]
pub fn resp_double_for(resp: u8, d: f64) -> Vec<u8> {
    if resp >= 3 {
        resp_double_resp3(d)
    } else {
        resp_double_bulk(d)
    }
}

/// Set shaped for `resp` — RESP3's `~` or RESP2's `*`.
#[inline]
pub fn resp_set_for(resp: u8, members: Vec<Vec<u8>>) -> Vec<u8> {
    if resp >= 3 {
        resp_set_resp3(members)
    } else {
        resp_array(members)
    }
}

/// Map shaped for `resp` — RESP3's `%` or RESP2's flat `*` of alternating k/v.
///
/// `encoded_kv` is alternating already-encoded key/value pairs, length `2 * n`,
/// identical in both branches; only the header differs.
#[inline]
pub fn resp_map_for(resp: u8, encoded_kv: Vec<Vec<u8>>) -> Vec<u8> {
    if resp >= 3 {
        resp_map(encoded_kv)
    } else {
        resp_array(encoded_kv)
    }
}

/// Write a single bulk string into an existing buffer.
/// Used by commands that build their entire response in one pass
/// (LRANGE, SMEMBERS, HGETALL, ZRANGE, KEYS, etc.) to avoid
/// the N+1 allocation pattern of collect() + resp_array().
#[inline]
pub fn write_bulk(buf: &mut Vec<u8>, data: &[u8]) {
    buf.push(b'$');
    write!(buf, "{}", data.len()).unwrap();
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod decimal_tests {
    use super::{resp_bulk, resp_integer};

    #[test]
    fn integers_are_written_as_format_writes_them() {
        for n in [
            0,
            1,
            9,
            10,
            99,
            100,
            -1,
            -10,
            1234567890,
            i64::MAX,
            i64::MIN,
        ] {
            assert_eq!(resp_integer(n), format!(":{n}\r\n").into_bytes());
        }
    }

    #[test]
    fn a_bulk_string_carries_its_length() {
        assert_eq!(resp_bulk(b""), b"$0\r\n\r\n");
        assert_eq!(resp_bulk(b"hello"), b"$5\r\nhello\r\n");
        let long = vec![b'x'; 1024];
        assert!(resp_bulk(&long).starts_with(b"$1024\r\n"));
    }
}
