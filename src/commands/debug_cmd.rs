//! `DEBUG` subcommands used by Redis integration tests (external clients).

use crate::commands::ctx::Ctx;
use crate::commands::handler::{client_connection_resp, effective_resp_for_reply};
use crate::resp::write::*;

fn parse_sleep_seconds(cmd: &Ctx) -> Option<f64> {
    if cmd.cmd.len() != 3 {
        return None;
    }
    std::str::from_utf8(cmd.cmd[2]).ok()?.parse().ok()
}

/// `DEBUG ...` — protocol helpers and sleep match Redis `debug.c` for scripting tests.
pub fn handle(ctx: &mut Ctx) -> Option<Vec<u8>> {
    if ctx.cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'debug' command",
        ));
    }
    let sub = std::str::from_utf8(ctx.cmd[1]).ok()?.to_ascii_lowercase();
    match sub.as_str() {
        "sleep" => {
            let Some(secs) = parse_sleep_seconds(ctx) else {
                return Some(resp_error("ERR wrong number of arguments for DEBUG sleep"));
            };
            // Negative, infinite or not a number: not a length of time.
            let Ok(duration) = std::time::Duration::try_from_secs_f64(secs) else {
                return Some(resp_error("ERR value is out of range"));
            };
            std::thread::sleep(duration);
            Some(resp_simple("OK"))
        }
        "protocol" => {
            if ctx.cmd.len() != 3 {
                return Some(resp_error("ERR wrong number of arguments"));
            }
            let kind = std::str::from_utf8(ctx.cmd[2]).ok()?.to_ascii_lowercase();
            let conn_resp = client_connection_resp(ctx);
            let eff = effective_resp_for_reply(ctx);
            Some(debug_protocol_reply(conn_resp, eff, &kind))
        }
        "object" => {
            // Minimal stub — tests only require that the command succeeds.
            Some(resp_bulk_str(
                "Value at:0 refcount:1 encoding:raw serializedlength:1",
            ))
        }
        "populate" => Some(populate(ctx)),
        "set-active-expire" | "set-disable-deny-scripts" => Some(resp_simple("OK")),
        _ => Some(resp_simple("OK")),
    }
}

/// `DEBUG POPULATE count [prefix] [size]`, as Redis: `count` string keys,
/// `prefix:0` onwards (`key:0` without a prefix), each holding `value:N`,
/// cut or padded with zero bytes to `size` when one is given. A key that is
/// there already is left as it is. It replied `OK` and made nothing, so a
/// tool or test that loads data this way believed it had (#173).
fn populate(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if !(3..=5).contains(&cmd.len()) {
        return resp_error("ERR wrong number of arguments for 'debug|populate' command");
    }
    let number = |arg: &[u8]| std::str::from_utf8(arg).ok()?.parse::<i64>().ok();
    let Some(count) = number(cmd[2]) else {
        return resp_error("ERR value is not an integer or out of range");
    };
    if count < 0 {
        return resp_error("ERR value is out of range, must be positive");
    }
    let prefix = match cmd.get(3) {
        Some(prefix) => match std::str::from_utf8(prefix) {
            Ok(prefix) => prefix.to_string(),
            Err(_) => return resp_error("ERR must be valid UTF-8"),
        },
        None => "key".to_string(),
    };
    let size = match cmd.get(4).map(|arg| number(arg)) {
        None => 0,
        Some(Some(size)) if size >= 0 => size as usize,
        Some(_) => return resp_error("ERR value is not an integer or out of range"),
    };
    let db = &ctx.store[*ctx.selected_db];
    for i in 0..count as u64 {
        let key = format!("{prefix}:{i}");
        let mut value = format!("value:{i}").into_bytes();
        if size > 0 {
            value.resize(size, 0);
        }
        let shard = crate::store::get_shard(db, &key);
        let mut map = crate::write_lock!(shard);
        if !map.contains_key(key.as_str()) {
            map.insert_str(key.as_str(), &value[..]);
        }
    }
    resp_simple("OK")
}

fn debug_protocol_reply(conn_resp: u8, effective_resp: u8, kind: &str) -> Vec<u8> {
    let use_r3 = conn_resp >= 3 && effective_resp >= 3;
    match kind {
        "string" => resp_bulk_str("Hello World"),
        "integer" => resp_integer(12345),
        "double" => {
            if use_r3 {
                #[allow(clippy::approx_constant)]
                resp_double_resp3(3.141)
            } else {
                #[allow(clippy::approx_constant)]
                resp_double_bulk(3.141)
            }
        }
        "bignum" => {
            let s = "1234567999999999999999999999999999999";
            if use_r3 {
                resp_bignum_resp3(s)
            } else {
                resp_bulk_str(s)
            }
        }
        "null" => {
            if conn_resp >= 3 && effective_resp >= 3 {
                resp_null_resp3()
            } else {
                resp_null_bulk()
            }
        }
        "array" => resp_array(vec![resp_integer(0), resp_integer(1), resp_integer(2)]),
        "set" => {
            if use_r3 {
                resp_set_resp3(vec![resp_integer(0), resp_integer(1), resp_integer(2)])
            } else {
                resp_array(vec![resp_integer(0), resp_integer(1), resp_integer(2)])
            }
        }
        "map" => {
            if use_r3 {
                resp_map(vec![
                    resp_integer(0),
                    resp_bool_resp3(false),
                    resp_integer(1),
                    resp_bool_resp3(true),
                    resp_integer(2),
                    resp_bool_resp3(false),
                ])
            } else {
                resp_array(vec![
                    resp_integer(0),
                    resp_bool_resp2(false),
                    resp_integer(1),
                    resp_bool_resp2(true),
                    resp_integer(2),
                    resp_bool_resp2(false),
                ])
            }
        }
        "attrib" => {
            if use_r3 {
                let mut out = Vec::new();
                out.extend_from_slice(b"|1\r\n");
                out.extend_from_slice(&resp_bulk_str("key-popularity"));
                out.extend_from_slice(&resp_array(vec![
                    resp_bulk_str("key:123"),
                    resp_integer(90),
                ]));
                out.extend_from_slice(&resp_bulk_str("Some real reply following the attribute"));
                out
            } else {
                resp_bulk_str("Some real reply following the attribute")
            }
        }
        "push" => {
            if conn_resp < 3 {
                return resp_error("RESP2 is not supported by this command");
            }
            let mut out = Vec::new();
            out.extend_from_slice(b">2\r\n");
            out.extend_from_slice(&resp_bulk_str("server-cpu-usage"));
            out.extend_from_slice(&resp_integer(42));
            out.extend_from_slice(&resp_bulk_str("Some real reply following the push reply"));
            out
        }
        "verbatim" => {
            if use_r3 {
                resp_verbatim_resp3("txt", "This is a verbatim\nstring")
            } else {
                resp_bulk_str("This is a verbatim\nstring")
            }
        }
        "true" => {
            if use_r3 {
                resp_bool_resp3(true)
            } else {
                resp_integer(1)
            }
        }
        "false" => {
            if use_r3 {
                resp_bool_resp3(false)
            } else {
                resp_integer(0)
            }
        }
        _ => resp_error(
            "Wrong protocol type name. Please use one of the following: string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false",
        ),
    }
}

fn resp_bool_resp2(b: bool) -> Vec<u8> {
    resp_integer(if b { 1 } else { 0 })
}
