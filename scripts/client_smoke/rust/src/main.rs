//! redis-rs runner: run.py's contract (host port protocol cases.json).

use redis::Value;
use serde_json::{json, Value as J};

fn text(b: &[u8]) -> J {
    J::String(String::from_utf8_lossy(b).into_owned())
}

/// A reply as JSON: a map as {"map": [key, value] pairs}, a set as {"set": sorted items}, and a
/// simple string, a verbatim string and a big number each tagged with its kind; a bulk string is a
/// string.
fn norm(v: &Value) -> J {
    match v {
        Value::Nil => J::Null,
        Value::Int(i) => json!(i),
        Value::BulkString(b) => text(b),
        Value::SimpleString(s) => json!({"simple": s}),
        Value::Okay => json!({"simple": "OK"}),
        Value::Array(a) => J::Array(a.iter().map(norm).collect()),
        Value::Set(s) => {
            let mut items: Vec<J> = s.iter().map(norm).collect();
            items.sort_by_key(|x| x.to_string());
            json!({"set": items})
        }
        Value::Map(m) => {
            json!({"map": m.iter().map(|(k, x)| json!([norm(k), norm(x)])).collect::<Vec<_>>()})
        }
        Value::Double(d) if d.is_finite() => json!(d),
        Value::Double(d) => J::String(d.to_string()),
        Value::Boolean(b) => json!(b),
        Value::VerbatimString { format, text } => {
            json!({"verbatim": text, "format": format!("{format:?}")})
        }
        Value::BigNumber(n) => json!({"bignum": n.to_string()}),
        Value::Attribute { data, .. } => norm(data),
        other => J::String(format!("{other:?}")),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let resp = if args[3] == "3" {
        "?protocol=resp3"
    } else {
        ""
    };
    let client = redis::Client::open(format!("redis://{}:{}/{resp}", args[1], args[2])).unwrap();
    let mut con = client.get_connection().unwrap();
    let cases: J = serde_json::from_str(&std::fs::read_to_string(&args[4]).unwrap()).unwrap();
    println!("{}", json!({"client": "redis-rs", "version": "1.8.3"}));
    for case in cases.as_array().unwrap() {
        let cmd: Vec<&str> = case["cmd"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        let mut c = redis::cmd(cmd[0]);
        for a in &cmd[1..] {
            c.arg(*a);
        }
        let line = match c.query::<Value>(&mut con) {
            Ok(v) => json!({"case": case["name"], "reply": norm(&v)}),
            Err(e) => match e.code() {
                Some(code) => json!({"case": case["name"], "error": code}),
                None => panic!("{}: {e}", case["name"]),
            },
        };
        println!("{line}");
    }
}
