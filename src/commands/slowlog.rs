use crate::commands::ctx::Ctx;
use crate::resp::write::*;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if name != "SLOWLOG" {
        return None;
    }

    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'slowlog' command",
        ));
    }

    let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();
    match sub.as_str() {
        "GET" => {
            // Default is the latest ten entries; -1 means all of them.
            let count = if cmd.len() >= 3 {
                match std::str::from_utf8(cmd[2])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(-1) => usize::MAX,
                    Some(n) if n >= 0 => n as usize,
                    _ => {
                        return Some(resp_error(
                            "ERR count should be greater than or equal to -1",
                        ));
                    }
                }
            } else {
                10
            };

            let entries = ctx.slowlog.get(count);
            let mut out = format!("*{}\r\n", entries.len()).into_bytes();

            for e in entries {
                // Each entry is a 7-element array. The seventh — the command's
                // argument count before truncation — is new in Redis 8.10.
                out.extend_from_slice(b"*7\r\n");
                out.extend_from_slice(&resp_integer(e.id as i64));
                out.extend_from_slice(&resp_integer(e.timestamp as i64));
                out.extend_from_slice(&resp_integer(e.duration_us as i64));

                // The command as a nested array
                let cmd_arr: Vec<Vec<u8>> = e.cmd.iter().map(|s| resp_bulk_str(s)).collect();
                out.extend_from_slice(&resp_array(cmd_arr));

                out.extend_from_slice(&resp_bulk_str(&e.client_addr));
                out.extend_from_slice(&resp_bulk_str(&e.client_name));
                out.extend_from_slice(&resp_integer(e.argc as i64));
            }

            Some(out)
        }

        "LEN" => Some(resp_integer(ctx.slowlog.len() as i64)),

        "RESET" => {
            ctx.slowlog.reset();
            Some(resp_simple("OK"))
        }

        "HELP" => Some(resp_array(
            [
                "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "GET [<count>]",
                "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
                "    Entries are made of:",
                "    id, timestamp, time in microseconds, arguments array, client IP and port,",
                "    client name, total argument count",
                "LEN",
                "    Return the length of the slowlog.",
                "RESET",
                "    Reset the slowlog.",
                "HELP",
                "    Print this help.",
            ]
            .iter()
            .map(|l| resp_bulk_str(l))
            .collect(),
        )),

        _ => Some(resp_error("ERR unknown subcommand for SLOWLOG")),
    }
}
