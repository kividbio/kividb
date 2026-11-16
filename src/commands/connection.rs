use crate::commands::ctx::Ctx;
use crate::resp::write::*;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    match name {
        "PING" => {
            if cmd.len() > 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'ping' command",
                ));
            }
            if cmd.len() >= 2 {
                Some(resp_bulk(cmd[1]))
            } else {
                Some(resp_simple("PONG"))
            }
        }

        "ECHO" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'echo' command",
                ));
            }

            Some(resp_bulk(cmd[1]))
        }

        "SELECT" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'select' command",
                ));
            }

            let idx_str = match std::str::from_utf8(cmd[1]) {
                Ok(s) => s,
                Err(_) => return Some(resp_error("ERR invalid DB index")),
            };

            let idx: usize = match idx_str.parse() {
                Ok(s) => s,
                Err(_) => return Some(resp_error("ERR invalid DB index")),
            };

            if idx >= ctx.config.databases {
                return Some(resp_error("ERR DB index is out of range"));
            }

            // Before the first write that could be made there.
            ctx.repl_state.note_database_selected(idx);
            *ctx.selected_db = idx;
            Some(resp_simple("OK"))
        }

        "DEBUG" => crate::commands::debug_cmd::handle(ctx),

        _ => None, // ← tells the router this module doesn't handle it
    }
}
