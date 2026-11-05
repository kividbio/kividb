// src/commands/acl.rs
//
// ACL command handler. The users, their rules and how they are enforced are
// in `crate::acl`; this is the command surface over them.

use crate::acl::{AclLogEntry, AclUser};
use crate::commands::ctx::Ctx;
use crate::resp::write::*;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if name != "ACL" {
        return None;
    }

    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'acl' command",
        ));
    }

    let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();

    match sub.as_str() {
        // ── ACL WHOAMI ────────────────────────────────────────────────────────
        // Returns the username of the current connection.
        // We always return "default" — we only have one user.
        "WHOAMI" => Some(resp_bulk_str(ctx.authenticated_user)),

        // ── ACL LIST ──────────────────────────────────────────────────────────
        // Returns all users in ACL file format.
        "LIST" => {
            let store = ctx.acl_store.read();
            let mut users: Vec<String> = store.values().map(|u| u.to_acl_string()).collect();
            users.sort(); // deterministic output
            Some(resp_array(users.iter().map(|s| resp_bulk_str(s)).collect()))
        }

        // ── ACL USERS ─────────────────────────────────────────────────────────
        // Returns a list of all usernames.
        "USERS" => {
            let store = ctx.acl_store.read();
            let mut names: Vec<String> = store.keys().cloned().collect();
            names.sort();
            Some(resp_array(names.iter().map(|s| resp_bulk_str(s)).collect()))
        }

        // ── ACL CAT ───────────────────────────────────────────────────────────
        // ACL CAT           → list of all category names
        // ACL CAT <cat>     → list of commands in that category
        "CAT" => {
            if cmd.len() >= 3 {
                // Return commands in a specific category — delegate to server.rs helper
                let cat = String::from_utf8_lossy(cmd[2]).to_ascii_lowercase();
                match crate::commands::server::acl_cat_commands_opt(&cat) {
                    Some(cmds) => Some(resp_array(cmds.iter().map(|c| resp_bulk_str(c)).collect())),
                    None => Some(resp_error(&format!(
                        "ERR Unknown category '{}', try ACL CAT",
                        cat
                    ))),
                }
            } else {
                // Return all category names
                let cats = crate::commands::server::ACL_CATEGORIES;
                Some(resp_array(cats.iter().map(|c| resp_bulk_str(c)).collect()))
            }
        }

        // ── ACL GETUSER ───────────────────────────────────────────────────────
        // Returns the rules for a specific user as a map.
        "GETUSER" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'acl|getuser' command",
                ));
            }
            let username = match std::str::from_utf8(cmd[2]) {
                Ok(s) => s,
                Err(_) => return Some(resp_error("ERR invalid username")),
            };
            let store = ctx.acl_store.read();
            match store.get(username) {
                None => Some(resp_null_bulk()),
                Some(user) => {
                    // A flat array of key-value pairs (a map, in RESP2 terms).
                    // The rules are strings, as Redis 7 returns them — one
                    // string each for commands, keys and channels — which is
                    // what client libraries parse.
                    let mut flags = vec![if user.enabled { "on" } else { "off" }];
                    if user.nopass {
                        flags.push("nopass");
                    }
                    let passwords: Vec<Vec<u8>> =
                        user.passwords.iter().map(|h| resp_bulk_str(h)).collect();
                    let selectors: Vec<Vec<u8>> = user
                        .selectors
                        .iter()
                        .map(|s| {
                            let keys = s
                                .keys
                                .iter()
                                .map(|k| k.to_string())
                                .collect::<Vec<_>>()
                                .join(" ");
                            let channels = s
                                .channels
                                .iter()
                                .map(|c| format!("&{c}"))
                                .collect::<Vec<_>>()
                                .join(" ");
                            resp_array(vec![
                                resp_bulk_str("commands"),
                                resp_bulk_str(&s.commands),
                                resp_bulk_str("keys"),
                                resp_bulk_str(&keys),
                                resp_bulk_str("channels"),
                                resp_bulk_str(&channels),
                            ])
                        })
                        .collect();

                    Some(resp_array(vec![
                        resp_bulk_str("flags"),
                        resp_array(flags.iter().map(|f| resp_bulk_str(f)).collect()),
                        resp_bulk_str("passwords"),
                        resp_array(passwords),
                        resp_bulk_str("commands"),
                        resp_bulk_str(&user.commands),
                        resp_bulk_str("keys"),
                        resp_bulk_str(&user.keys_string()),
                        resp_bulk_str("channels"),
                        resp_bulk_str(&user.channels_string()),
                        resp_bulk_str("selectors"),
                        resp_array(selectors),
                    ]))
                }
            }
        }

        // ── ACL LOG ───────────────────────────────────────────────────────────
        // ACL LOG [count | RESET]
        "LOG" => {
            if cmd.len() >= 3 {
                let arg = String::from_utf8_lossy(cmd[2]).to_ascii_uppercase();
                if arg == "RESET" {
                    ctx.acl_log.reset();
                    return Some(resp_simple("OK"));
                }
                // ACL LOG <count>
                let count: usize = match std::str::from_utf8(cmd[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(n) => n,
                    None => return Some(resp_error("ERR value is not an integer or out of range")),
                };
                Some(format_acl_log(ctx.acl_log.get(count)))
            } else {
                // ACL LOG with no args — return all (up to max_len)
                Some(format_acl_log(ctx.acl_log.get(usize::MAX)))
            }
        }

        // ── ACL SETUSER ───────────────────────────────────────────────────────
        // ACL SETUSER username [rule ...]
        // Creates or modifies a user. Rules: on/off, nopass, >password, ~pattern, +cmd, -cmd
        "SETUSER" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'acl|setuser' command",
                ));
            }
            // What tracking clients are still to be told of (the writes before
            // this in a transaction) is told under the permissions they had,
            // before this changes them, as Redis does: a BCAST client is told
            // only of keys its user may read.
            crate::tracking::flush_pending(
                ctx.tracking_table,
                ctx.client_registry,
                Some(ctx.client_id),
            );
            let username = match std::str::from_utf8(cmd[2]) {
                Ok(s) => s.to_string(),
                Err(_) => return Some(resp_error("ERR invalid username")),
            };
            if username.is_empty() || username.contains([' ', '\0', '\t', '\n', '\r']) {
                return Some(resp_error(
                    "ERR Usernames can't contain spaces or null characters",
                ));
            }
            let mut args: Vec<&str> = Vec::with_capacity(cmd.len().saturating_sub(3));
            for rule_bytes in cmd.iter().skip(3) {
                match std::str::from_utf8(rule_bytes) {
                    Ok(s) => args.push(s),
                    Err(_) => return Some(resp_error("ERR invalid rule")),
                }
            }
            let rules = match crate::acl::merge_selector_args(args.into_iter()) {
                Ok(rules) => rules,
                Err(e) => return Some(resp_error(&format!("ERR {e}"))),
            };

            // The rules are applied to a copy, and the copy replaces the user
            // only if every one of them was accepted: a SETUSER that returns
            // an error has changed nothing.
            let mut store = ctx.acl_store.write();
            let mut user = store
                .get(&username)
                .cloned()
                .unwrap_or_else(|| AclUser::new(&username));
            for rule in &rules {
                if let Err(e) = crate::acl::apply_rule(&mut user, rule) {
                    return Some(resp_error(&format!(
                        "ERR Error in ACL SETUSER modifier '{rule}': {e}"
                    )));
                }
            }
            store.insert(username, user);
            drop(store);
            // Connections already open as this user pick the change up.
            crate::acl::acl_changed();

            // The ACL file is written by `ACL SAVE`, and only by it. Writing
            // it here made the command fail — after it had taken effect —
            // wherever the file is not writable (a read-only mount), and
            // overwrote a file that something else manages.
            Some(resp_simple("OK"))
        }

        // ── ACL DELUSER ───────────────────────────────────────────────────────
        // ACL DELUSER username [username ...]
        "DELUSER" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'acl|deluser' command",
                ));
            }
            // Checked before anything is deleted, so that an error means
            // nothing happened.
            if cmd.iter().skip(2).any(|name| *name == b"default") {
                return Some(resp_error("ERR The 'default' user cannot be removed"));
            }
            let mut deleted = 0i64;
            let mut store = ctx.acl_store.write();
            for name_bytes in cmd.iter().skip(2) {
                let name = match std::str::from_utf8(name_bytes) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                if store.remove(name).is_some() {
                    deleted += 1;
                }
            }
            drop(store);
            if deleted > 0 {
                // Connections logged in as a deleted user are closed.
                crate::acl::acl_changed();
            }
            Some(resp_integer(deleted))
        }

        // ── ACL GENPASS ───────────────────────────────────────────────────────
        // ACL GENPASS [bits]
        // Generates a cryptographically random hex string.
        "GENPASS" => {
            let bits: usize = if cmd.len() >= 3 {
                match std::str::from_utf8(cmd[2])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(n) if n > 0 && n <= 4096 => n.div_ceil(4) * 4,
                    _ => {
                        return Some(resp_error(
                            "ERR The optional argument must be between 1 and 4096, and divisible by 4",
                        ));
                    }
                }
            } else {
                256
            };
            // A hex digit per 4 bits, as Redis gives them: 1 to 4 bits are one
            // digit. Whole bytes were taken, none for under 8 bits, and the
            // password was empty (#140).
            let digits = bits.div_ceil(4);
            let random_bytes: Vec<u8> = (0..digits.div_ceil(2))
                .map(|_| rand::random::<u8>())
                .collect();
            let mut hex: String = random_bytes.iter().map(|b| format!("{:02x}", b)).collect();
            hex.truncate(digits);
            Some(resp_bulk_str(&hex))
        }

        // ── ACL DRYRUN ────────────────────────────────────────────────────────
        // ACL DRYRUN username command [arg ...]
        // Says whether the user could run the command — the command, its
        // keys and its channels — without running it.
        "DRYRUN" => {
            if cmd.len() < 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'acl|dryrun' command",
                ));
            }
            let username = match std::str::from_utf8(cmd[2]) {
                Ok(s) => s,
                Err(_) => return Some(resp_error("ERR invalid username")),
            };
            let target_cmd = String::from_utf8_lossy(cmd[3]).to_ascii_uppercase();
            let store = ctx.acl_store.read();
            let Some(user) = store.get(username) else {
                return Some(resp_error(&format!("ERR User '{username}' not found")));
            };
            if !crate::commands::server::is_registered_command(&target_cmd) {
                return Some(resp_error(&format!(
                    "ERR Command '{}' not found",
                    String::from_utf8_lossy(cmd[3])
                )));
            }
            match crate::acl::check_permission(user, &target_cmd, &cmd[3..]) {
                Ok(()) => Some(resp_simple("OK")),
                Err(denied) => {
                    let message = denied.message(username);
                    Some(resp_bulk_str(
                        message.strip_prefix("NOPERM ").unwrap_or(&message),
                    ))
                }
            }
        }

        // ── ACL SAVE / LOAD ───────────────────────────────────────────────────
        // SAVE writes the users in memory to the ACL file; LOAD makes the
        // file's users the users in memory — all of them, or, if the file
        // has an error in it, none.
        "SAVE" => {
            if ctx.config.aclfile.is_empty() {
                return Some(resp_error(
                    "ERR This KiviDB instance is not configured to use an ACL file. \
                    You may want to specify users via the ACL SETUSER command and \
                    make sure to also set an aclfile configuration directive.",
                ));
            }
            match crate::acl::save_acl_file(&ctx.config.aclfile, ctx.acl_store) {
                Ok(_) => Some(resp_simple("OK")),
                Err(e) => Some(resp_error(&format!("ERR saving ACL file: {e}"))),
            }
        }

        "LOAD" => {
            if ctx.config.aclfile.is_empty() {
                return Some(resp_error(
                    "ERR This KiviDB instance is not configured to use an ACL file",
                ));
            }
            match crate::acl::load_acl_file(
                &ctx.config.aclfile,
                ctx.acl_store,
                &ctx.config.requirepass,
            ) {
                Ok(_) => Some(resp_simple("OK")),
                Err(e) => Some(resp_error(&format!(
                    "ERR {e}. WARNING: ACL errors detected, no change to the previously \
                     active ACL rules was performed"
                ))),
            }
        }

        // Its error said to try it, and it was unknown (#134).
        "HELP" => Some(crate::commands::server::help(
            "ACL",
            &[
                (
                    "CAT [<category>]",
                    "List all commands that belong to <category>, or all command categories",
                ),
                (
                    "DELUSER <username> [<username> ...]",
                    "Delete a list of users.",
                ),
                (
                    "DRYRUN <username> <command> [<arg> ...]",
                    "Returns whether the user can execute the given command without executing the command.",
                ),
                ("GETUSER <username>", "Get the user's details."),
                (
                    "GENPASS [<bits>]",
                    "Generate a secure 256-bit user password. The optional `bits` argument can be used to specify a different size.",
                ),
                ("LIST", "Show users details in config file format."),
                ("LOAD", "Reload users from the ACL file."),
                ("LOG [<count> | RESET]", "Show the ACL log entries."),
                ("SAVE", "Save the current config to the ACL file."),
                (
                    "SETUSER <username> <attribute> [<attribute> ...]",
                    "Create or modify a user with the specified attributes.",
                ),
                ("USERS", "List all the registered usernames."),
                ("WHOAMI", "Return the current connection username."),
            ],
        )),

        _ => Some(resp_error(&format!(
            "ERR unknown subcommand '{}'. Try ACL HELP.",
            sub
        ))),
    }
}

// ── Format ACL LOG entries as RESP ────────────────────────────────────────────

fn format_acl_log(entries: Vec<AclLogEntry>) -> Vec<u8> {
    let items: Vec<Vec<u8>> = entries
        .into_iter()
        .map(|e| {
            resp_array(vec![
                resp_bulk_str("id"),
                resp_integer(e.id as i64),
                resp_bulk_str("timestamp-created"),
                resp_integer(e.timestamp_created as i64),
                resp_bulk_str("timestamp-last-updated"),
                resp_integer(e.timestamp_created as i64),
                resp_bulk_str("client-info"),
                resp_bulk_str(&e.client_info),
                resp_bulk_str("reason"),
                resp_bulk_str(&e.reason),
                resp_bulk_str("context"),
                resp_bulk_str(&e.context),
                resp_bulk_str("object"),
                resp_bulk_str(&e.object),
                resp_bulk_str("username"),
                resp_bulk_str(&e.username),
                resp_bulk_str("age-seconds"),
                resp_bulk_str(&format!("{:.3}", e.age_seconds)),
                resp_bulk_str("count"),
                resp_integer(e.count as i64),
            ])
        })
        .collect();
    resp_array(items)
}
