// AUTH and HELLO: how a connection becomes somebody, and which protocol it
// speaks.
//
// Both change the connection's own state, which lives in the connection loop
// (`server.rs`, `server_uring.rs`), so neither goes through `execute_command`.
// They are here, once, because they run in two places in each loop: when they
// are sent, and from a transaction's queue when its `EXEC` runs, as in Redis
// (#550).

use crate::resp::write::{resp_array, resp_bulk_str, resp_integer, resp_map, resp_simple};

/// The connection that runs `AUTH` or `HELLO`: what they read, and the
/// state of it they change.
pub struct Login<'a> {
    pub acl_store: &'a crate::acl::AclStore,
    pub acl_log: &'a crate::acl::SharedAclLog,
    pub client_registry: &'a crate::client_registry::ClientRegistry,
    pub client_id: u64,
    pub peer_addr: &'a str,
    /// The user the connection is, if it is anybody yet.
    pub user: &'a mut Option<String>,
    /// See `acl::track_restriction_counters`.
    pub key_restricted: &'a mut bool,
    pub cmd_restricted: &'a mut bool,
}

const WRONGPASS: &[u8] = b"-WRONGPASS invalid username-password pair or user is disabled.\r\n";

impl Login<'_> {
    /// The connection is now `user`.
    fn become_user(&mut self, user: &crate::acl::AclUser) {
        // The restricted-client counters are adjusted before the user is
        // replaced, so that a connection that changes user is counted right.
        crate::acl::track_restriction_counters(user, self.key_restricted, self.cmd_restricted);
        *self.user = Some(user.name.clone());
        self.client_registry
            .update(self.client_id, |info| info.user = user.name.clone());
    }

    /// `AUTH [username] password`.
    ///
    /// The acl_store is checked in every case. The default user's password
    /// is stored there (seeded from --requirepass at startup); nopass users
    /// authenticate with any password.
    pub fn auth(&mut self, cmd: &[&[u8]]) -> Vec<u8> {
        if cmd.len() < 2 {
            return b"-ERR wrong number of arguments for 'auth' command\r\n".to_vec();
        }
        if cmd.len() == 2 && crate::acl::default_user_is_open(self.acl_store) {
            // As Redis: how a client learns that the password it was given
            // is not in use. It was answered OK (#135).
            return crate::acl::AUTH_WITHOUT_PASSWORD.as_bytes().to_vec();
        }
        let (provided_username, provided_password) = if cmd.len() >= 3 {
            // AUTH username password
            (
                std::str::from_utf8(cmd[1]).unwrap_or("").to_string(),
                std::str::from_utf8(cmd[2]).unwrap_or("").to_string(),
            )
        } else {
            // AUTH password — legacy form, authenticate as "default"
            (
                "default".to_string(),
                std::str::from_utf8(cmd[1]).unwrap_or("").to_string(),
            )
        };

        match crate::acl::authenticate(self.acl_store, &provided_username, &provided_password) {
            Some(user) => {
                self.become_user(&user);
                // Simple string, not a bulk string: real Redis replies `+OK`
                // here, and clients that text-match a leading `+OK` on
                // control connections read `$2\r\nOK\r\n` as a failed AUTH.
                resp_simple("OK")
            }
            None => {
                let entry = crate::acl::AclLogEntry {
                    id: self.acl_log.next_id(),
                    reason: "auth".into(),
                    context: "toplevel".into(),
                    object: provided_username.clone(),
                    username: provided_username,
                    age_seconds: 0.0,
                    client_info: self.peer_addr.to_string(),
                    timestamp_created: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                    count: 1,
                };
                self.acl_log.push(entry);
                WRONGPASS.to_vec()
            }
        }
    }

    /// `HELLO [protover [AUTH username password] [SETNAME clientname]]`.
    ///
    /// Protover sets the connection's RESP (`resp_version`, and
    /// `client_registry.resp`), and is echoed in the `proto` field.
    pub fn hello(
        &mut self,
        cmd: &[&[u8]],
        resp_version: &mut u8,
        repl_state: &crate::replication::ReplState,
    ) -> Vec<u8> {
        let mut i = 1usize; // index into cmd, past "HELLO"

        let mut proto_reply: i64 = *resp_version as i64;

        // Optional protocol version
        if let Some(ver_bytes) = cmd.get(i) {
            let ver = std::str::from_utf8(ver_bytes).unwrap_or("2");
            // protover 0 is a special case meaning "RESET to defaults"
            if ver != "0" && ver != "1" && ver != "2" && ver != "3" {
                return b"-ERR Protocol version is not an integer or out of range\r\n".to_vec();
            }
            // "0", "1", "2" — Redis resets/alternate protos to RESP2 wire.
            proto_reply = if ver == "3" { 3 } else { 2 };
            i += 1;
        }
        // The protocol changes only once the rest of the command has been
        // accepted, as in Redis: a HELLO 3 whose AUTH fails leaves the
        // connection on the protocol it had.

        // Optional AUTH username password
        if cmd
            .get(i)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.eq_ignore_ascii_case("AUTH"))
            .unwrap_or(false)
        {
            i += 1; // consume "AUTH"
            let (provided_username, provided_password) = match (cmd.get(i), cmd.get(i + 1)) {
                (Some(u), Some(p)) => {
                    i += 2;
                    (
                        std::str::from_utf8(u).unwrap_or("default"),
                        std::str::from_utf8(p).unwrap_or(""),
                    )
                }
                (Some(p), None) => {
                    i += 1;
                    ("default", std::str::from_utf8(p).unwrap_or(""))
                }
                _ => return b"-ERR Syntax error in HELLO option 'auth'\r\n".to_vec(),
            };

            // The user named, and no other. (This used to accept `default`
            // as a name for *any* user whose password matched.)
            match crate::acl::authenticate(self.acl_store, provided_username, provided_password) {
                Some(user) => self.become_user(&user),
                None => return WRONGPASS.to_vec(),
            }
        }

        // HELLO is let through the auth gate so that it can carry AUTH;
        // without it, it needs a connection that is already authenticated.
        if self.user.is_none() {
            return b"-NOAUTH HELLO must be called with the client already \
                     authenticated, otherwise the HELLO <proto> AUTH <user> <pass> \
                     option can be used to authenticate the client and select the \
                     RESP protocol version at the same time\r\n"
                .to_vec();
        }

        // Optional SETNAME clientname
        if cmd
            .get(i)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.eq_ignore_ascii_case("SETNAME"))
            .unwrap_or(false)
        {
            i += 1;
            if let Some(name_bytes) = cmd.get(i) {
                let name = String::from_utf8_lossy(name_bytes).to_string();
                self.client_registry
                    .update(self.client_id, |info| info.name = name);
            }
        }

        *resp_version = proto_reply as u8;
        self.client_registry
            .update(self.client_id, |info| info.resp = proto_reply as u8);

        // Build the response — RESP3 clients (e.g. modern redis-py, whose
        // default changed to protocol 3) expect the HELLO reply itself encoded
        // as a `%` map once RESP3 is negotiated; RESP2 stays a flat `*` array
        // of pairs.
        let role = repl_state.role();
        // The Redis release this server follows, as `INFO`'s `redis_version`
        // says: clients read these two to know what they may use (#451).
        // KiviDB's own release follows them, as Dragonfly's does, and is
        // `INFO`'s `kividb_version` too.
        let kv = vec![
            resp_bulk_str("server"),
            resp_bulk_str("redis"),
            resp_bulk_str("version"),
            resp_bulk_str(crate::commands::server::REDIS_VERSION),
            resp_bulk_str("kividb_version"),
            resp_bulk_str(env!("CARGO_PKG_VERSION")),
            resp_bulk_str("proto"),
            resp_integer(proto_reply),
            resp_bulk_str("id"),
            resp_integer(self.client_id as i64),
            resp_bulk_str("mode"),
            resp_bulk_str("standalone"),
            resp_bulk_str("role"),
            resp_bulk_str(&role),
            resp_bulk_str("modules"),
            crate::commands::server::module_list_reply(proto_reply as u8),
        ];
        if proto_reply == 3 {
            resp_map(kv)
        } else {
            resp_array(kv)
        }
    }
}
