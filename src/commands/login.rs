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
    /// Logs the connection in as `username` with `password`, or writes the
    /// failure to the ACL log as Redis's `ACLAuthenticateUser` does: reason
    /// `auth`, the user named, and the command as the client sent it
    /// (`command`, `AUTH` or `HELLO`) as the object.
    ///
    /// The password is any bytes, as in Redis: a `nopass` user takes any,
    /// and one set by its hash (`#<sha256>`) is checked against the bytes
    /// sent. User names are text here: one that is not UTF-8 is no user's.
    /// (It was read as `default`, and such a password as an empty one.)
    fn log_in(&mut self, command: &[u8], username: &[u8], password: &[u8]) -> bool {
        let user = std::str::from_utf8(username)
            .ok()
            .and_then(|username| crate::acl::authenticate(self.acl_store, username, password));
        match user {
            Some(user) => {
                self.become_user(&user);
                true
            }
            None => {
                self.acl_log.record(
                    "auth",
                    &String::from_utf8_lossy(command),
                    &String::from_utf8_lossy(username),
                    &crate::acl::log_client_info(
                        self.client_registry,
                        self.client_id,
                        self.peer_addr,
                    ),
                );
                false
            }
        }
    }

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
        // Only the two forms, as in Redis.
        if cmd.len() > 3 {
            return b"-ERR syntax error\r\n".to_vec();
        }
        // `AUTH password` is the legacy form: the user is `default`.
        let (username, password): (&[u8], &[u8]) = if cmd.len() == 3 {
            (cmd[1], cmd[2])
        } else {
            (b"default", cmd[1])
        };
        if self.log_in(cmd[0], username, password) {
            // Simple string, not a bulk string: real Redis replies `+OK`
            // here, and clients that text-match a leading `+OK` on
            // control connections read `$2\r\nOK\r\n` as a failed AUTH.
            resp_simple("OK")
        } else {
            WRONGPASS.to_vec()
        }
    }

    /// `HELLO [protover [AUTH username password] [SETNAME clientname]]`, as
    /// Redis's `helloCommand`: the options in any order, each checked before
    /// anything changes; then the AUTH; then the name and the protocol
    /// (`resp_version`, and `client_registry.resp`), which is echoed in the
    /// `proto` field.
    pub fn hello(
        &mut self,
        cmd: &[&[u8]],
        resp_version: &mut u8,
        repl_state: &crate::replication::ReplState,
    ) -> Vec<u8> {
        let mut protocol: Option<u8> = None;
        let mut next = 1usize;
        if let Some(ver) = cmd.get(1) {
            next = 2;
            let Some(ver) = strict_integer(ver) else {
                return b"-ERR Protocol version is not an integer or out of range\r\n".to_vec();
            };
            if !(2..=3).contains(&ver) {
                return b"-NOPROTO unsupported protocol version\r\n".to_vec();
            }
            protocol = Some(ver as u8);
        }

        let mut credentials: Option<(&[u8], &[u8])> = None;
        let mut name: Option<&[u8]> = None;
        let mut j = next;
        while j < cmd.len() {
            let more = cmd.len() - 1 - j;
            let option = cmd[j];
            if option.eq_ignore_ascii_case(b"AUTH") && more >= 2 {
                credentials = Some((cmd[j + 1], cmd[j + 2]));
                j += 3;
            } else if option.eq_ignore_ascii_case(b"SETNAME") && more >= 1 {
                let wanted = cmd[j + 1];
                // As CLIENT SETNAME: it is printed in `CLIENT LIST`, one
                // client to a line and one field to a word. Empty removes it.
                if !wanted.is_empty() && !crate::commands::server::is_printable_word(wanted) {
                    return b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n"
                        .to_vec();
                }
                name = Some(wanted);
                j += 2;
            } else {
                // Line breaks cannot be part of an error reply.
                let shown = String::from_utf8_lossy(option).replace(['\r', '\n'], " ");
                return crate::resp::write::resp_error(&format!(
                    "ERR Syntax error in HELLO option '{shown}'"
                ));
            }
        }

        if let Some((username, password)) = credentials
            && !self.log_in(cmd[0], username, password)
        {
            return WRONGPASS.to_vec();
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

        if let Some(name) = name {
            let name = String::from_utf8_lossy(name).into_owned();
            self.client_registry
                .update(self.client_id, |info| info.name = name);
        }
        if let Some(protocol) = protocol {
            *resp_version = protocol;
            self.client_registry
                .update(self.client_id, |info| info.resp = protocol);
        }
        let proto_reply = *resp_version as i64;

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

/// An integer as Redis's `string2ll` reads one: digits, a `-` before them
/// at most, no `+`, no leading zero (but `0` itself), no space.
fn strict_integer(raw: &[u8]) -> Option<i64> {
    let digits = raw.strip_prefix(b"-").unwrap_or(raw);
    let well_formed = match digits {
        [] => false,
        [b'0'] => raw.len() == 1,
        [first, rest @ ..] => (b'1'..=b'9').contains(first) && rest.iter().all(u8::is_ascii_digit),
    };
    if !well_formed {
        return None;
    }
    std::str::from_utf8(raw).ok()?.parse().ok()
}
