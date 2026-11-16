// src/protected_mode.rs
//
// `protected-mode`, as in Redis: while the `default` user has no password and
// no `bind` address was configured, a client that is not on this machine is
// told so and disconnected. Without it a server installed with no
// configuration, listening on every address, took commands from anyone who
// could reach it. A `bind` that was set is taken as meant, as in Redis
// before 7.

use std::sync::atomic::{AtomicBool, Ordering};

static ON: AtomicBool = AtomicBool::new(true);
/// Set when `bind` was configured: then protected mode does not apply.
static BIND_EXPLICIT: AtomicBool = AtomicBool::new(false);

pub fn set_bind_explicit(explicit: bool) {
    BIND_EXPLICIT.store(explicit, Ordering::Relaxed);
}

pub fn set(on: bool) {
    ON.store(on, Ordering::Relaxed);
}

pub fn is_on() -> bool {
    ON.load(Ordering::Relaxed)
}

const DENIED: &[u8] = b"-DENIED KiviDB is running in protected mode because protected mode is \
enabled and no password is set for the default user. In this mode connections are only \
accepted from the loopback interface. To accept connections from other machines, do one of \
these: 1) set a password for the default user (requirepass, or ACL SETUSER default \
>password); 2) send 'CONFIG SET protected-mode no' from the loopback interface; 3) set \
'protected-mode no' in the configuration file and restart; 4) start the server with \
'--protected-mode no'. Make sure the server cannot be reached from the internet if you \
choose 2, 3 or 4.\r\n";

/// The reply that turns a client away, or `None` if it may connect.
pub fn refusal(acl_store: &crate::acl::AclStore, peer_addr: &str) -> Option<&'static [u8]> {
    if !is_on() || BIND_EXPLICIT.load(Ordering::Relaxed) || is_local(peer_addr) {
        return None;
    }
    let open = acl_store
        .read()
        .get("default")
        .is_none_or(|user| user.nopass);
    open.then_some(DENIED)
}

/// Whether a client at `peer_addr` is on this machine.
fn is_local(peer_addr: &str) -> bool {
    match peer_addr.parse::<std::net::SocketAddr>() {
        Ok(addr) => addr.ip().is_loopback(),
        // Not an IP address (a Unix socket): on this machine.
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_clients_of_this_machine_are_local() {
        assert!(is_local("127.0.0.1:5000"));
        assert!(is_local("[::1]:5000"));
        assert!(is_local("/tmp/kividb.sock"));
        assert!(!is_local("10.0.0.7:5000"));
        assert!(!is_local("[2001:db8::1]:5000"));
    }
}
