//! What a KeyDB master sends its replicas that Redis does not.
//!
//! - `EXPIREMEMBER`, `EXPIREMEMBERAT` and `PEXPIREMEMBERAT`: a TTL for one
//!   member of a hash, set or sorted set. A hash field's is applied as the
//!   field's TTL (`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`). A set's
//!   or sorted set's member has no TTL here: the link is closed rather than
//!   carry on without it.
//! - `RREPLAY <uuid> <commands> <db> <stamp>`: every write of a master in
//!   active replication (`active-replica yes`), wrapped, and wrapped again
//!   for each master of the mesh it went through. It is not followed: the
//!   link is closed, saying so. Migrate from such a master by its RDB file,
//!   or with `MIGRATE`.
//!
//! Before, all of them were unknown commands, refused one by one, and the
//! link stayed up while those writes were lost.

/// What a KeyDB master sent that this replica can't follow: the link ends,
/// loudly, rather than go on without it.
#[derive(Debug)]
pub(crate) struct Unfollowable(pub String);

impl std::fmt::Display for Unfollowable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unfollowable {}

/// Why a replica of a KeyDB master in active replication stops.
pub(crate) const RREPLAY_REFUSAL: &str = "KeyDB active replication (RREPLAY) is not supported: \
     migrate from that server by its RDB file or with MIGRATE";

/// Whether a command is one of KeyDB's that [`translate`] handles. Every
/// other command is applied as it is, with nothing more done for it.
pub(crate) fn is_keydb_command(cmd: &[Vec<u8>]) -> bool {
    [
        &b"RREPLAY"[..],
        b"EXPIREMEMBER",
        b"EXPIREMEMBERAT",
        b"PEXPIREMEMBERAT",
    ]
    .iter()
    .any(|name| named(cmd, name))
}

fn named(cmd: &[Vec<u8>], name: &[u8]) -> bool {
    cmd.first().is_some_and(|c| c.eq_ignore_ascii_case(name))
}

/// One of KeyDB's commands, as it is applied here.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Translated {
    /// Applied as it is (a form KeyDB refuses itself, and never sends).
    AsIs(Vec<Vec<u8>>),
    /// A member TTL, as the hash-field command that sets it here. A key that
    /// is not a hash (`WRONGTYPE`) ends the link: `what` says why.
    MemberTtl { command: Vec<Vec<u8>>, what: String },
}

/// KeyDB's member TTL commands as the hash-field ones. `RREPLAY` is refused.
///
/// - `EXPIREMEMBER key member n [s|ms]`: in `n` seconds, or milliseconds.
/// - `EXPIREMEMBERAT key member t`: at Unix time `t`, in seconds.
/// - `PEXPIREMEMBERAT key member t`: at Unix time `t`, in milliseconds.
pub(crate) fn translate(cmd: Vec<Vec<u8>>) -> Result<Translated, Unfollowable> {
    if named(&cmd, b"RREPLAY") {
        return Err(Unfollowable(RREPLAY_REFUSAL.into()));
    }
    let name = cmd.first().map(|c| c.to_ascii_uppercase());
    let unit = cmd.get(4).map(|u| u.to_ascii_lowercase());
    let verb: &[u8] = match (name.as_deref(), cmd.len(), unit.as_deref()) {
        (Some(b"EXPIREMEMBER"), 4, _) | (Some(b"EXPIREMEMBER"), 5, Some(b"s")) => b"HEXPIRE",
        (Some(b"EXPIREMEMBER"), 5, Some(b"ms")) => b"HPEXPIRE",
        (Some(b"EXPIREMEMBERAT"), 4, _) => b"HEXPIREAT",
        (Some(b"PEXPIREMEMBERAT"), 4, _) => b"HPEXPIREAT",
        _ => return Ok(Translated::AsIs(cmd)),
    };
    let command = vec![
        verb.to_vec(),
        cmd[1].clone(),
        cmd[3].clone(),
        b"FIELDS".to_vec(),
        b"1".to_vec(),
        cmd[2].clone(),
    ];
    let what = format!(
        "KeyDB's {} gave the member {} of {} its own TTL, and {} is not a hash: KiviDB keeps \
         no TTLs for set or sorted-set members, and following on without it would keep the \
         member after KeyDB deletes it",
        String::from_utf8_lossy(&cmd[0]),
        String::from_utf8_lossy(&cmd[2]),
        String::from_utf8_lossy(&cmd[1]),
        String::from_utf8_lossy(&cmd[1]),
    );
    Ok(Translated::MemberTtl { command, what })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    #[test]
    fn rreplay_is_refused_by_name() {
        let rreplay = args(&[
            "RREPLAY",
            "4f1c2b52-0d0f-4a5e-9a3c-2b1d5f6e7a8b",
            "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
            "0",
            "5",
        ]);
        assert!(is_keydb_command(&rreplay));
        let refused = translate(rreplay).unwrap_err().0;
        assert!(refused.contains("RREPLAY) is not supported"), "{refused}");
    }

    #[test]
    fn keydb_member_ttls_are_hash_field_ttls() {
        let one = |cmd: &[&str]| match translate(args(cmd)).unwrap() {
            Translated::MemberTtl { command, .. } => command,
            Translated::AsIs(cmd) => panic!("not rewritten: {cmd:?}"),
        };
        assert_eq!(
            one(&["EXPIREMEMBER", "h", "f", "10"]),
            args(&["HEXPIRE", "h", "10", "FIELDS", "1", "f"])
        );
        assert_eq!(
            one(&["expiremember", "h", "f", "1500", "ms"]),
            args(&["HPEXPIRE", "h", "1500", "FIELDS", "1", "f"])
        );
        assert_eq!(
            one(&["EXPIREMEMBERAT", "h", "f", "4102444800"]),
            args(&["HEXPIREAT", "h", "4102444800", "FIELDS", "1", "f"])
        );
        assert_eq!(
            one(&["PEXPIREMEMBERAT", "h", "f", "4102444800000"]),
            args(&["HPEXPIREAT", "h", "4102444800000", "FIELDS", "1", "f"])
        );
        // A unit KeyDB itself refuses: left as it is.
        assert_eq!(
            translate(args(&["EXPIREMEMBER", "h", "f", "1", "h"])).unwrap(),
            Translated::AsIs(args(&["EXPIREMEMBER", "h", "f", "1", "h"]))
        );
        assert!(!is_keydb_command(&args(&["HSET", "h", "f", "v"])));
    }
}
