//! Asking GitHub, once at startup, whether there is a newer KiviDB release.
//!
//! One HTTPS request, made from a thread of its own, for
//! `https://github.com/kividbio/kividb/releases/latest`. GitHub answers it
//! with a redirect to the newest stable release's page,
//! `.../releases/tag/v1.1.0`, so the tag is read from the `Location` header:
//! no API, no token, no rate limit, and no body to parse. If that release is
//! newer than this build, a notice is the first thing the server prints,
//! above the banner.
//!
//! Startup waits for the reply at most `BANNER_WAIT`, and a slower reply is
//! dropped, so a slow or absent network delays startup by that much at worst
//! and never touches a client. Nothing about the machine, its configuration
//! or its data is sent; the request carries only a `User-Agent` naming the
//! running version. Any failure (no network, DNS, TLS, a timeout, an
//! unexpected reply) is logged at debug level and otherwise ignored.
//!
//! The tag comes from the network and is printed to the operator's terminal
//! or log, so only a version-shaped tag is accepted: digits and dots, an
//! optional leading `v` and an optional `-suffix` of letters and digits.
//! Nothing else from the reply is shown.
//!
//! Turned off by `update-check no` in the config file, `--update-check no`,
//! or `KIVIDB_NO_UPDATE_CHECK=1`; and never made by a debug build, or when
//! `CI` is set in the environment. docs/UPDATE_CHECK.md has the details.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

/// Where the check is made unless `update-check-url` says otherwise.
pub const DEFAULT_URL: &str = "https://github.com/kividbio/kividb/releases/latest";

/// For each connection attempt, and each read and write after it.
const TIMEOUT: Duration = Duration::from_secs(4);
/// How long startup waits for the reply, at most, so the notice is the first
/// thing printed. The only time the check can hold up startup.
const BANNER_WAIT: Duration = Duration::from_millis(1500);
/// Only the head of the reply is wanted; a redirect has no body worth the name.
const MAX_REPLY: usize = 64 * 1024;
/// Longer than any real version tag.
const MAX_TAG: usize = 40;

/// The newest release, as the redirect named it.
#[derive(Debug, PartialEq)]
pub struct Release {
    /// The tag, e.g. `v1.1.0`.
    pub tag: String,
    /// Its page, where the downloads are.
    pub url: String,
}

/// Whether the check is to be made at all.
pub fn enabled(config: &crate::config::Config) -> bool {
    if cfg!(debug_assertions) || !config.update_check {
        return false;
    }
    let set = |name: &str| {
        std::env::var(name)
            .is_ok_and(|v| !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false"))
    };
    !set("KIVIDB_NO_UPDATE_CHECK") && !set("CI")
}

/// A check under way: started early, shown by the banner.
pub struct Pending {
    reply: mpsc::Receiver<Result<Option<Release>, String>>,
    deadline: Instant,
}

/// Start the check on a thread of its own. `None` if it is turned off.
pub fn start(config: &crate::config::Config) -> Option<Pending> {
    if !enabled(config) {
        return None;
    }
    let url = config.update_check_url.clone();
    let (tx, reply) = mpsc::channel();
    std::thread::Builder::new()
        .name("update-check".into())
        .spawn(move || {
            let _ = tx.send(check(&url));
        })
        .ok()?;
    Some(Pending {
        reply,
        deadline: Instant::now() + BANNER_WAIT,
    })
}

/// Print what the check found, before the banner. Waits until `BANNER_WAIT`
/// after the check started; a reply later than that is dropped rather than
/// printed somewhere among the log lines.
pub fn show(pending: Pending) {
    let wait = pending.deadline.saturating_duration_since(Instant::now());
    let error = match pending.reply.recv_timeout(wait) {
        Ok(Ok(release)) => {
            let lines = release
                .map(|r| render(env!("CARGO_PKG_VERSION"), &r))
                .unwrap_or_default();
            if !lines.is_empty() {
                for line in lines {
                    println!("{line}");
                }
                println!();
            }
            return;
        }
        Ok(Err(e)) => e,
        Err(_) => format!("no reply within {} ms", BANNER_WAIT.as_millis()),
    };
    log_debug!("update check: {error}");
}

/// The build's variant, as the release names it.
pub fn variant() -> &'static str {
    match (cfg!(feature = "lua"), cfg!(feature = "tls")) {
        (true, true) => "full",
        (true, false) => "lua",
        (false, true) => "tls",
        (false, false) => "default",
    }
}

/// `Ok(None)`: the repository has no release yet.
pub fn check(url: &str) -> Result<Option<Release>, String> {
    let (host, path) = url
        .strip_prefix("https://")
        .map(|rest| {
            rest.split_once('/')
                .map_or((rest, "/".to_string()), |(h, p)| (h, format!("/{p}")))
        })
        .ok_or_else(|| format!("'{url}' is not an https:// URL"))?;
    let (host_name, port) = match host.rsplit_once(':') {
        Some((name, port)) => (
            name,
            port.parse::<u16>()
                .map_err(|_| format!("bad port in '{url}'"))?,
        ),
        None => (host, 443),
    };
    let head = https_head(host_name, port, &path)?;
    release_from(&head)
}

/// The status line and headers of the reply to a `GET`.
fn https_head(host: &str, port: u16, path: &str) -> Result<String, String> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| format!("bad host '{host}': {e}"))?;
    let connection = rustls::ClientConnection::new(Arc::new(config), server_name)
        .map_err(|e| format!("TLS: {e}"))?;

    let addrs = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolving {host}: {e}"))?;
    let mut last = format!("no address for {host}");
    let mut socket = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, TIMEOUT) {
            Ok(s) => {
                socket = Some(s);
                break;
            }
            Err(e) => last = format!("connecting to {addr}: {e}"),
        }
    }
    let socket = socket.ok_or(last)?;
    socket
        .set_read_timeout(Some(TIMEOUT))
        .map_err(|e| e.to_string())?;
    socket
        .set_write_timeout(Some(TIMEOUT))
        .map_err(|e| e.to_string())?;

    let mut tls = rustls::StreamOwned::new(connection, socket);
    let request = format!(
        "GET {path} HTTP/1.0\r\nHost: {host}\r\nUser-Agent: kividb/{}\r\nConnection: close\r\n\r\n",
        env!("CARGO_PKG_VERSION")
    );
    tls.write_all(request.as_bytes())
        .map_err(|e| format!("sending: {e}"))?;

    // Read until the end of the headers; the body is not wanted.
    let mut reply = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        if let Some(end) = reply.windows(4).position(|w| w == b"\r\n\r\n") {
            return Ok(String::from_utf8_lossy(&reply[..end]).into_owned());
        }
        match tls.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                reply.extend_from_slice(&chunk[..n]);
                if reply.len() > MAX_REPLY {
                    return Err("reply too large".into());
                }
            }
            // A server that closes without TLS's close_notify.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("reading: {e}")),
        }
    }
    Err("no HTTP header in the reply".into())
}

/// Read the release out of the reply's head: a redirect whose `Location`
/// ends in `/releases/tag/<tag>`. A redirect anywhere else (GitHub sends
/// `/releases` when there is no release) means there is none.
pub fn release_from(head: &str) -> Result<Option<Release>, String> {
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or("no HTTP status in the reply")?;
    if !(300..400).contains(&status) {
        return Err(format!("HTTP {status}, not a redirect"));
    }
    let location = head
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("location"))
        .map(|(_, value)| value.trim())
        .ok_or("a redirect without a Location")?;
    let Some((page, tag)) = location.rsplit_once("/releases/tag/") else {
        return Ok(None);
    };
    if !is_version_tag(tag) {
        return Err(format!("'{}' is not a version tag", tag.escape_debug()));
    }
    if !page.starts_with("https://") || page.chars().any(|c| c.is_control() || c == ' ') {
        return Err("an unexpected Location".into());
    }
    Ok(Some(Release {
        tag: tag.to_string(),
        url: location.to_string(),
    }))
}

/// `v1.2.3`, `1.2`, `v1.2.3-rc1`: nothing that could carry anything else.
fn is_version_tag(tag: &str) -> bool {
    let version = tag.strip_prefix('v').unwrap_or(tag);
    let (core, pre) = match version.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (version, None),
    };
    tag.len() <= MAX_TAG
        && core
            .split('.')
            .all(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        && pre.is_none_or(|p| {
            !p.is_empty() && p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.')
        })
}

/// A version as numbers, and whether it is a pre-release (`1.0.5-rc1`).
fn version_key(version: &str) -> Option<(Vec<u64>, bool, String)> {
    let version = version.trim().trim_start_matches('v');
    let (core, pre) = match version.split_once('-') {
        Some((core, pre)) => (core, pre.to_string()),
        None => (version, String::new()),
    };
    let numbers = core
        .split('.')
        .map(|n| n.parse::<u64>().ok())
        .collect::<Option<Vec<_>>>()?;
    Some((numbers, !pre.is_empty(), pre))
}

/// Whether `latest` is a newer release than `running`. A release is newer
/// than its own release candidates; two release candidates of one version
/// are compared by their suffix. Anything that cannot be read as a version
/// is not newer.
pub fn is_newer(latest: &str, running: &str) -> bool {
    let (Some(latest), Some(running)) = (version_key(latest), version_key(running)) else {
        return false;
    };
    let width = latest.0.len().max(running.0.len());
    let pad = |mut v: Vec<u64>| {
        v.resize(width, 0);
        v
    };
    match pad(latest.0).cmp(&pad(running.0)) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => match (latest.1, running.1) {
            (false, true) => true,
            (true, true) => latest.2 > running.2,
            _ => false,
        },
    }
}

/// What is printed, line by line; nothing if this build is up to date.
pub fn render(running: &str, release: &Release) -> Vec<String> {
    if !is_newer(&release.tag, running) {
        return Vec::new();
    }
    let latest = release.tag.trim_start_matches('v');
    let body = [
        format!("A new KiviDB release is available: {latest} (you are running {running})."),
        format!("Download it from {}", release.url),
    ];
    let width = body.iter().map(|l| l.chars().count()).max().unwrap_or(0);
    let mut out = vec![String::new(), format!("  +{}+", "-".repeat(width + 2))];
    for line in &body {
        let pad = width - line.chars().count();
        out.push(format!("  | {line}{} |", " ".repeat(pad)));
    }
    out.push(format!("  +{}+", "-".repeat(width + 2)));
    out.push(String::new());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const REDIRECT: &str = "HTTP/1.1 302 Found\r\nContent-Length: 0\r\n\
        location: https://github.com/kividbio/kividb/releases/tag/v1.1.0";

    #[test]
    fn versions_compare_as_releases_do() {
        assert!(is_newer("1.0.6", "1.0.5"));
        assert!(is_newer("1.1.0", "1.0.9"));
        assert!(is_newer("1.0.10", "1.0.9"));
        assert!(is_newer("v2.0.0", "1.9.9"));
        assert!(is_newer("1.0.5", "1.0.5-rc1"));
        assert!(is_newer("1.0.5-rc2", "1.0.5-rc1"));
        assert!(!is_newer("1.0.5", "1.0.5"));
        assert!(!is_newer("1.0.4", "1.0.5"));
        assert!(!is_newer("1.0.5-rc1", "1.0.5"));
        assert!(!is_newer("nonsense", "1.0.5"));
        assert!(!is_newer("1.0.6", "nonsense"));
    }

    #[test]
    fn the_release_is_read_from_the_redirect() {
        let release = release_from(REDIRECT).unwrap().unwrap();
        assert_eq!(release.tag, "v1.1.0");
        assert_eq!(
            release.url,
            "https://github.com/kividbio/kividb/releases/tag/v1.1.0"
        );
        // No release yet: GitHub sends the releases list instead.
        let none = "HTTP/1.1 302 Found\r\nLocation: https://github.com/kividbio/kividb/releases";
        assert_eq!(release_from(none).unwrap(), None);
        assert!(release_from("HTTP/1.1 404 Not Found").is_err());
        assert!(release_from("HTTP/1.1 200 OK\r\nLocation: x").is_err());
        assert!(release_from("HTTP/1.1 302 Found").is_err());
    }

    #[test]
    fn only_a_version_shaped_tag_is_accepted() {
        for good in ["v1.1.0", "1.1.0", "v2.0", "v1.1.0-rc1", "v1.1.0-beta.2"] {
            assert!(is_version_tag(good), "{good}");
        }
        for bad in [
            "",
            "v",
            "latest",
            "v1..0",
            "v1.1.0-",
            "v1.1.0\u{1b}[2J",
            "v1.1.0 hello",
            "v1.1.0-rc1<script>",
            "v1.1.0\u{202e}",
        ] {
            assert!(!is_version_tag(bad), "{bad:?}");
        }
        let head =
            "HTTP/1.1 302 Found\r\nLocation: https://github.com/x/y/releases/tag/v1.1.0\u{1b}[31m";
        assert!(release_from(head).is_err());
    }

    #[test]
    fn a_newer_release_is_announced_and_a_current_one_is_not() {
        let release = release_from(REDIRECT).unwrap().unwrap();
        let shown = render("1.0.6", &release).join("\n");
        assert!(
            shown.contains("A new KiviDB release is available: 1.1.0 (you are running 1.0.6)."),
            "{shown}"
        );
        assert!(
            shown.contains(
                "Download it from https://github.com/kividbio/kividb/releases/tag/v1.1.0"
            ),
            "{shown}"
        );
        assert!(render("1.1.0", &release).is_empty());
        assert!(!render("1.1.0-rc1", &release).is_empty());
        for line in render("1.0.6", &release) {
            assert!(!line.chars().any(|c| c.is_control()), "{line:?}");
        }
    }
}
