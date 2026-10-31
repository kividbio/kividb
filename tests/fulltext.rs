// tests/fulltext.rs
//
// Integration tests for full-text tokenisation:
//   - Token match correctness (not substring)
//   - Inverted index lookup (correct candidates returned)
//   - Stopword filtering
//   - BM25 ranking (higher-frequency term ranks first)
//   - Multi-field indexing and field isolation
//   - Backfill on FT.CREATE over existing keys
//   - Incremental update on HSET
//   - Delete removes from inverted index
//   - Prefix queries
//   - AND / OR filter combinations via inverted index

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── Minimal RESP client ───────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..40)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    fn read_value(&mut self) -> RespValue {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => RespValue::SimpleStr(line[1..].to_string()),
            Some(b'-') => RespValue::Error(line[1..].to_string()),
            Some(b':') => RespValue::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len == -1 {
                    return RespValue::Nil;
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                RespValue::Bulk(data)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                let items = (0..count).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            _ => panic!("unexpected RESP line: {line:?}"),
        }
    }
}

// ── RespValue helpers ─────────────────────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) => s,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap_or(""),
            _ => "",
        }
    }

    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap_or("0").parse().unwrap_or(0),
            _ => 0,
        }
    }

    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => &[],
        }
    }

    /// For FT.SEARCH: extract matched key names from the result array.
    /// Format: [total, key1, [fields...], key2, [fields...], ...]
    fn ft_search_keys(&self) -> Vec<&str> {
        let arr = self.as_array();
        if arr.is_empty() {
            return vec![];
        }
        // arr[0] = total count; then alternating key, fields-array
        arr[1..]
            .chunks(2)
            .filter_map(|chunk| {
                if !chunk.is_empty() {
                    Some(chunk[0].as_str())
                } else {
                    None
                }
            })
            .collect()
    }

    fn ft_search_total(&self) -> i64 {
        self.as_array().first().map(|v| v.as_int()).unwrap_or(0)
    }
}

// ── Server harness ────────────────────────────────────────────────────────────

/// Whether nothing has `port`. It is asked with a socket that is bound and
/// never listens. One that listened, as this used to, could be given a
/// connection: a server another test is starting holds, between its fork and
/// its exec, a copy of every socket this process has open, and a client that
/// dialled the port in that moment was connected to the copy and then reset
/// ("Connection reset by peer", at the first command of a test).
fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    // As the server's own socket, so that a copy of this one, while it
    // lasts, does not keep the server from the port.
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

/// A port nothing is listening on — and not one the system hands out to
/// outgoing connections. A client that starts dialling before its server is
/// listening can otherwise be given, as its own end, the very port it is
/// dialling: it is then connected to itself and reads back what it writes.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(31);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn start_server(port: u16) {
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
}

/// Helper: create a TEXT-only index on the given prefix and field name.
fn create_text_index(c: &mut Conn, index: &str, prefix: &str, field: &str) {
    c.cmds(&[
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        field,
        "TEXT",
    ]);
}

// ── Token match correctness ───────────────────────────────────────────────────

/// The old substring match would return "trust" for @title:rust.
/// Token match must not.
#[test]
fn test_token_match_does_not_match_substring() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "trust issues"]);
    c.cmds(&["HSET", "doc:2", "title", "rust programming"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@title:rust", "LIMIT", "0", "10"]);
    let keys = r.ft_search_keys();

    assert!(
        keys.contains(&"doc:2"),
        "doc:2 ('rust programming') must match @title:rust"
    );
    assert!(
        !keys.contains(&"doc:1"),
        "doc:1 ('trust issues') must NOT match @title:rust — 'rust' is a substring of 'trust', not a token"
    );
}

/// Suffix substring must not match. "cat" should not match "concatenate".
#[test]
fn test_token_match_does_not_match_mid_word() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    c.cmds(&["HSET", "doc:1", "body", "concatenate strings"]);
    c.cmds(&["HSET", "doc:2", "body", "cat sat on mat"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@body:cat", "LIMIT", "0", "10"]);
    let keys = r.ft_search_keys();

    assert!(keys.contains(&"doc:2"), "doc:2 must match @body:cat");
    assert!(
        !keys.contains(&"doc:1"),
        "doc:1 must NOT match @body:cat — 'cat' is inside 'concatenate'"
    );
}

/// Exact whole-word match works.
#[test]
fn test_token_match_exact_word() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "rust is fast"]);
    c.cmds(&["HSET", "doc:2", "title", "python is slow"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@title:rust", "LIMIT", "0", "10"]);
    assert_eq!(r.ft_search_total(), 1);
    assert_eq!(r.ft_search_keys(), vec!["doc:1"]);
}

/// Case-insensitive matching — "Rust" and "RUST" both match @title:rust.
#[test]
fn test_token_match_case_insensitive() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "Rust Programming"]);
    c.cmds(&["HSET", "doc:2", "title", "RUST is great"]);
    c.cmds(&["HSET", "doc:3", "title", "python rules"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@title:rust", "LIMIT", "0", "10"]);
    let keys = r.ft_search_keys();

    assert!(keys.contains(&"doc:1"), "mixed-case 'Rust' must match");
    assert!(keys.contains(&"doc:2"), "upper-case 'RUST' must match");
    assert!(!keys.contains(&"doc:3"), "doc:3 has no 'rust' token");
}

// ── Stopword filtering ────────────────────────────────────────────────────────

/// Common stopwords must not match any document even if the word is present.
#[test]
fn test_stopword_the_returns_no_results() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    c.cmds(&["HSET", "doc:1", "body", "the quick brown fox"]);
    c.cmds(&["HSET", "doc:2", "body", "jumped over the lazy dog"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@body:the", "LIMIT", "0", "10"]);
    assert_eq!(
        r.ft_search_total(),
        0,
        "'the' is a stopword and must return 0 results"
    );
}

#[test]
fn test_stopwords_not_indexed() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    // All of these contain stopwords — none should be findable by stopword query
    c.cmds(&["HSET", "doc:1", "body", "this is a test"]);
    c.cmds(&["HSET", "doc:2", "body", "to be or not to be"]);
    thread::sleep(Duration::from_millis(50));

    for stopword in &["this", "is", "a", "to", "be", "or", "not"] {
        let query = format!("@body:{stopword}");
        let r = c.cmds(&["FT.SEARCH", "idx", &query, "LIMIT", "0", "10"]);
        assert_eq!(
            r.ft_search_total(),
            0,
            "stopword '{stopword}' must return 0 results"
        );
    }
}

/// Non-stopword in the same document must still be findable.
#[test]
fn test_non_stopword_still_matches_in_stopword_heavy_doc() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    c.cmds(&["HSET", "doc:1", "body", "this is a jemalloc allocator"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@body:jemalloc", "LIMIT", "0", "10"]);
    assert_eq!(r.ft_search_total(), 1, "non-stopword 'jemalloc' must match");
    assert!(r.ft_search_keys().contains(&"doc:1"));
}

// ── BM25 ranking ─────────────────────────────────────────────────────────────

/// Document with higher term frequency should rank above document with lower TF.
#[test]
fn test_bm25_higher_tf_ranks_first() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    // doc:1 mentions "rust" three times; doc:2 mentions it once.
    // BM25 should rank doc:1 above doc:2.
    c.cmds(&[
        "HSET",
        "doc:1",
        "body",
        "rust rust rust programming language",
    ]);
    c.cmds(&["HSET", "doc:2", "body", "rust programming"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@body:rust", "LIMIT", "0", "10"]);
    let keys = r.ft_search_keys();

    assert_eq!(keys.len(), 2, "both docs must match");
    assert_eq!(
        keys[0], "doc:1",
        "doc:1 (3x 'rust') must rank above doc:2 (1x 'rust') under BM25"
    );
}

/// Rare term (appearing in fewer docs) scores higher than common term.
#[test]
fn test_bm25_rare_term_scores_higher() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    // "jemalloc" only appears in doc:1. "programming" appears in all three.
    // doc:1 should rank highest for @body:jemalloc since it's the only match.
    c.cmds(&["HSET", "doc:1", "body", "jemalloc allocator programming"]);
    c.cmds(&["HSET", "doc:2", "body", "programming language design"]);
    c.cmds(&["HSET", "doc:3", "body", "systems programming"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@body:jemalloc", "LIMIT", "0", "10"]);
    assert_eq!(r.ft_search_total(), 1);
    assert_eq!(r.ft_search_keys()[0], "doc:1");
}

/// IDF: a term appearing in every document should score lower than a rare term.
/// We test this indirectly: query two terms, rare term doc ranks first.
#[test]
fn test_bm25_idf_penalises_common_terms() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    // "systems" appears in all 5 docs. "kividb" appears only in doc:1.
    // doc:1 should rank first for "@body:kividb" because kividb has high IDF.
    for i in 1..=5 {
        if i == 1 {
            c.cmds(&[
                "HSET",
                &format!("doc:{i}"),
                "body",
                "kividb systems database",
            ]);
        } else {
            c.cmds(&["HSET", &format!("doc:{i}"), "body", "systems database"]);
        }
    }
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@body:kividb", "LIMIT", "0", "10"]);
    assert_eq!(r.ft_search_total(), 1, "only doc:1 has 'kividb'");
    assert_eq!(r.ft_search_keys()[0], "doc:1");
}

// ── Multi-field indexing ──────────────────────────────────────────────────────

/// A query against @title must not match a token that only exists in @body.
#[test]
fn test_field_isolation_title_vs_body() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Index with two TEXT fields
    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "title",
        "TEXT",
        "body",
        "TEXT",
    ]);

    // "kividb" only in body, not title
    c.cmds(&[
        "HSET",
        "doc:1",
        "title",
        "database systems",
        "body",
        "kividb storage",
    ]);
    // "kividb" in title
    c.cmds(&[
        "HSET",
        "doc:2",
        "title",
        "kividb overview",
        "body",
        "fast storage",
    ]);
    thread::sleep(Duration::from_millis(50));

    // @title:kividb must only find doc:2
    let r = c.cmds(&["FT.SEARCH", "idx", "@title:kividb", "LIMIT", "0", "10"]);
    let keys = r.ft_search_keys();
    assert!(keys.contains(&"doc:2"), "doc:2 has 'kividb' in title");
    assert!(
        !keys.contains(&"doc:1"),
        "doc:1 must NOT match @title:kividb — 'kividb' is only in body"
    );

    // @body:kividb must only find doc:1
    let r2 = c.cmds(&["FT.SEARCH", "idx", "@body:kividb", "LIMIT", "0", "10"]);
    let keys2 = r2.ft_search_keys();
    assert!(keys2.contains(&"doc:1"), "doc:1 has 'kividb' in body");
    assert!(
        !keys2.contains(&"doc:2"),
        "doc:2 must NOT match @body:kividb — 'kividb' is only in title"
    );
}

/// A term present in both fields of the same document is found by either query.
#[test]
fn test_term_in_both_fields_matches_both_queries() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "title",
        "TEXT",
        "body",
        "TEXT",
    ]);

    c.cmds(&[
        "HSET",
        "doc:1",
        "title",
        "rust language",
        "body",
        "rust programming guide",
    ]);
    thread::sleep(Duration::from_millis(50));

    let r1 = c.cmds(&["FT.SEARCH", "idx", "@title:rust", "LIMIT", "0", "10"]);
    assert_eq!(r1.ft_search_total(), 1, "@title:rust must find doc:1");

    let r2 = c.cmds(&["FT.SEARCH", "idx", "@body:rust", "LIMIT", "0", "10"]);
    assert_eq!(r2.ft_search_total(), 1, "@body:rust must find doc:1");
}

// ── Inverted index — AND / OR ─────────────────────────────────────────────────

/// AND query: both terms must be present.
#[test]
fn test_and_filter_both_terms_required() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    c.cmds(&["HSET", "doc:1", "body", "rust async programming"]);
    c.cmds(&["HSET", "doc:2", "body", "rust programming"]);
    c.cmds(&["HSET", "doc:3", "body", "async tasks"]);
    thread::sleep(Duration::from_millis(50));

    // Both "rust" AND "async" required
    let r = c.cmds(&[
        "FT.SEARCH",
        "idx",
        "(@body:rust @body:async)",
        "LIMIT",
        "0",
        "10",
    ]);
    let keys = r.ft_search_keys();

    assert!(keys.contains(&"doc:1"), "doc:1 has both rust and async");
    assert!(!keys.contains(&"doc:2"), "doc:2 has rust but not async");
    assert!(!keys.contains(&"doc:3"), "doc:3 has async but not rust");
}

/// OR query: either term is sufficient.
#[test]
fn test_or_filter_either_term_sufficient() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "body");

    c.cmds(&["HSET", "doc:1", "body", "rust programming"]);
    c.cmds(&["HSET", "doc:2", "body", "python scripting"]);
    c.cmds(&["HSET", "doc:3", "body", "javascript frontend"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&[
        "FT.SEARCH",
        "idx",
        "(@body:rust | @body:python)",
        "LIMIT",
        "0",
        "10",
    ]);
    let keys = r.ft_search_keys();

    assert!(keys.contains(&"doc:1"), "doc:1 (rust) must match OR");
    assert!(keys.contains(&"doc:2"), "doc:2 (python) must match OR");
    assert!(
        !keys.contains(&"doc:3"),
        "doc:3 (javascript) must not match"
    );
}

// ── Backfill on FT.CREATE ─────────────────────────────────────────────────────

/// Documents written BEFORE FT.CREATE must be findable after creation.
#[test]
fn test_backfill_existing_docs_are_indexed() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Write hashes BEFORE creating the index
    c.cmds(&["HSET", "doc:1", "title", "rust allocator design"]);
    c.cmds(&["HSET", "doc:2", "title", "python garbage collection"]);

    // Now create the index — should backfill doc:1 and doc:2
    create_text_index(&mut c, "idx", "doc:", "title");
    thread::sleep(Duration::from_millis(100)); // backfill is synchronous but give it a moment

    let r = c.cmds(&["FT.SEARCH", "idx", "@title:rust", "LIMIT", "0", "10"]);
    assert_eq!(r.ft_search_total(), 1, "backfilled doc:1 must be found");
    assert_eq!(r.ft_search_keys()[0], "doc:1");
}

/// Backfill also populates BM25 stats — ranking must work on backfilled docs.
#[test]
fn test_backfill_bm25_ranking_works() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "doc:1", "body", "rust rust rust"]);
    c.cmds(&["HSET", "doc:2", "body", "rust language"]);

    create_text_index(&mut c, "idx", "doc:", "body");
    thread::sleep(Duration::from_millis(100));

    let r = c.cmds(&["FT.SEARCH", "idx", "@body:rust", "LIMIT", "0", "10"]);
    let keys = r.ft_search_keys();
    assert_eq!(keys.len(), 2);
    assert_eq!(
        keys[0], "doc:1",
        "doc:1 (3x rust) must rank above doc:2 (1x rust) even after backfill"
    );
}

// ── Incremental update via HSET ───────────────────────────────────────────────

/// A new HSET after FT.CREATE makes the document immediately findable.
#[test]
fn test_incremental_hset_adds_to_inverted_index() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    // Nothing indexed yet
    let r0 = c.cmds(&["FT.SEARCH", "idx", "@title:kividb", "LIMIT", "0", "10"]);
    assert_eq!(r0.ft_search_total(), 0);

    // Add a doc
    c.cmds(&["HSET", "doc:1", "title", "kividb storage engine"]);
    thread::sleep(Duration::from_millis(50));

    let r1 = c.cmds(&["FT.SEARCH", "idx", "@title:kividb", "LIMIT", "0", "10"]);
    assert_eq!(r1.ft_search_total(), 1, "doc:1 must now be findable");
}

/// Updating a doc's text field makes the new tokens searchable.
#[test]
fn test_incremental_hset_update_adds_new_tokens() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "storage engine"]);
    thread::sleep(Duration::from_millis(50));

    // "kividb" not yet in the document
    let r0 = c.cmds(&["FT.SEARCH", "idx", "@title:kividb", "LIMIT", "0", "10"]);
    assert_eq!(r0.ft_search_total(), 0);

    // Update the doc with a new title containing "kividb"
    c.cmds(&["HSET", "doc:1", "title", "kividb storage engine"]);
    thread::sleep(Duration::from_millis(50));

    let r1 = c.cmds(&["FT.SEARCH", "idx", "@title:kividb", "LIMIT", "0", "10"]);
    assert_eq!(
        r1.ft_search_total(),
        1,
        "updated doc:1 must now match @title:kividb"
    );
}

// ── Delete removes from inverted index ───────────────────────────────────────

/// After DEL, the document must no longer appear in search results.
#[test]
fn test_del_removes_doc_from_search() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "rust programming language"]);
    c.cmds(&["HSET", "doc:2", "title", "rust allocator"]);
    thread::sleep(Duration::from_millis(50));

    // Both found before delete
    let r0 = c.cmds(&["FT.SEARCH", "idx", "@title:rust", "LIMIT", "0", "10"]);
    assert_eq!(r0.ft_search_total(), 2);

    // Delete doc:1
    c.cmds(&["DEL", "doc:1"]);
    thread::sleep(Duration::from_millis(50));

    let r1 = c.cmds(&["FT.SEARCH", "idx", "@title:rust", "LIMIT", "0", "10"]);
    assert_eq!(r1.ft_search_total(), 1, "only doc:2 should remain");
    assert!(
        r1.ft_search_keys().contains(&"doc:2"),
        "doc:2 must still be found"
    );
    assert!(
        !r1.ft_search_keys().contains(&"doc:1"),
        "deleted doc:1 must not appear"
    );
}

/// Deleting all docs for a term leaves that term returning 0 results.
#[test]
fn test_del_last_doc_for_term_returns_zero() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "quuxbazxyz"]); // single unambiguous token
    thread::sleep(Duration::from_millis(50));

    let r0 = c.cmds(&["FT.SEARCH", "idx", "@title:quuxbazxyz", "LIMIT", "0", "10"]);
    assert_eq!(r0.ft_search_total(), 1);

    c.cmds(&["DEL", "doc:1"]);
    thread::sleep(Duration::from_millis(50));

    let r1 = c.cmds(&["FT.SEARCH", "idx", "@title:quuxbazxyz", "LIMIT", "0", "10"]);
    assert_eq!(
        r1.ft_search_total(),
        0,
        "posting list must be empty after delete"
    );
}

// ── Prefix queries ────────────────────────────────────────────────────────────

/// @field:ru* should match "rust", "ruby", "runtime" but not "programming".
#[test]
fn test_prefix_query_matches_correct_tokens() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "rust language"]);
    c.cmds(&["HSET", "doc:2", "title", "ruby scripting"]);
    c.cmds(&["HSET", "doc:3", "title", "runtime error"]);
    c.cmds(&["HSET", "doc:4", "title", "programming guide"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@title:ru*", "LIMIT", "0", "10"]);
    let keys = r.ft_search_keys();

    assert!(keys.contains(&"doc:1"), "rust must match ru*");
    assert!(keys.contains(&"doc:2"), "ruby must match ru*");
    assert!(keys.contains(&"doc:3"), "runtime must match ru*");
    assert!(!keys.contains(&"doc:4"), "programming must NOT match ru*");
}

/// Prefix with no matches returns 0 results.
#[test]
fn test_prefix_query_no_matches() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "rust programming"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "@title:xyz*", "LIMIT", "0", "10"]);
    assert_eq!(
        r.ft_search_total(),
        0,
        "prefix 'xyz*' must return 0 results"
    );
}

// ── Match-all with no TEXT clauses still works ────────────────────────────────

/// A pure TAG filter with no TEXT clauses must still use the scan path correctly.
#[test]
fn test_pure_tag_filter_no_text_clause() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "title",
        "TEXT",
        "category",
        "TAG",
    ]);

    c.cmds(&[
        "HSET",
        "doc:1",
        "title",
        "rust guide",
        "category",
        "systems",
    ]);
    c.cmds(&[
        "HSET",
        "doc:2",
        "title",
        "python guide",
        "category",
        "scripting",
    ]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&[
        "FT.SEARCH",
        "idx",
        "@category:{systems}",
        "LIMIT",
        "0",
        "10",
    ]);
    let keys = r.ft_search_keys();
    assert_eq!(keys.len(), 1);
    assert!(keys.contains(&"doc:1"));
}

/// Match-all (*) returns every indexed document.
#[test]
fn test_match_all_returns_all_docs() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "first document"]);
    c.cmds(&["HSET", "doc:2", "title", "second document"]);
    c.cmds(&["HSET", "doc:3", "title", "third document"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&["FT.SEARCH", "idx", "*", "LIMIT", "0", "10"]);
    assert_eq!(r.ft_search_total(), 3, "match-all must return all 3 docs");
}

// ── Backslash escapes ─────────────────────────────────────────────────────────

/// Sorted keys of an FT.SEARCH reply, or the error text.
fn search_keys(c: &mut Conn, index: &str, query: &str) -> Result<Vec<String>, String> {
    match c.cmds(&["FT.SEARCH", index, query, "NOCONTENT", "DIALECT", "2"]) {
        RespValue::Error(e) => Err(e),
        r => {
            let mut keys: Vec<String> = r.as_array()[1..]
                .iter()
                .map(|k| k.as_str().to_string())
                .collect();
            keys.sort();
            Ok(keys)
        }
    }
}

/// A backslash makes the next character part of the word, in a document and
/// in a query, as in Redis. LangGraph's RedisStore depends on it: it stores a
/// namespace such as `users\.u_ab12` and finds it, and the namespaces under
/// it, with `@prefix:users\.u_ab12` and `@prefix:users\.u_ab12*`. Expected
/// results are what Redis 8.10.2 returns for the same data.
#[test]
fn test_backslash_escape_keeps_punctuation_in_the_word() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "e:", "p");
    c.cmds(&["HSET", "e:1", "p", r"users\.u_ab12"]);
    c.cmds(&["HSET", "e:2", "p", r"users\.u_ab12\.sub"]);
    c.cmds(&["HSET", "e:3", "p", r"docs\.v_zz"]);
    c.cmds(&["HSET", "e:4", "p", "users.plain"]);
    thread::sleep(Duration::from_millis(50));

    let cases: &[(&str, &[&str])] = &[
        (r"@p:users\.u_ab12", &["e:1"]),
        (r"@p:users\.u_ab12*", &["e:1", "e:2"]),
        (r"@p:users\.u*", &["e:1", "e:2"]),
        (r"@p:users\.u_ab12\.sub", &["e:2"]),
        (r"@p:users\.u_ab12\.*", &["e:2"]),
        (r"@p:(users\.u_ab12*)", &["e:1", "e:2"]),
        ("@p:users*", &["e:1", "e:2", "e:4"]),
        ("@p:users", &["e:4"]),
        ("@p:u_ab12", &[]),
        ("@p:plain", &["e:4"]),
    ];
    for (query, want) in cases {
        let want: Vec<String> = want.iter().map(|k| k.to_string()).collect();
        assert_eq!(search_keys(&mut c, "idx", query), Ok(want), "query {query}");
    }
}

/// An escaped separator in a query does not match the two words it separates
/// in a document that did not escape it: `users\.plain` is one word, and the
/// document holds `users` and `plain`. Redis returns nothing.
#[test]
fn test_escaped_query_word_does_not_match_unescaped_document() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "e:", "p");
    c.cmds(&["HSET", "e:1", "p", "users.plain"]);
    c.cmds(&["HSET", "e:2", "p", "us-er"]);
    thread::sleep(Duration::from_millis(50));

    for query in [r"users\.plain", r"users\.pl*", r"us\-er*", r"users\.*"] {
        assert_eq!(
            search_keys(&mut c, "idx", query),
            Ok(vec![]),
            "query {query}"
        );
    }
}

/// The LangGraph RedisStore shape: a JSON index with the escaped namespace in
/// a TEXT field and the item key in a TAG. A parent namespace's query must
/// not return the child namespace's item with the same key.
#[test]
fn test_escaped_namespace_on_json_index_keeps_parent_and_child_apart() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "store",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "store:",
        "SCHEMA",
        "$.prefix",
        "AS",
        "prefix",
        "TEXT",
        "$.key",
        "AS",
        "key",
        "TAG",
    ]);
    c.cmds(&[
        "JSON.SET",
        "store:1",
        "$",
        r#"{"prefix":"org\\.t1","key":"profile"}"#,
    ]);
    c.cmds(&[
        "JSON.SET",
        "store:2",
        "$",
        r#"{"prefix":"org\\.t1\\.private","key":"profile"}"#,
    ]);
    thread::sleep(Duration::from_millis(50));

    let both = || Ok(vec!["store:1".to_string(), "store:2".to_string()]);
    assert_eq!(
        search_keys(&mut c, "store", r"(@prefix:(org\.t1) @key:{profile})"),
        Ok(vec!["store:1".to_string()])
    );
    assert_eq!(
        search_keys(&mut c, "store", r"@prefix:org\.t1\.private @key:{profile}"),
        Ok(vec!["store:2".to_string()])
    );
    assert_eq!(search_keys(&mut c, "store", r"@prefix:org\.t1*"), both());
}

/// A fuzzy term reads escapes as any other word does: `%users\.u_ab13%` is
/// one edit from the word `users.u_ab12`. Redis 8.10.2 matches it too.
#[test]
fn test_fuzzy_term_with_escape_matches_the_escaped_word() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "e:", "p");
    c.cmds(&["HSET", "e:1", "p", r"users\.u_ab12"]);
    thread::sleep(Duration::from_millis(50));

    assert_eq!(
        search_keys(&mut c, "idx", r"%users\.u_ab13%"),
        Ok(vec!["e:1".to_string()])
    );
}

/// HIGHLIGHT wraps an escaped word whole.
#[test]
fn test_highlight_wraps_an_escaped_word_whole() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "e:", "p");
    c.cmds(&["HSET", "e:1", "p", r"in users\.u_ab12 now"]);
    thread::sleep(Duration::from_millis(50));

    let r = c.cmds(&[
        "FT.SEARCH",
        "idx",
        r"@p:users\.u_ab12",
        "HIGHLIGHT",
        "DIALECT",
        "2",
    ]);
    let fields = r.as_array()[2].as_array();
    assert_eq!(fields[1].as_str(), r"in <b>users\.u_ab12</b> now", "{r:?}");
}

/// A parameter's value is one word: escapes are read, punctuation does not
/// split it. Redis 8.10.2 gives these results for the same data.
#[test]
fn test_param_value_is_one_word() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "e:", "p");
    c.cmds(&["HSET", "e:1", "p", r"users\.u_ab12"]);
    c.cmds(&["HSET", "e:2", "p", "users.plain"]);
    thread::sleep(Duration::from_millis(50));

    for (value, want) in [
        (r"users\.u_ab12", vec!["e:1"]),
        ("users.u_ab12", vec!["e:1"]),
        ("users.plain", vec![]),
        (r"users\.plain", vec![]),
    ] {
        let r = c.cmds(&[
            "FT.SEARCH",
            "idx",
            "@p:$v",
            "PARAMS",
            "2",
            "v",
            value,
            "NOCONTENT",
            "DIALECT",
            "2",
        ]);
        let keys: Vec<&str> = r.as_array()[1..].iter().map(|k| k.as_str()).collect();
        assert_eq!(keys, want, "value {value}");
    }
}

/// A parameter's value with a space in it is still one word, so it matches
/// no document: not the one that holds those words in that order, and not
/// the ones that hold them apart. The words in it are not query syntax
/// either. KiviDB used to split it into an AND of its words. Redis 7.4.7,
/// 8.2.10 and 8.10.2 give these results for the same data.
#[test]
fn test_param_value_with_a_space_is_one_word() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "d:", "p");
    c.cmds(&["HSET", "d:1", "p", "hello world"]);
    c.cmds(&["HSET", "d:2", "p", "hello"]);
    c.cmds(&["HSET", "d:3", "p", "world"]);
    c.cmds(&["HSET", "d:4", "p", "hello there world"]);
    c.cmds(&["HSET", "d:5", "p", "world hello"]);
    thread::sleep(Duration::from_millis(50));

    let all_hello = vec!["d:1", "d:2", "d:4", "d:5"];
    for query in ["@p:$v", "$v"] {
        for (value, want) in [
            ("hello world", vec![]),
            ("Hello World", vec![]),
            ("hello  world", vec![]),
            ("  hello  ", vec![]),
            ("hello|world", vec![]),
            ("hel*", vec![]),
            ("hello", all_hello.clone()),
            ("HELLO", all_hello.clone()),
        ] {
            let r = c.cmds(&[
                "FT.SEARCH",
                "idx",
                query,
                "PARAMS",
                "2",
                "v",
                value,
                "NOCONTENT",
                "DIALECT",
                "2",
            ]);
            let mut keys: Vec<&str> = r.as_array()[1..].iter().map(|k| k.as_str()).collect();
            keys.sort();
            assert_eq!(keys, want, "{query} with v = {value:?}");
        }
    }
}

/// FT.SPELLCHECK reads the query's words as the index has them: a word with
/// an escape or an underscore is spelled correctly when the index holds it.
#[test]
fn test_spellcheck_reads_words_as_the_index_does() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "e:", "p");
    c.cmds(&["HSET", "e:1", "p", r"users\.u_ab12 hello world_x"]);
    thread::sleep(Duration::from_millis(50));

    for query in [r"users\.u_ab12", "world_x"] {
        let r = c.cmds(&["FT.SPELLCHECK", "idx", query]);
        assert!(r.as_array().is_empty(), "{query}: {r:?}");
    }
    let r = c.cmds(&["FT.SPELLCHECK", "idx", r"users\.u_ab13"]);
    let term = r.as_array()[0].as_array();
    assert_eq!(term[1].as_str(), "users.u_ab13", "{r:?}");
    assert_eq!(
        term[2].as_array()[0].as_array()[1].as_str(),
        "users.u_ab12",
        "{r:?}"
    );
}

/// A query term matches every term of its synonym groups (`FT.SYNUPDATE`),
/// in documents indexed before the group was made as well as after, as in
/// RediSearch. The groups were kept, and searching changed nothing (#130).
#[test]
fn test_synonym_groups_expand_query_terms() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "syn:1", "name", "Desk lamp"]);
    assert!(
        c.cmds(&[
            "FT.CREATE",
            "syn",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "syn:",
            "SCHEMA",
            "name",
            "TEXT"
        ])
        .as_str()
            == "OK"
    );
    assert!(
        c.cmds(&["FT.SYNUPDATE", "syn", "g1", "lamp", "light"])
            .as_str()
            == "OK"
    );
    c.cmds(&["HSET", "syn:3", "name", "Reading lamp"]);
    c.cmds(&["HSET", "syn:4", "name", "Ceiling light"]);
    c.cmds(&["HSET", "syn:5", "name", "Office chair"]);
    let found = |c: &mut Conn, query: &str| -> Vec<String> {
        let reply = c.cmds(&["FT.SEARCH", "syn", query, "NOCONTENT", "SORTBY", "name"]);
        let mut keys: Vec<String> = reply.as_array()[1..]
            .iter()
            .map(|key| key.as_str().to_string())
            .collect();
        keys.sort();
        keys
    };
    let lamps = ["syn:1", "syn:3", "syn:4"];
    assert_eq!(found(&mut c, "light"), lamps);
    assert_eq!(found(&mut c, "lamp"), lamps);
    assert_eq!(found(&mut c, "@name:light"), lamps);
    assert_eq!(found(&mut c, "light office"), Vec::<String>::new());
    assert_eq!(found(&mut c, "chair"), ["syn:5"]);
}

// ── Document ids reused ──────────────────────────────────────────────────────

/// The TEXT index knows documents by numbers, which a deleted document gives
/// back to the next one: what a document held must not be found under the
/// one that gets its number, and a document written again holds only its
/// new words. Patterns are answered from the index as words are.
#[test]
fn test_reused_document_numbers_hold_only_their_own_words() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    create_text_index(&mut c, "idx", "doc:", "title");

    c.cmds(&["HSET", "doc:1", "title", "apple orchard"]);
    c.cmds(&["HSET", "doc:2", "title", "banana grove"]);
    c.cmds(&["HSET", "doc:3", "title", "cherry orchard"]);
    c.cmds(&["DEL", "doc:1"]);
    // Takes the number doc:1 gave back.
    c.cmds(&["HSET", "doc:4", "title", "zebra crossing"]);
    // Written again: its old words go.
    c.cmds(&["HSET", "doc:2", "title", "kiwi orchard"]);
    thread::sleep(Duration::from_millis(50));

    let keys = |c: &mut Conn, q: &str| -> Vec<String> {
        let r = c.cmds(&["FT.SEARCH", "idx", q, "LIMIT", "0", "10"]);
        let mut keys: Vec<String> = r.ft_search_keys().iter().map(|k| k.to_string()).collect();
        keys.sort();
        keys
    };
    assert!(keys(&mut c, "apple").is_empty());
    assert!(keys(&mut c, "banana").is_empty());
    assert_eq!(keys(&mut c, "zebra"), ["doc:4"]);
    assert_eq!(keys(&mut c, "orchard"), ["doc:2", "doc:3"]);
    assert_eq!(keys(&mut c, "kiwi | zebra"), ["doc:2", "doc:4"]);
    assert_eq!(keys(&mut c, "orchard -kiwi"), ["doc:3"]);
    assert_eq!(keys(&mut c, "orch*"), ["doc:2", "doc:3"]);
    assert_eq!(keys(&mut c, "*ebra"), ["doc:4"]);
    assert!(keys(&mut c, "app*").is_empty());
    // Stemmed: `crossing` is indexed with `+cross`.
    assert_eq!(keys(&mut c, "crosses"), ["doc:4"]);

    c.cmds(&["DEL", "doc:2", "doc:3", "doc:4"]);
    thread::sleep(Duration::from_millis(50));
    assert!(keys(&mut c, "orchard").is_empty());
    assert!(keys(&mut c, "orch*").is_empty());
}
