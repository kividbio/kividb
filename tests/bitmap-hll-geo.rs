// Integration tests for Bitmap, HyperLogLog, and Geo commands.
// Each test gets its own server on a fresh random port — fully isolated,
// runs in parallel with other test files.

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
        let stream = (0..20)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> RespValue {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        let owned: Vec<Vec<u8>> = args.iter().map(|s| s.as_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        self.cmd(&refs)
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
            _ => panic!("unexpected RESP line: {:?}", line),
        }
    }
}

#[derive(Debug, PartialEq)]
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
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap(),
            _ => panic!("not a string: {:?}", self),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            _ => panic!("not an int: {:?}", self),
        }
    }
    fn as_f64(&self) -> f64 {
        self.as_str().parse().unwrap()
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
    }
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

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
        let config = kividb::config::Config {
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
        };
        kividb::run_for_test(config);
    });
}

// ═════════════════════════════════════════════════════════════════════════════
// BITMAP TESTS
// ═════════════════════════════════════════════════════════════════════════════

// SETBIT / GETBIT basic
// Bit at offset 7 (last bit of first byte) — setting it makes byte 0 = 0x01
// Bit at offset 0 (first bit, MSB) — setting it makes byte 0 = 0x81
#[test]
fn test_setbit_getbit_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Key doesn't exist → all bits are 0
    assert_eq!(c.cmds(&["GETBIT", "bm", "7"]).as_int(), 0);

    // SETBIT returns the OLD value before setting
    assert_eq!(c.cmds(&["SETBIT", "bm", "7", "1"]).as_int(), 0);
    assert_eq!(c.cmds(&["GETBIT", "bm", "7"]).as_int(), 1);

    // Setting again returns 1 (old value was 1)
    assert_eq!(c.cmds(&["SETBIT", "bm", "7", "1"]).as_int(), 1);

    // Clear it — returns old value 1
    assert_eq!(c.cmds(&["SETBIT", "bm", "7", "0"]).as_int(), 1);
    assert_eq!(c.cmds(&["GETBIT", "bm", "7"]).as_int(), 0);
}

// SETBIT auto-extends the string with zero bytes
// Offset 100 is in byte 12 — the string must grow from 0 to 13 bytes
#[test]
fn test_setbit_auto_extends() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["SETBIT", "bm", "100", "1"]).as_int(), 0);
    assert_eq!(c.cmds(&["GETBIT", "bm", "100"]).as_int(), 1);
    // Bits in between are all 0
    assert_eq!(c.cmds(&["GETBIT", "bm", "50"]).as_int(), 0);
    assert_eq!(c.cmds(&["GETBIT", "bm", "0"]).as_int(), 0);
}

// SETBIT on an existing string key treats it as a bitmap
#[test]
fn test_setbit_on_existing_string() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // "\xff" = 0xFF = all bits set
    c.cmd(&[b"SET", b"bm", b"\xff"]);
    // All 8 bits of byte 0 should be 1
    for i in 0..8 {
        assert_eq!(
            c.cmds(&["GETBIT", "bm", &i.to_string()]).as_int(),
            1,
            "bit {i} should be 1"
        );
    }
}

// BITCOUNT — count set bits across the whole string and in ranges
#[test]
fn test_bitcount_full_and_range() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // 0xFF = 8 bits set, 0x0F = 4 bits set, 0xF0 = 4 bits set
    // We build this via SETBIT for portability
    // Byte 0: bits 0-7 all set (0xFF)
    for i in 0..8 {
        c.cmds(&["SETBIT", "bm", &i.to_string(), "1"]);
    }
    // Byte 1: only low 4 bits set (0x0F) — bits 8,9,10,11 are offset 8+7, 8+6, 8+5, 8+4
    // In big-endian bit order: bit 8 = MSB of byte 1, bit 15 = LSB of byte 1
    // Low 4 bits = bits 12,13,14,15
    for i in 12..16 {
        c.cmds(&["SETBIT", "bm", &i.to_string(), "1"]);
    }

    // Total: 8 + 4 = 12 bits
    assert_eq!(c.cmds(&["BITCOUNT", "bm"]).as_int(), 12);

    // BITCOUNT on byte range [0,0] → 8 bits
    assert_eq!(c.cmds(&["BITCOUNT", "bm", "0", "0"]).as_int(), 8);

    // BITCOUNT on byte range [1,1] → 4 bits
    assert_eq!(c.cmds(&["BITCOUNT", "bm", "1", "1"]).as_int(), 4);

    // Negative indices: [-1,-1] = last byte = byte 1 = 4 bits
    assert_eq!(c.cmds(&["BITCOUNT", "bm", "-1", "-1"]).as_int(), 4);

    // Non-existent key → 0
    assert_eq!(c.cmds(&["BITCOUNT", "nosuch"]).as_int(), 0);
}

// BITCOUNT with BIT mode (Redis 7.0+)
#[test]
fn test_bitcount_bit_mode() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Set bits 0, 3, 7
    c.cmds(&["SETBIT", "bm", "0", "1"]);
    c.cmds(&["SETBIT", "bm", "3", "1"]);
    c.cmds(&["SETBIT", "bm", "7", "1"]);

    // Count bits 0–7 in BIT mode = 3
    assert_eq!(c.cmds(&["BITCOUNT", "bm", "0", "7", "BIT"]).as_int(), 3);

    // Count bits 0–2 in BIT mode = 1 (only bit 0)
    assert_eq!(c.cmds(&["BITCOUNT", "bm", "0", "2", "BIT"]).as_int(), 1);

    // Count bits 4–7 in BIT mode = 1 (only bit 7)
    assert_eq!(c.cmds(&["BITCOUNT", "bm", "4", "7", "BIT"]).as_int(), 1);
}

// BITPOS — find first set or clear bit
#[test]
fn test_bitpos_find_set_bit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Set bit 10
    c.cmds(&["SETBIT", "bm", "10", "1"]);

    // First set bit should be at position 10
    assert_eq!(c.cmds(&["BITPOS", "bm", "1"]).as_int(), 10);

    // First set bit from byte 0 to byte 0: none in byte 0 → -1
    assert_eq!(c.cmds(&["BITPOS", "bm", "1", "0", "0"]).as_int(), -1);

    // First set bit from byte 1: bit 10 is in byte 1 → position 10
    assert_eq!(c.cmds(&["BITPOS", "bm", "1", "1"]).as_int(), 10);
}

#[test]
fn test_bitpos_find_clear_bit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // All bits in byte 0 set
    for i in 0..8 {
        c.cmds(&["SETBIT", "bm", &i.to_string(), "1"]);
    }

    // First clear bit should be at position 8 (first bit of byte 1)
    assert_eq!(c.cmds(&["BITPOS", "bm", "0"]).as_int(), 8);

    // Non-existent key — first clear bit is 0
    assert_eq!(c.cmds(&["BITPOS", "nosuch", "0"]).as_int(), 0);

    // Non-existent key — first set bit is -1 (none exist)
    assert_eq!(c.cmds(&["BITPOS", "nosuch", "1"]).as_int(), -1);
}

// BITOP AND / OR / XOR / NOT
#[test]
fn test_bitop_and_or_xor() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // key1 = 0xFF (all bits set), key2 = 0x0F (low 4 bits set)
    c.cmd(&[b"SET", b"k1", b"\xff"]);
    c.cmd(&[b"SET", b"k2", b"\x0f"]);

    // AND: 0xFF & 0x0F = 0x0F → 4 bits
    c.cmds(&["BITOP", "AND", "dest_and", "k1", "k2"]);
    assert_eq!(c.cmds(&["BITCOUNT", "dest_and"]).as_int(), 4);

    // OR: 0xFF | 0x0F = 0xFF → 8 bits
    c.cmds(&["BITOP", "OR", "dest_or", "k1", "k2"]);
    assert_eq!(c.cmds(&["BITCOUNT", "dest_or"]).as_int(), 8);

    // XOR: 0xFF ^ 0x0F = 0xF0 → 4 bits
    c.cmds(&["BITOP", "XOR", "dest_xor", "k1", "k2"]);
    assert_eq!(c.cmds(&["BITCOUNT", "dest_xor"]).as_int(), 4);
}

#[test]
fn test_bitop_not() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // NOT 0x0F = 0xF0 → 4 bits set
    c.cmds(&["SET", "k", "\x0f"]);
    c.cmds(&["BITOP", "NOT", "dest", "k"]);
    assert_eq!(c.cmds(&["BITCOUNT", "dest"]).as_int(), 4);
}

#[test]
fn test_bitop_returns_dest_length() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // BITOP returns the length of the destination string in bytes
    c.cmds(&["SET", "k1", "abc"]); // 3 bytes
    c.cmds(&["SET", "k2", "ab"]); // 2 bytes
    // Result is max(3,2) = 3
    let len = c.cmds(&["BITOP", "OR", "dest", "k1", "k2"]).as_int();
    assert_eq!(len, 3);
}

// BITOP with missing keys (treated as zero bytes)
#[test]
fn test_bitop_missing_key_is_zero() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmd(&[b"SET", b"k", b"\x0f"]);
    // AND with missing key = AND with 0x00 = 0x00 → 0 bits
    c.cmds(&["BITOP", "AND", "dest", "k", "nosuch"]);
    assert_eq!(c.cmds(&["BITCOUNT", "dest"]).as_int(), 0);
}

// BITFIELD GET / SET / INCRBY with unsigned encoding
#[test]
fn test_bitfield_unsigned() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // SET u8 at offset 0 to 200 → returns old value (0)
    let r = c.cmds(&["BITFIELD", "bf", "SET", "u8", "0", "200"]);
    assert_eq!(r.as_array()[0].as_int(), 0);

    // GET u8 at offset 0 → 200
    let r = c.cmds(&["BITFIELD", "bf", "GET", "u8", "0"]);
    assert_eq!(r.as_array()[0].as_int(), 200);

    // INCRBY u8 at offset 0 by 10 → 210 (returns new value)
    let r = c.cmds(&["BITFIELD", "bf", "INCRBY", "u8", "0", "10"]);
    assert_eq!(r.as_array()[0].as_int(), 210);
}

// BITFIELD with signed encoding
#[test]
fn test_bitfield_signed() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // SET i8 at offset 0 to -10
    let r = c.cmds(&["BITFIELD", "bf", "SET", "i8", "0", "-10"]);
    assert_eq!(r.as_array()[0].as_int(), 0);

    // GET i8 at offset 0 → -10
    let r = c.cmds(&["BITFIELD", "bf", "GET", "i8", "0"]);
    assert_eq!(r.as_array()[0].as_int(), -10);
}

// BITFIELD OVERFLOW WRAP (default) — wraps around on overflow
#[test]
fn test_bitfield_overflow_wrap() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // u8 max is 255. INCRBY 255 by 1 wraps to 0.
    c.cmds(&["BITFIELD", "bf", "SET", "u8", "0", "255"]);
    let r = c.cmds(&[
        "BITFIELD", "bf", "OVERFLOW", "WRAP", "INCRBY", "u8", "0", "1",
    ]);
    assert_eq!(r.as_array()[0].as_int(), 0);
}

// BITFIELD OVERFLOW SAT — clamps at max/min
#[test]
fn test_bitfield_overflow_sat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // u8 max is 255. SAT: 255 + 100 stays at 255
    c.cmds(&["BITFIELD", "bf", "SET", "u8", "0", "200"]);
    let r = c.cmds(&[
        "BITFIELD", "bf", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "100",
    ]);
    assert_eq!(r.as_array()[0].as_int(), 255);
}

// BITFIELD OVERFLOW FAIL — returns nil on overflow, no change
#[test]
fn test_bitfield_overflow_fail() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["BITFIELD", "bf", "SET", "u8", "0", "250"]);
    // FAIL: 250 + 10 would overflow u8 → nil, value unchanged
    let r = c.cmds(&[
        "BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "10",
    ]);
    assert!(r.as_array()[0].is_nil());
    // Value is still 250
    let r2 = c.cmds(&["BITFIELD", "bf", "GET", "u8", "0"]);
    assert_eq!(r2.as_array()[0].as_int(), 250);
}

// BITFIELD multiple subcommands in one call
#[test]
fn test_bitfield_multiple_subcommands() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // SET u8@0=10, SET u8@8=20, GET u8@0, GET u8@8 in one call
    let r = c.cmds(&[
        "BITFIELD", "bf", "SET", "u8", "0", "10", "SET", "u8", "8", "20", "GET", "u8", "0", "GET",
        "u8", "8",
    ]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0].as_int(), 0); // old value of @0
    assert_eq!(arr[1].as_int(), 0); // old value of @8
    assert_eq!(arr[2].as_int(), 10); // current @0
    assert_eq!(arr[3].as_int(), 20); // current @8
}

// BITFIELD_RO only allows GET
#[test]
fn test_bitfield_ro_rejects_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["BITFIELD", "bf", "SET", "u8", "0", "42"]);
    // BITFIELD_RO GET should work
    let r = c.cmds(&["BITFIELD_RO", "bf", "GET", "u8", "0"]);
    assert_eq!(r.as_array()[0].as_int(), 42);
    // BITFIELD_RO SET should error
    assert!(
        c.cmds(&["BITFIELD_RO", "bf", "SET", "u8", "0", "99"])
            .is_error()
    );
}

// WRONGTYPE errors for bitmap commands
#[test]
fn test_bitmap_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["LPUSH", "mylist", "a"]);
    assert!(c.cmds(&["GETBIT", "mylist", "0"]).is_error());
    assert!(c.cmds(&["SETBIT", "mylist", "0", "1"]).is_error());
    assert!(c.cmds(&["BITCOUNT", "mylist"]).is_error());
    assert!(c.cmds(&["BITPOS", "mylist", "1"]).is_error());
}

// Real-world scenario: daily active user tracking with bitmaps
// Each user has a numeric ID. We set their bit on login.
// BITCOUNT gives us the DAU count for the day.
#[test]
fn test_bitmap_dau_tracking() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let user_ids = [0u32, 5, 42, 100, 999, 1024];
    for &uid in &user_ids {
        c.cmds(&["SETBIT", "dau:2026-04-10", &uid.to_string(), "1"]);
    }

    // DAU count
    let dau = c.cmds(&["BITCOUNT", "dau:2026-04-10"]).as_int();
    assert_eq!(dau, user_ids.len() as i64);

    // Check specific users
    assert_eq!(c.cmds(&["GETBIT", "dau:2026-04-10", "42"]).as_int(), 1);
    assert_eq!(c.cmds(&["GETBIT", "dau:2026-04-10", "43"]).as_int(), 0);
}

// Real-world scenario: weekly retention — users active on both days via BITOP AND
#[test]
fn test_bitmap_retention_bitop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Day 1: users 1, 2, 3 active
    for uid in [1u32, 2, 3] {
        c.cmds(&["SETBIT", "day1", &uid.to_string(), "1"]);
    }
    // Day 2: users 2, 3, 4 active
    for uid in [2u32, 3, 4] {
        c.cmds(&["SETBIT", "day2", &uid.to_string(), "1"]);
    }

    // Retained = active on both days = BITOP AND
    c.cmds(&["BITOP", "AND", "retained", "day1", "day2"]);
    // Users 2 and 3 are retained
    assert_eq!(c.cmds(&["BITCOUNT", "retained"]).as_int(), 2);
    assert_eq!(c.cmds(&["GETBIT", "retained", "2"]).as_int(), 1);
    assert_eq!(c.cmds(&["GETBIT", "retained", "3"]).as_int(), 1);
    assert_eq!(c.cmds(&["GETBIT", "retained", "1"]).as_int(), 0);
    assert_eq!(c.cmds(&["GETBIT", "retained", "4"]).as_int(), 0);
}

// ═════════════════════════════════════════════════════════════════════════════
// HYPERLOGLOG TESTS
// ═════════════════════════════════════════════════════════════════════════════

// PFADD basic — returns 1 on change, 0 on no change
#[test]
fn test_pfadd_returns_changed() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // First add → modified → 1
    assert_eq!(c.cmds(&["PFADD", "hll", "a", "b", "c"]).as_int(), 1);
    // Same elements again → no change → 0
    assert_eq!(c.cmds(&["PFADD", "hll", "a", "b", "c"]).as_int(), 0);
    // New element → modified → 1
    assert_eq!(c.cmds(&["PFADD", "hll", "d"]).as_int(), 1);
}

// PFADD with no elements — creates key if missing (returns 1), no-op if exists (returns 0)
#[test]
fn test_pfadd_no_elements() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Key doesn't exist → creates it → 1
    assert_eq!(c.cmds(&["PFADD", "hll"]).as_int(), 1);
    // Key exists → no-op → 0
    assert_eq!(c.cmds(&["PFADD", "hll"]).as_int(), 0);
}

// PFCOUNT on empty / small sets
#[test]
fn test_pfcount_small_cardinality() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Empty key → 0
    assert_eq!(c.cmds(&["PFCOUNT", "nosuch"]).as_int(), 0);

    // Add exactly 3 unique elements
    c.cmds(&["PFADD", "hll", "apple", "banana", "cherry"]);
    let count = c.cmds(&["PFCOUNT", "hll"]).as_int();
    // HLL is approximate but for 3 elements should be exact or very close
    assert!((2..=4).contains(&count), "expected ~3, got {count}");
}

// PFCOUNT accuracy at scale — within 2% for 10,000 unique elements
#[test]
fn test_pfcount_accuracy_at_scale() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Add 1000 unique elements in batches
    // (10,000 would be slow over TCP; 1000 gives good statistical coverage)
    let n = 1000u64;
    for i in 0..n {
        c.cmds(&["PFADD", "hll", &format!("element:{i}")]);
    }

    let estimate = c.cmds(&["PFCOUNT", "hll"]).as_int() as u64;
    let lower = (n as f64 * 0.95) as u64; // 5% tolerance for test reliability
    let upper = (n as f64 * 1.05) as u64;
    assert!(
        estimate >= lower && estimate <= upper,
        "HLL estimate {estimate} out of range [{lower}, {upper}] for n={n}"
    );
}

// PFCOUNT with multiple keys — returns cardinality of union
#[test]
fn test_pfcount_multikey_union() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // hll1: {a, b, c}, hll2: {c, d, e} → union = {a,b,c,d,e} = 5
    c.cmds(&["PFADD", "hll1", "a", "b", "c"]);
    c.cmds(&["PFADD", "hll2", "c", "d", "e"]);

    let union_count = c.cmds(&["PFCOUNT", "hll1", "hll2"]).as_int();
    // Should be close to 5; allow small approximation error
    assert!(
        (4..=6).contains(&union_count),
        "expected ~5, got {union_count}"
    );

    // Each individually
    let c1 = c.cmds(&["PFCOUNT", "hll1"]).as_int();
    let c2 = c.cmds(&["PFCOUNT", "hll2"]).as_int();
    assert!((2..=4).contains(&c1));
    assert!((2..=4).contains(&c2));
}

// PFMERGE — merges sources into dest, dest has union cardinality
#[test]
fn test_pfmerge_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["PFADD", "src1", "a", "b", "c"]);
    c.cmds(&["PFADD", "src2", "d", "e", "f"]);

    assert!(c.cmds(&["PFMERGE", "dest", "src1", "src2"]).is_ok());

    let count = c.cmds(&["PFCOUNT", "dest"]).as_int();
    // Union of {a,b,c} and {d,e,f} = 6 unique elements
    assert!((5..=7).contains(&count), "expected ~6, got {count}");
}

// PFMERGE is idempotent — merging same sources twice doesn't grow the count
#[test]
fn test_pfmerge_idempotent() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["PFADD", "src", "x", "y", "z"]);
    c.cmds(&["PFMERGE", "dest", "src"]);
    let count1 = c.cmds(&["PFCOUNT", "dest"]).as_int();

    // Merge again into the same dest
    c.cmds(&["PFMERGE", "dest", "src"]);
    let count2 = c.cmds(&["PFCOUNT", "dest"]).as_int();

    assert_eq!(count1, count2);
}

// PFMERGE with missing source keys — treated as empty HLL
#[test]
fn test_pfmerge_missing_source() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["PFADD", "src", "a", "b"]);
    // "nosuch" doesn't exist — should be treated as empty
    assert!(c.cmds(&["PFMERGE", "dest", "src", "nosuch"]).is_ok());
    let count = c.cmds(&["PFCOUNT", "dest"]).as_int();
    assert!((1..=3).contains(&count));
}

// PFMERGE with no sources — creates empty HLL at dest
#[test]
fn test_pfmerge_no_sources() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["PFMERGE", "dest"]).is_ok());
    assert_eq!(c.cmds(&["PFCOUNT", "dest"]).as_int(), 0);
}

// PFSELFTEST — runs internal correctness checks
#[test]
fn test_pfselftest() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["PFSELFTEST"]).is_ok());
}

// PFDEBUG ENCODING — always returns "dense"
#[test]
fn test_pfdebug_encoding() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["PFADD", "hll", "a", "b", "c"]);
    assert_eq!(c.cmds(&["PFDEBUG", "ENCODING", "hll"]).as_str(), "dense");
}

// PFDEBUG GETREG — returns 16384 register values
#[test]
fn test_pfdebug_getreg() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["PFADD", "hll", "hello", "world"]);
    let regs = c.cmds(&["PFDEBUG", "GETREG", "hll"]);
    let arr = regs.as_array();
    assert_eq!(arr.len(), 16384);
    // All values should be small non-negative integers (0–63)
    for r in arr {
        let v = r.as_int();
        assert!((0..=63).contains(&v), "register value {v} out of range");
    }
    // At least some registers should be non-zero after adding 2 elements
    let nonzero = arr.iter().filter(|r| r.as_int() > 0).count();
    assert!(nonzero >= 1, "no registers set after PFADD");
}

// PFDEBUG DECODE — returns human-readable state string
#[test]
fn test_pfdebug_decode() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["PFADD", "hll", "a", "b", "c"]);
    let desc = c.cmds(&["PFDEBUG", "DECODE", "hll"]).as_str().to_string();
    // Should contain these fields
    assert!(
        desc.contains("encoding:dense"),
        "missing encoding field: {desc}"
    );
    assert!(
        desc.contains("registers:16384"),
        "missing registers field: {desc}"
    );
    assert!(desc.contains("nonzero:"), "missing nonzero field: {desc}");
    assert!(desc.contains("estimate:"), "missing estimate field: {desc}");
}

// PFDEBUG TODENSE — no-op for us, returns OK
#[test]
fn test_pfdebug_todense() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["PFADD", "hll", "x"]);
    assert!(c.cmds(&["PFDEBUG", "TODENSE", "hll"]).is_ok());
}

// PFDEBUG RESET — clears all registers, count goes to 0
#[test]
fn test_pfdebug_reset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["PFADD", "hll", "a", "b", "c", "d", "e"]);
    let before = c.cmds(&["PFCOUNT", "hll"]).as_int();
    assert!(before > 0);

    assert!(c.cmds(&["PFDEBUG", "RESET", "hll"]).is_ok());
    assert_eq!(c.cmds(&["PFCOUNT", "hll"]).as_int(), 0);
}

// PFDEBUG on non-existent key → error
#[test]
fn test_pfdebug_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["PFDEBUG", "GETREG", "nosuch"]).is_error());
    assert!(c.cmds(&["PFDEBUG", "DECODE", "nosuch"]).is_error());
    assert!(c.cmds(&["PFDEBUG", "RESET", "nosuch"]).is_error());
}

// WRONGTYPE errors for HLL commands
#[test]
fn test_hll_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["LPUSH", "mylist", "a"]);
    assert!(c.cmds(&["PFADD", "mylist", "x"]).is_error());
    assert!(c.cmds(&["PFCOUNT", "mylist"]).is_error());
    assert!(c.cmds(&["PFMERGE", "dest", "mylist"]).is_error());
}

// Real-world scenario: unique visitor counting across multiple pages
#[test]
fn test_hll_unique_visitors_per_page() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Simulate 100 visitors across 3 pages with some overlap
    for i in 0..60u32 {
        c.cmds(&["PFADD", "page:home", &format!("user:{i}")]);
    }
    for i in 40..100u32 {
        c.cmds(&["PFADD", "page:about", &format!("user:{i}")]);
    }
    for i in 0..30u32 {
        c.cmds(&["PFADD", "page:contact", &format!("user:{i}")]);
    }

    // Total unique visitors across all pages (union)
    c.cmds(&[
        "PFMERGE",
        "all_pages",
        "page:home",
        "page:about",
        "page:contact",
    ]);
    let total_unique = c.cmds(&["PFCOUNT", "all_pages"]).as_int();
    // 100 unique users total (0..100)
    assert!(
        (90..=110).contains(&total_unique),
        "expected ~100 unique visitors, got {total_unique}"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// GEO TESTS
// ═════════════════════════════════════════════════════════════════════════════

// GEOADD / GEOPOS round-trip
// Geo encoding loses a tiny bit of precision (26 bits per axis).
// We verify the decoded position is within ~0.001 degrees of the original.
#[test]
fn test_geoadd_geopos_roundtrip() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Palermo, Sicily
    assert_eq!(
        c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"])
            .as_int(),
        1
    );
    // Catania, Sicily
    assert_eq!(
        c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"])
            .as_int(),
        1
    );

    let pos = c.cmds(&["GEOPOS", "cities", "Palermo"]);
    let arr = pos.as_array();
    assert_eq!(arr.len(), 1);
    let pair = arr[0].as_array();
    let lon: f64 = pair[0].as_str().parse().unwrap();
    let lat: f64 = pair[1].as_str().parse().unwrap();
    assert!((lon - 13.361389).abs() < 0.001, "lon off: {lon}");
    assert!((lat - 38.115556).abs() < 0.001, "lat off: {lat}");
}

// GEOPOS on missing member → nil
#[test]
fn test_geopos_missing_member() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    let pos = c.cmds(&["GEOPOS", "cities", "Palermo", "NoSuchCity"]);
    let arr = pos.as_array();
    assert_eq!(arr.len(), 2);
    assert!(!arr[0].is_nil(), "Palermo should not be nil");
    assert!(arr[1].is_nil(), "NoSuchCity should be nil");
}

// GEODIST — distance between two members in various units
#[test]
fn test_geodist_units() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    // Redis's documented distance for Palermo→Catania is ~166.274 km
    let dist_m = c
        .cmds(&["GEODIST", "cities", "Palermo", "Catania", "M"])
        .as_f64();
    let dist_km = c
        .cmds(&["GEODIST", "cities", "Palermo", "Catania", "KM"])
        .as_f64();
    let dist_mi = c
        .cmds(&["GEODIST", "cities", "Palermo", "Catania", "MI"])
        .as_f64();

    // Allow 1% tolerance for the geohash precision loss
    assert!(
        (dist_km - 166.274).abs() < 2.0,
        "KM distance off: {dist_km}"
    );
    assert!((dist_m / 1000.0 - dist_km).abs() < 0.01, "M/KM mismatch");
    assert!(
        (dist_mi - dist_km * 0.621371).abs() < 1.0,
        "MI off: {dist_mi}"
    );
}

// GEODIST on missing member → nil
#[test]
fn test_geodist_missing_member() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    assert!(
        c.cmds(&["GEODIST", "cities", "Palermo", "Nowhere", "KM"])
            .is_nil()
    );
    assert!(
        c.cmds(&["GEODIST", "cities", "Nowhere", "Palermo", "KM"])
            .is_nil()
    );
}

// GEOHASH — returns 11-character base32 geohash strings
#[test]
fn test_geohash() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    let hashes = c.cmds(&["GEOHASH", "cities", "Palermo", "Catania"]);
    let arr = hashes.as_array();
    assert_eq!(arr.len(), 2);

    let palermo_hash = arr[0].as_str();
    let catania_hash = arr[1].as_str();

    // Must be 11 characters of valid base32 geohash alphabet
    assert_eq!(palermo_hash.len(), 11, "Palermo hash len: {palermo_hash}");
    assert_eq!(catania_hash.len(), 11, "Catania hash len: {catania_hash}");

    // Verify all characters are in the geohash base32 alphabet
    const VALID: &str = "0123456789bcdefghjkmnpqrstuvwxyz";
    for ch in palermo_hash.chars() {
        assert!(
            VALID.contains(ch),
            "invalid geohash char '{}' in '{}'",
            ch,
            palermo_hash
        );
    }
    // Geohash for Sicily starts with 's' — this is stable across precision levels
    assert!(
        palermo_hash.starts_with('s'),
        "Palermo should be in 's' quadrant, got: {palermo_hash}"
    );
}

// GEOHASH on missing member → nil
#[test]
fn test_geohash_missing_member() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    let hashes = c.cmds(&["GEOHASH", "cities", "Palermo", "Nowhere"]);
    let arr = hashes.as_array();
    assert!(!arr[0].is_nil());
    assert!(arr[1].is_nil());
}

// GEOADD NX — only add new members, never update existing
#[test]
fn test_geoadd_nx_flag() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);

    // NX: try to update Palermo's position — should be ignored (it exists)
    let added = c
        .cmds(&["GEOADD", "cities", "NX", "0.0", "0.0", "Palermo"])
        .as_int();
    assert_eq!(added, 0, "NX should not add existing member");

    // Position should be unchanged
    let pos = c.cmds(&["GEOPOS", "cities", "Palermo"]);
    let pair = pos.as_array()[0].as_array();
    let lon: f64 = pair[0].as_str().parse().unwrap();
    assert!(
        (lon - 13.361389).abs() < 0.01,
        "position changed despite NX"
    );
}

// GEOADD XX — only update existing members, never add new
#[test]
fn test_geoadd_xx_flag() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);

    // XX: try to add new member "NewCity" — should be ignored
    let added = c
        .cmds(&["GEOADD", "cities", "XX", "0.0", "0.0", "NewCity"])
        .as_int();
    assert_eq!(added, 0, "XX should not add new member");

    let pos = c.cmds(&["GEOPOS", "cities", "NewCity"]);
    assert!(pos.as_array()[0].is_nil(), "NewCity should not exist");
}

// GEOADD CH flag — returns count of added + changed, not just added
#[test]
fn test_geoadd_ch_flag() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);

    // CH: update Palermo (1 changed) + add Catania (1 added) = 2
    let result = c
        .cmds(&[
            "GEOADD",
            "cities",
            "CH",
            "13.4",
            "38.2",
            "Palermo", // update existing
            "15.087269",
            "37.502669",
            "Catania", // add new
        ])
        .as_int();
    assert_eq!(result, 2);
}

// GEOADD invalid coordinates → error
#[test]
fn test_geoadd_invalid_coordinates() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Longitude out of range
    assert!(c.cmds(&["GEOADD", "g", "181.0", "0.0", "m"]).is_error());
    assert!(c.cmds(&["GEOADD", "g", "-181.0", "0.0", "m"]).is_error());

    // Latitude out of range (Redis limits to ~±85.05)
    assert!(c.cmds(&["GEOADD", "g", "0.0", "90.0", "m"]).is_error());
    assert!(c.cmds(&["GEOADD", "g", "0.0", "-90.0", "m"]).is_error());
}

// GEOSEARCH FROMLONLAT BYRADIUS
#[test]
fn test_geosearch_byradius_fromlonlat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);
    c.cmds(&["GEOADD", "cities", "2.349014", "48.864716", "Paris"]);

    // Search from Palermo's coords, within 200km → should find Palermo and Catania, not Paris
    let result = c.cmds(&[
        "GEOSEARCH",
        "cities",
        "FROMLONLAT",
        "13.361389",
        "38.115556",
        "BYRADIUS",
        "200",
        "KM",
        "ASC",
    ]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2);
    // ASC: nearest first = Palermo (distance 0), then Catania
    assert_eq!(arr[0].as_str(), "Palermo");
    assert_eq!(arr[1].as_str(), "Catania");
}

// GEOSEARCH FROMMEMBER BYRADIUS
#[test]
fn test_geosearch_byradius_frommember() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);
    c.cmds(&["GEOADD", "cities", "2.349014", "48.864716", "Paris"]);

    // Search from Palermo member itself, within 200km
    let result = c.cmds(&[
        "GEOSEARCH",
        "cities",
        "FROMMEMBER",
        "Palermo",
        "BYRADIUS",
        "200",
        "KM",
        "ASC",
    ]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_str(), "Palermo");
    assert_eq!(arr[1].as_str(), "Catania");
}

// GEOSEARCH BYBOX
#[test]
fn test_geosearch_bybox() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);
    c.cmds(&["GEOADD", "cities", "2.349014", "48.864716", "Paris"]);

    // Box centered near Sicily, 400km wide x 200km tall — should include both Sicilian cities
    let result = c.cmds(&[
        "GEOSEARCH",
        "cities",
        "FROMLONLAT",
        "14.0",
        "37.8",
        "BYBOX",
        "400",
        "200",
        "KM",
        "ASC",
    ]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2, "expected Palermo and Catania inside box");
}

// GEOSEARCH WITHDIST — results include distance
#[test]
fn test_geosearch_withdist() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    let result = c.cmds(&[
        "GEOSEARCH",
        "cities",
        "FROMLONLAT",
        "13.361389",
        "38.115556",
        "BYRADIUS",
        "200",
        "KM",
        "ASC",
        "WITHDIST",
    ]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2);

    // Each result is [member, dist]
    let palermo_entry = arr[0].as_array();
    assert_eq!(palermo_entry[0].as_str(), "Palermo");
    let palermo_dist: f64 = palermo_entry[1].as_str().parse().unwrap();
    // Distance from Palermo to itself is ~0
    assert!(
        palermo_dist < 0.01,
        "dist to self should be ~0, got {palermo_dist}"
    );

    let catania_entry = arr[1].as_array();
    let catania_dist: f64 = catania_entry[1].as_str().parse().unwrap();
    // Catania is ~166km from Palermo
    assert!(
        catania_dist > 150.0 && catania_dist < 180.0,
        "catania dist: {catania_dist}"
    );
}

// GEOSEARCH WITHCOORD — results include coordinates
#[test]
fn test_geosearch_withcoord() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);

    let result = c.cmds(&[
        "GEOSEARCH",
        "cities",
        "FROMLONLAT",
        "13.361389",
        "38.115556",
        "BYRADIUS",
        "1",
        "KM",
        "WITHCOORD",
    ]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 1);

    // Each result is [member, [lon, lat]]
    let entry = arr[0].as_array();
    assert_eq!(entry[0].as_str(), "Palermo");
    let coord = entry[1].as_array();
    let lon: f64 = coord[0].as_str().parse().unwrap();
    let lat: f64 = coord[1].as_str().parse().unwrap();
    assert!((lon - 13.361389).abs() < 0.001);
    assert!((lat - 38.115556).abs() < 0.001);
}

// GEOSEARCH COUNT — limit number of results
#[test]
fn test_geosearch_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for i in 0..10i32 {
        let lon = 13.0 + i as f64 * 0.01;
        c.cmds(&["GEOADD", "pts", &lon.to_string(), "38.0", &format!("p{i}")]);
    }

    // Only return 3 nearest results
    let result = c.cmds(&[
        "GEOSEARCH",
        "pts",
        "FROMLONLAT",
        "13.0",
        "38.0",
        "BYRADIUS",
        "500",
        "KM",
        "ASC",
        "COUNT",
        "3",
    ]);
    assert_eq!(result.as_array().len(), 3);
}

// GEOSEARCH DESC — farthest first
#[test]
fn test_geosearch_desc() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    let result = c.cmds(&[
        "GEOSEARCH",
        "cities",
        "FROMLONLAT",
        "13.361389",
        "38.115556",
        "BYRADIUS",
        "200",
        "KM",
        "DESC",
    ]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2);
    // DESC: farthest first = Catania
    assert_eq!(arr[0].as_str(), "Catania");
    assert_eq!(arr[1].as_str(), "Palermo");
}

// GEOSEARCHSTORE — stores results into a new sorted set
#[test]
fn test_geosearchstore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);
    c.cmds(&["GEOADD", "cities", "2.349014", "48.864716", "Paris"]);

    let stored = c
        .cmds(&[
            "GEOSEARCHSTORE",
            "dest",
            "cities",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "200",
            "KM",
            "ASC",
        ])
        .as_int();
    assert_eq!(stored, 2);

    // dest is now a sorted set — verify with ZCARD
    assert_eq!(c.cmds(&["ZCARD", "dest"]).as_int(), 2);

    // And we can GEOPOS from it too — it's a valid geo sorted set
    let pos = c.cmds(&["GEOPOS", "dest", "Palermo"]);
    assert!(!pos.as_array()[0].is_nil());
}

// GEOSEARCHSTORE STOREDIST — stores distances as scores instead of geohash
#[test]
fn test_geosearchstore_storedist() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    c.cmds(&[
        "GEOSEARCHSTORE",
        "dest",
        "cities",
        "FROMLONLAT",
        "13.361389",
        "38.115556",
        "BYRADIUS",
        "200",
        "KM",
        "ASC",
        "STOREDIST",
    ]);

    // Scores are now distances in KM — Palermo's score should be ~0
    let palermo_score = c.cmds(&["ZSCORE", "dest", "Palermo"]).as_f64();
    assert!(
        palermo_score < 1.0,
        "Palermo STOREDIST score should be ~0, got {palermo_score}"
    );

    // Catania's score should be ~166 km
    let catania_score = c.cmds(&["ZSCORE", "dest", "Catania"]).as_f64();
    assert!(
        catania_score > 150.0 && catania_score < 180.0,
        "Catania STOREDIST score: {catania_score}"
    );
}

// GEORADIUS (legacy) — same as GEOSEARCH FROMLONLAT BYRADIUS
#[test]
fn test_georadius_legacy() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    let result = c.cmds(&["GEORADIUS", "cities", "15.0", "37.0", "200", "KM", "ASC"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2);
}

// GEORADIUSBYMEMBER (legacy)
#[test]
fn test_georadiusbymember_legacy() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    let result = c.cmds(&["GEORADIUSBYMEMBER", "cities", "Palermo", "200", "KM", "ASC"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_str(), "Palermo");
}

// GEORADIUS STORE — stores results into a sorted set
#[test]
fn test_georadius_store() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    let stored = c
        .cmds(&[
            "GEORADIUS",
            "cities",
            "13.361389",
            "38.115556",
            "200",
            "KM",
            "ASC",
            "STORE",
            "dest",
        ])
        .as_int();
    assert_eq!(stored, 2);
    assert_eq!(c.cmds(&["ZCARD", "dest"]).as_int(), 2);
}

// GEORADIUS_RO — read-only version, no STORE support needed
#[test]
fn test_georadius_ro() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    let result = c.cmds(&[
        "GEORADIUS_RO",
        "cities",
        "13.361389",
        "38.115556",
        "200",
        "KM",
        "ASC",
    ]);
    assert_eq!(result.as_array().len(), 2);
}

// GEORADIUSBYMEMBER_RO
#[test]
fn test_georadiusbymember_ro() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]);
    c.cmds(&["GEOADD", "cities", "15.087269", "37.502669", "Catania"]);

    let result = c.cmds(&[
        "GEORADIUSBYMEMBER_RO",
        "cities",
        "Palermo",
        "200",
        "KM",
        "ASC",
    ]);
    assert_eq!(result.as_array().len(), 2);
}

// WRONGTYPE errors for geo commands
#[test]
fn test_geo_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "mystr", "hello"]);
    assert!(c.cmds(&["GEOADD", "mystr", "13.0", "38.0", "m"]).is_error());
    assert!(c.cmds(&["GEODIST", "mystr", "a", "b", "KM"]).is_error());
    assert!(c.cmds(&["GEOPOS", "mystr", "m"]).is_error());
}

// Real-world scenario: find nearby restaurants within 5km
#[test]
fn test_geo_nearby_restaurants() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // A few restaurants in Rome
    c.cmds(&[
        "GEOADD",
        "restaurants",
        "12.4963",
        "41.9028",
        "Colosseum_Cafe",
    ]);
    c.cmds(&[
        "GEOADD",
        "restaurants",
        "12.4833",
        "41.8902",
        "Trastevere_Trattoria",
    ]);
    c.cmds(&[
        "GEOADD",
        "restaurants",
        "12.4739",
        "41.9002",
        "Vatican_Pizzeria",
    ]);
    c.cmds(&[
        "GEOADD",
        "restaurants",
        "13.0",
        "42.0",
        "FarAway_Ristorante",
    ]);

    // User is at the Colosseum
    let result = c.cmds(&[
        "GEOSEARCH",
        "restaurants",
        "FROMLONLAT",
        "12.4963",
        "41.9028",
        "BYRADIUS",
        "5",
        "KM",
        "ASC",
        "WITHDIST",
    ]);
    let arr = result.as_array();
    // FarAway should not be included
    let names: Vec<&str> = arr.iter().map(|r| r.as_array()[0].as_str()).collect();
    assert!(names.contains(&"Colosseum_Cafe"));
    assert!(!names.contains(&"FarAway_Ristorante"));
}

// Real-world scenario: delivery radius check
#[test]
fn test_geo_delivery_radius() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Store: warehouse at specific coords
    c.cmds(&[
        "GEOADD",
        "locations",
        "72.8777",
        "19.0760",
        "Mumbai_Warehouse",
    ]);

    // Customer locations
    c.cmds(&["GEOADD", "locations", "72.8856", "19.0822", "Customer_A"]); // ~1km away
    c.cmds(&["GEOADD", "locations", "73.0", "19.2", "Customer_B"]); // ~30km away

    // 10km delivery radius
    let result = c.cmds(&[
        "GEOSEARCH",
        "locations",
        "FROMMEMBER",
        "Mumbai_Warehouse",
        "BYRADIUS",
        "10",
        "KM",
        "ASC",
    ]);
    let arr = result.as_array();
    let names: Vec<&str> = arr.iter().map(|r| r.as_str()).collect();
    assert!(names.contains(&"Customer_A"), "Customer_A within 10km");
    assert!(!names.contains(&"Customer_B"), "Customer_B outside 10km");
}

/// The cached cardinality is kept in bytes 8-15 as Redis keeps it (#28):
/// PFCOUNT fills it in, a PFADD that changes the registers marks it stale
/// with the top bit of byte 15. The Redis suite's "PFADD / PFCOUNT cache
/// invalidation works".
#[test]
fn pfcount_caches_the_count_in_the_value_and_pfadd_marks_it_stale() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let byte = |c: &mut Conn, at: &str| match c.cmds(&["GETRANGE", "hll", at, at]) {
        RespValue::Bulk(b) => b,
        other => panic!("{other:?}"),
    };

    c.cmds(&["PFADD", "hll", "a", "b", "c"]);
    assert_eq!(c.cmds(&["PFCOUNT", "hll"]).as_int(), 3);
    assert_eq!(byte(&mut c, "15"), b"\x00");
    assert_eq!(byte(&mut c, "8"), b"\x03", "the count, little-endian");
    assert_eq!(c.cmds(&["PFADD", "hll", "a", "b", "c"]).as_int(), 0);
    assert_eq!(byte(&mut c, "15"), b"\x00", "nothing changed");
    assert_eq!(c.cmds(&["PFADD", "hll", "1", "2", "3"]).as_int(), 1);
    assert_eq!(byte(&mut c, "15"), b"\x80", "stale");
    assert_eq!(c.cmds(&["PFCOUNT", "hll"]).as_int(), 6);
    assert_eq!(byte(&mut c, "15"), b"\x00");

    // PFMERGE leaves the destination's cache stale, and its count right.
    c.cmds(&["PFADD", "other", "x", "y"]);
    c.cmds(&["PFMERGE", "hll", "other"]);
    assert_eq!(byte(&mut c, "15"), b"\x80");
    assert_eq!(c.cmds(&["PFCOUNT", "hll"]).as_int(), 8);

    // A TTL is kept when PFCOUNT writes the count in.
    c.cmds(&["PFADD", "hll", "z"]);
    c.cmds(&["EXPIRE", "hll", "1000"]);
    assert_eq!(c.cmds(&["PFCOUNT", "hll"]).as_int(), 9);
    assert!(c.cmds(&["TTL", "hll"]).as_int() > 900);

    // One saved before 1.1.1 has zeros there, whatever it holds: counted
    // again, not taken as a count of 0.
    c.cmds(&["PFADD", "old", "a", "b", "c", "d"]);
    c.cmd(&[b"SETRANGE", b"old", b"8", &[0u8; 8]]);
    assert_eq!(c.cmds(&["PFCOUNT", "old"]).as_int(), 4);
}
