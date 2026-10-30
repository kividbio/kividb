use ahash::AHashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_client_id() -> u64 {
    NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}

/// A monotonic clock the whole process shares, started at first use. Idle
/// time is measured against it, not the wall clock, so `idle=` cannot jump
/// when the system clock is set back or forwards.
static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Whole seconds since the process's monotonic epoch. Non-decreasing.
fn now_seconds() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_secs()
}

/// A handle the server loop holds to detect a CLIENT KILL.
/// When the kill command fires, the Receiver resolves and the loop breaks.
pub type KillRx = oneshot::Receiver<()>;

/// What a connection was last asked to do, for `CLIENT LIST`'s `cmd=` and
/// `db=`.
///
/// Written by the connection on every command, so it has to cost next to
/// nothing: a few words stored without a lock. The registry — one lock for
/// every worker thread — is not touched. Read only to list the clients; a
/// reader that catches the connection between two commands may see the name
/// of one with the subcommand of the other, and that is all that can go
/// wrong.
#[derive(Debug, Default)]
pub struct Activity {
    /// The command's name, as sent, cut to 24 bytes.
    name: [AtomicU64; 3],
    /// When the connection last interacted, in seconds on the monotonic
    /// clock [`now_seconds`] reads. Written once per socket read — see
    /// [`touch`](Activity::touch) — and read to print `idle=`. Nothing else
    /// reads it, so a torn view between the two is a second of error on that
    /// one field and nothing more.
    last_interaction: AtomicU64,
    /// Its first argument, if that is of 16 bytes or fewer: the subcommand,
    /// if the command has them.
    sub: [AtomicU64; 2],
    /// The two lengths.
    lens: AtomicU64,
    db: std::sync::atomic::AtomicUsize,
    /// What is to be written to the connection after the reply to the
    /// command it is running: a message that command gave rise to, for this
    /// same connection. See `tracking::tell`.
    after_reply: parking_lot::Mutex<Vec<u8>>,
    has_after_reply: std::sync::atomic::AtomicBool,
    /// The connection has killed itself (`CLIENT KILL … SKIPME no`): it is
    /// to be closed, but not before the reply to that command is written.
    closing_after_reply: std::sync::atomic::AtomicBool,
    /// Redis 8.10's per-client counters for `CLIENT LIST`: bytes read and
    /// written, commands run, reads, and commands per read (sum and count).
    /// Written only by the connection's own loop (or the script it is
    /// waiting on), so each is a load and a store, no locked instruction.
    io: [AtomicU64; 7],
    /// `CLIENT LIST`'s `multi` and `watch` (#158): the commands queued in the
    /// transaction, plus one (0 outside one), in the high half; the keys
    /// watched in the low. Written by the connection's loop as its replies go
    /// out, only when they changed.
    tx: AtomicU64,
}

/// Clients blocked in a command with a timeout: `clients_in_timeout_table`.
static IN_TIMEOUT_TABLE: AtomicU64 = AtomicU64::new(0);

const IO_NET_IN: usize = 0;
const IO_NET_OUT: usize = 1;
const IO_CMDS: usize = 2;
const IO_READS: usize = 3;
const IO_PIPE_SUM: usize = 4;
const IO_PIPE_CNT: usize = 5;
/// 1 from when a command starts until its read's replies are written. As
/// in Redis, a command counts once it has run: a `CLIENT INFO` does not count
/// itself, and a `BLPOP` that is blocked counts when it is answered. It is
/// in `IO_CMDS` from the start (one store more a command than that alone),
/// and `tot-cmds` is `IO_CMDS` less this.
const IO_RUNNING: usize = 6;

/// A connection's current RESP query buffer, published without taking the
/// registry lock. `free` is the spare capacity at the end of the backing Vec.
#[derive(Debug, Default)]
pub struct QueryBufferMetrics {
    /// The high and low halves hold qbuf and qbuf-free. Redis's maximum query
    /// buffer is below 2 GiB, so each value fits in 32 bits.
    values: AtomicU64,
}

impl QueryBufferMetrics {
    #[inline]
    pub fn update(&self, len: usize, free: usize, private: bool) {
        let len = len.min(u32::MAX as usize) as u64;
        let free = if private {
            free.min(u32::MAX as usize) as u64
        } else {
            0
        };
        self.values.store((len << 32) | free, Ordering::Relaxed);
    }

    #[inline]
    fn values(&self) -> (u64, u64) {
        let values = self.values.load(Ordering::Relaxed);
        (values >> 32, values & u32::MAX as u64)
    }
}

/// `CLIENT REPLY ON | OFF | SKIP`, as the connection loop keeps it.
///
/// The loop builds one buffer of replies for each batch of commands it
/// reads. [`begin`](Self::begin) is called as each command is about to run
/// and [`settle`](Self::settle) when the batch is over; between them they
/// take out of the buffer the replies the client asked not to be sent. A
/// connection that never uses the command pays one comparison per command.
#[derive(Default)]
pub struct ReplyGate {
    mode: ReplyMode,
    /// Where in the buffer the running command's reply starts.
    mark: usize,
    /// A command has begun whose reply has not been looked at.
    open: bool,
    /// That command is one whose output is not a reply: see `begin`.
    keep: bool,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum ReplyMode {
    #[default]
    On,
    Off,
    /// `SKIP` was just given: no reply to it, nor to the command after it.
    SkipTwo,
    /// No reply to this command; then replies again.
    SkipOne,
}

impl ReplyGate {
    #[inline(always)]
    pub fn begin(&mut self, cmd: &[&[u8]], out: &mut Vec<u8>) {
        // Nearly always: replies are on, and this is not `CLIENT REPLY`.
        if self.mode == ReplyMode::On && !(cmd.len() == 3 && cmd[0].len() == 6) {
            return;
        }
        self.begin_gated(cmd, out);
    }

    #[cold]
    fn begin_gated(&mut self, cmd: &[&[u8]], out: &mut Vec<u8>) {
        if self.mode != ReplyMode::On {
            self.settle(out);
        }
        if cmd.len() == 3
            && cmd[0].len() == 6
            && cmd[0].eq_ignore_ascii_case(b"CLIENT")
            && cmd[1].eq_ignore_ascii_case(b"REPLY")
        {
            if cmd[2].eq_ignore_ascii_case(b"ON") {
                self.mode = ReplyMode::On;
            } else if cmd[2].eq_ignore_ascii_case(b"OFF") {
                self.mode = ReplyMode::Off;
            } else if cmd[2].eq_ignore_ascii_case(b"SKIP") && self.mode == ReplyMode::On {
                self.mode = ReplyMode::SkipTwo;
            }
        }
        if self.mode != ReplyMode::On {
            self.mark = out.len();
            self.open = true;
            // What the subscription commands write is not a reply but a
            // message — the first of those the subscription brings — and
            // messages are sent whatever was said about replies.
            self.keep =
                cmd[0].len() >= 9 && cmd[0][cmd[0].len() - 9..].eq_ignore_ascii_case(b"SUBSCRIBE");
        }
    }

    /// Drop the reply of the command that has just run, if it is not wanted.
    #[inline(always)]
    pub fn settle(&mut self, out: &mut Vec<u8>) {
        if self.open {
            self.settle_open(out);
        }
    }

    #[cold]
    fn settle_open(&mut self, out: &mut Vec<u8>) {
        self.open = false;
        // A push (RESP3's `>`) is not a reply: the confirmation of a
        // subscription still goes out.
        if !self.keep && out.get(self.mark) != Some(&b'>') {
            out.truncate(self.mark);
        }
        self.mode = match self.mode {
            ReplyMode::SkipTwo => ReplyMode::SkipOne,
            ReplyMode::SkipOne => ReplyMode::On,
            mode => mode,
        };
    }
}

/// What kind of client a connection is, as `CLIENT LIST TYPE` and
/// `CLIENT KILL TYPE` name them.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClientKind {
    Normal,
    Master,
    Replica,
    PubSub,
}

impl ClientKind {
    pub fn parse(raw: &[u8]) -> Option<Self> {
        if raw.eq_ignore_ascii_case(b"normal") {
            Some(ClientKind::Normal)
        } else if raw.eq_ignore_ascii_case(b"master") {
            Some(ClientKind::Master)
        } else if raw.eq_ignore_ascii_case(b"replica") || raw.eq_ignore_ascii_case(b"slave") {
            Some(ClientKind::Replica)
        } else if raw.eq_ignore_ascii_case(b"pubsub") {
            Some(ClientKind::PubSub)
        } else {
            None
        }
    }
}

/// Which clients `CLIENT LIST` shows or `CLIENT KILL` closes: those that
/// match every condition given.
#[derive(Default)]
pub struct ClientFilter {
    pub ids: Option<Vec<u64>>,
    pub addr: Option<String>,
    pub laddr: Option<String>,
    pub user: Option<String>,
    pub kind: Option<ClientKind>,
    /// Connected for at least this many seconds.
    pub max_age: Option<u64>,
    /// Leave out the client that is asking.
    pub skip_me: bool,
}

impl ClientFilter {
    fn matches(&self, client: &ClientInfo, me: u64) -> bool {
        if self.skip_me && client.id == me {
            return false;
        }
        self.ids.as_ref().is_none_or(|ids| ids.contains(&client.id))
            && self.addr.as_ref().is_none_or(|addr| *addr == client.addr)
            && self.laddr.as_ref().is_none_or(|addr| *addr == client.laddr)
            && self.user.as_ref().is_none_or(|user| *user == client.user)
            && self.kind.is_none_or(|kind| kind == client.kind())
            && self
                .max_age
                .is_none_or(|age| client.connected_at.elapsed().as_secs() >= age)
    }
}

/// `bytes` — eight of them at most — as a number, the first the lowest.
#[inline(always)]
fn up_to_eight(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    if len >= 8 {
        let mut all = [0u8; 8];
        all.copy_from_slice(&bytes[..8]);
        u64::from_le_bytes(all)
    } else if len >= 4 {
        // The first four and the last four, which overlap: where they do,
        // they agree.
        let mut head = [0u8; 4];
        head.copy_from_slice(&bytes[..4]);
        let mut tail = [0u8; 4];
        tail.copy_from_slice(&bytes[len - 4..]);
        u32::from_le_bytes(head) as u64 | (u32::from_le_bytes(tail) as u64) << (8 * (len - 4))
    } else if len == 0 {
        0
    } else {
        // One, two or three: the first, the middle and the last, which are
        // the same byte where there are fewer than three.
        (bytes[0] as u64)
            | (bytes[len / 2] as u64) << (8 * (len / 2))
            | (bytes[len - 1] as u64) << (8 * (len - 1))
    }
}

/// `word`'s first bytes, eight to a slot. Returns how many were kept.
///
/// Only the slots the word reaches are written: what an earlier, longer
/// word left in the others is past the length recorded with this one, and
/// is not read.
#[inline(always)]
fn pack<const N: usize>(word: &[u8], into: &[AtomicU64; N]) -> u64 {
    // Nearly always: a word of eight bytes or fewer.
    if word.len() <= 8 {
        into[0].store(up_to_eight(word), Ordering::Relaxed);
        return word.len() as u64;
    }
    // Often: a key of nine to sixteen bytes in the place of a subcommand.
    // Two stores, without the iterator the general case below goes through.
    if word.len() <= 16 && N >= 2 {
        into[0].store(up_to_eight(&word[..8]), Ordering::Relaxed);
        into[1].store(up_to_eight(&word[8..]), Ordering::Relaxed);
        return word.len() as u64;
    }
    let len = word.len().min(N * 8);
    for (slot, chunk) in into.iter().zip(word[..len].chunks(8)) {
        slot.store(up_to_eight(chunk), Ordering::Relaxed);
    }
    len as u64
}

fn unpack<const N: usize>(from: &[AtomicU64; N], len: usize) -> String {
    let bytes: Vec<u8> = from
        .iter()
        .flat_map(|slot| slot.load(Ordering::Relaxed).to_le_bytes())
        .take(len.min(N * 8))
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

impl Activity {
    /// The connection's transaction now: the commands queued if it is in
    /// one, and the keys it watches. See [`Activity::tx`].
    #[inline]
    pub fn note_tx(&self, queued: Option<usize>, watched: usize) {
        let multi = queued.map_or(0, |n| n as u64 + 1).min(u32::MAX as u64);
        let packed = (multi << 32) | (watched as u64).min(u32::MAX as u64);
        if self.tx.load(Ordering::Relaxed) != packed {
            self.tx.store(packed, Ordering::Relaxed);
        }
    }

    /// `(multi, watch)` as `CLIENT LIST` writes them: `multi` is -1 outside
    /// a transaction.
    fn tx(&self) -> (i64, u64) {
        let packed = self.tx.load(Ordering::Relaxed);
        ((packed >> 32) as i64 - 1, packed & u32::MAX as u64)
    }

    /// See [`Activity::closing_after_reply`].
    pub fn closes_after_reply(&self) -> bool {
        self.closing_after_reply.load(Ordering::Relaxed)
    }

    pub fn push_after_reply(&self, frame: &[u8]) {
        self.after_reply.lock().extend_from_slice(frame);
        self.has_after_reply.store(true, Ordering::Relaxed);
    }

    /// Move what was held to the end of `out`.
    #[inline]
    pub fn drain_after_reply(&self, out: &mut Vec<u8>) {
        if self.has_after_reply.swap(false, Ordering::Relaxed) {
            out.append(&mut self.after_reply.lock());
        }
    }

    /// The database the connection has selected.
    pub fn db(&self) -> usize {
        self.db.load(Ordering::Relaxed)
    }

    /// Note that the connection has just interacted, so `idle=` restarts
    /// from now. One relaxed store.
    ///
    /// Called once per socket read by the connection loop — not once per
    /// command — so a pipeline of many commands that arrives in a single read
    /// pays for one clock reading, and a client that is idle between reads
    /// keeps the idle time it has built up. Reading client information does
    /// not touch, so `CLIENT LIST` leaves every listed client's timer alone.
    #[inline]
    pub fn touch(&self) {
        self.last_interaction
            .store(now_seconds(), Ordering::Relaxed);
    }

    /// Whole seconds since the connection last interacted: what Redis prints
    /// as `idle=`. Reading it does not touch the connection, so `CLIENT LIST`
    /// leaves every listed client's idle timer where it was.
    pub fn idle_seconds(&self) -> u64 {
        self.idle_seconds_at(now_seconds())
    }

    /// [`idle_seconds`](Self::idle_seconds) for a given clock reading, so the
    /// elapsed-time arithmetic can be checked without waiting on a clock.
    fn idle_seconds_at(&self, now: u64) -> u64 {
        now.saturating_sub(self.last_interaction.load(Ordering::Relaxed))
    }

    /// The monotonic reading this activity last recorded, for tests and for
    /// anyone reasoning about the clock directly.
    #[cfg(test)]
    fn interaction_at(&self) -> u64 {
        self.last_interaction.load(Ordering::Relaxed)
    }

    #[inline]
    fn bump(&self, which: usize, by: u64) {
        let counter = &self.io[which];
        counter.store(
            counter.load(Ordering::Relaxed).wrapping_add(by),
            Ordering::Relaxed,
        );
    }

    /// `n` bytes read from the connection: one read event.
    #[inline]
    pub fn note_read(&self, n: usize) {
        self.bump(IO_NET_IN, n as u64);
        self.bump(IO_READS, 1);
    }

    /// Commands run so far: taken before a read's commands are parsed, for
    /// [`note_flush`](Self::note_flush) to see how many that read held.
    #[inline]
    pub fn commands_run(&self) -> u64 {
        self.io[IO_CMDS].load(Ordering::Relaxed)
    }

    /// The replies to one read's commands written, `out` bytes of them.
    #[inline]
    pub fn note_flush(&self, out: usize, commands_before: u64) {
        self.bump(IO_NET_OUT, out as u64);
        self.io[IO_RUNNING].store(0, Ordering::Relaxed);
        let ran = self.commands_run().wrapping_sub(commands_before);
        if ran > 0 {
            self.bump(IO_PIPE_SUM, ran);
            self.bump(IO_PIPE_CNT, 1);
        }
    }

    fn io(&self, which: usize) -> u64 {
        self.io[which].load(Ordering::Relaxed)
    }

    /// Commands a script it ran ran (`redis.call`): Redis counts them as
    /// the client's.
    pub fn add_commands(&self, n: u64) {
        self.bump(IO_CMDS, n);
    }

    #[inline]
    pub fn record(&self, cmd: &[&[u8]], db: usize) {
        self.bump(IO_CMDS, 1);
        self.io[IO_RUNNING].store(1, Ordering::Relaxed);
        self.db.store(db, Ordering::Relaxed);
        let name = pack(cmd[0], &self.name);
        // An argument too long to be the name of a subcommand is a key, or
        // a value: nothing that is shown.
        let sub = match cmd.get(1) {
            Some(sub) if sub.len() <= 16 => pack(sub, &self.sub),
            _ => 0,
        };
        self.lens.store(name | (sub << 8), Ordering::Relaxed);
    }

    /// The name as Redis prints it: lower case, and `client|list` for a
    /// command that has subcommands.
    pub fn last_cmd(&self) -> String {
        let lens = self.lens.load(Ordering::Relaxed);
        let name = unpack(&self.name, (lens & 0xff) as usize);
        let sub = unpack(&self.sub, (lens >> 8) as usize);
        if name.is_empty() {
            return "NULL".to_string();
        }
        if !sub.is_empty() && crate::commands::arity::is_container(&name.to_ascii_uppercase()) {
            format!("{name}|{sub}").to_ascii_lowercase()
        } else {
            name.to_ascii_lowercase()
        }
    }
}

/// How many out-of-band messages (invalidations, RESP3 pub/sub, MONITOR
/// lines) a connection's push queue holds before a client that does not read
/// them is closed (#161). Generous because it costs nothing until used: the
/// channel allocates as it fills. Redis puts no limit on a normal client's
/// output buffer at all.
pub const PUSH_QUEUE_FRAMES: usize = 65_536;

#[derive(Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: String,
    /// The address the client connected to.
    pub laddr: String,
    /// The user it is authenticated as.
    pub user: String,
    pub name: String,
    pub db: usize,
    pub flags: String,
    pub connected_at: Instant,
    pub activity: Arc<Activity>,
    pub query_buffer: Arc<QueryBufferMetrics>,
    pub no_evict: bool,
    /// True iff the client has issued ASKING for the next command.
    /// Single-shot: cleared after the next command runs (whatever the result).
    pub asking: bool,
    /// True iff the client has issued READONLY (cluster replica reads).
    /// Persistent until READWRITE clears it.
    pub readonly: bool,
    // Sending to this closes the connection. Wrapped in Option so we can
    // take() it exactly once inside kill(). None after the first kill call.
    kill_tx: Option<oneshot::Sender<()>>,
    /// While the client is blocked in a command: how `CLIENT UNBLOCK` ends
    /// its wait.
    unblock_tx: Option<oneshot::Sender<crate::commands::blocking::Unblock>>,
    /// Blocked with a timeout: counted in [`IN_TIMEOUT_TABLE`].
    blocked_with_timeout: bool,
    // Tracking state
    pub tracking: bool,
    pub tracking_bcast: bool,
    pub tracking_prefixes: Vec<String>,
    pub tracking_redirect: Option<u64>, // redirect invalidation to another client ID
    /// The client they were redirected to has gone.
    pub tracking_redirect_broken: bool,
    // Channel to push out-of-band messages (invalidations) to this client.
    // None until the connection registers it after setup.
    pub push_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// OPTIN/OPTOUT tracking mode.
    pub tracking_optin: bool, // true = OPTIN mode
    pub tracking_optout: bool, // true = OPTOUT mode
    /// Single-shot flag set by CLIENT CACHING yes/no.
    /// Applies to the next read command only, then cleared in handler.rs.
    pub tracking_caching_next: Option<bool>, // Some(true)=yes, Some(false)=no, None=unset
    pub tracking_noloop: bool,
    pub lib_name: String,
    pub lib_ver: String,
    /// Negotiated Redis protocol version per connection (`HELLO`): 2 or 3.
    pub resp: u8,
    /// Port announced with `REPLCONF listening-port`, 0 if none. Read when the
    /// connection turns into a replica link, so the master can list the
    /// replica by the port it serves clients on.
    pub replica_listening_port: u16,
    /// A replica that said `REPLCONF capa dataset-digest`, and has not yet
    /// been offered the fingerprint: see `replication::digest`.
    pub replica_takes_digest: bool,
    /// Channels, patterns and shard channels this client is subscribed to.
    pub subscriptions: [usize; 3],
}

impl ClientInfo {
    /// Create a new ClientInfo. Returns the paired kill receiver for the
    /// connection loop to select on.
    pub fn new(id: u64, addr: String) -> (Self, KillRx) {
        let (kill_tx, kill_rx) = oneshot::channel();
        let activity = Activity::default();
        activity.touch();
        let info = Self {
            id,
            addr,
            laddr: String::new(),
            user: "default".to_string(),
            name: String::new(),
            db: 0,
            flags: "N".to_string(),
            connected_at: Instant::now(),
            activity: Arc::new(activity),
            query_buffer: Arc::default(),
            no_evict: false,
            asking: false,
            readonly: false,
            tracking: false,
            tracking_bcast: false,
            tracking_prefixes: Vec::new(),
            tracking_redirect: None,
            tracking_redirect_broken: false,
            kill_tx: Some(kill_tx),
            unblock_tx: None,
            blocked_with_timeout: false,
            push_tx: None,
            tracking_optin: false,
            tracking_optout: false,
            tracking_caching_next: None,
            tracking_noloop: false,
            lib_name: String::new(),
            lib_ver: String::new(),
            resp: 2,
            replica_listening_port: 0,
            replica_takes_digest: false,
            subscriptions: [0; 3],
        };
        (info, kill_rx)
    }

    pub fn kind(&self) -> ClientKind {
        if self.flags.contains('S') {
            ClientKind::Replica
        } else if self.flags.contains('M') {
            ClientKind::Master
        } else if self.subscriptions.iter().any(|n| *n > 0) {
            ClientKind::PubSub
        } else {
            ClientKind::Normal
        }
    }

    /// One line of `CLIENT LIST`, in Redis's layout. Query-buffer counters
    /// describe RESP bytes awaiting parsing and spare Vec capacity; other
    /// Redis-specific buffer and memory counters remain zero.
    pub fn to_list_line(&self) -> String {
        let age = self.connected_at.elapsed().as_secs();
        let idle = self.activity.idle_seconds();
        let (qbuf, qbuf_free) = self.query_buffer.values();
        let mut flags = String::new();
        if self.flags != "N" {
            flags.push_str(&self.flags);
        }
        if self.subscriptions.iter().any(|n| *n > 0) {
            flags.push('P');
        }
        if self.tracking {
            flags.push('t');
        }
        if flags.is_empty() {
            flags.push('N');
        }
        format!(
            "id={} addr={} laddr={} fd=0 name={} age={} idle={} flags={} db={} sub={} psub={} \
            ssub={} multi={} watch={} qbuf={} qbuf-free={} argv-mem=0 multi-mem=0 \
            rbs=0 rbp=0 obl=0 oll=0 omem=0 omem-shared=0 omem-unshared=0 tot-mem={} events=r cmd={} \
            user={} redir={} resp={} lib-name={} lib-ver={} io-thread=0 tot-net-in={} \
            tot-net-out={} tot-cmds={} read-events={} avg-pipeline-len-sum={} \
            avg-pipeline-len-cnt={}\n",
            self.id,
            self.addr,
            self.laddr,
            self.name,
            age,
            idle,
            flags,
            self.activity.db(),
            self.subscriptions[0],
            self.subscriptions[1],
            self.subscriptions[2],
            self.activity.tx().0,
            self.activity.tx().1,
            qbuf,
            qbuf_free,
            // What KiviDB measures of a client's memory: its query buffer.
            qbuf + qbuf_free,
            self.activity.last_cmd(),
            self.user,
            self.tracking_redirect.map_or(-1, |id| id as i64),
            self.resp,
            self.lib_name,
            self.lib_ver,
            self.activity.io(IO_NET_IN),
            self.activity.io(IO_NET_OUT),
            self.activity
                .io(IO_CMDS)
                .saturating_sub(self.activity.io(IO_RUNNING)),
            self.activity.io(IO_READS),
            self.activity.io(IO_PIPE_SUM),
            self.activity.io(IO_PIPE_CNT),
        )
    }
}

// ClientInfo is stored in an Arc<RwLock<HashMap>> so it needs to be Send.
// oneshot::Sender<()> is Send, so this is automatic — but we note it explicitly.

pub type ClientRegistry = Arc<ClientRegistryInner>;

pub struct ClientRegistryInner {
    clients: RwLock<AHashMap<u64, ClientInfo>>,
}

impl ClientRegistryInner {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(AHashMap::new()),
        }
    }

    pub fn register(&self, info: ClientInfo) {
        log_verbose!("Client id={} connected from {}", info.id, info.addr);
        self.clients.write().insert(info.id, info);
    }

    pub fn unregister(&self, id: u64) {
        let gone = self.clients.write().remove(&id);
        if gone.as_ref().is_some_and(|info| info.blocked_with_timeout) {
            IN_TIMEOUT_TABLE.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(info) = gone.as_ref() {
            log_verbose!(
                "Client id={} ({}{}) disconnected after {:.1}s",
                id,
                info.addr,
                if info.name.is_empty() {
                    String::new()
                } else {
                    format!(", name={}", info.name)
                },
                info.connected_at.elapsed().as_secs_f64()
            );
        }
        Self::no_longer_tracking(gone);
        // Per-connection state kept outside the registry goes with it.
        crate::commands::hash_import::forget_client(id);
    }

    /// Connected client count without cloning every `ClientInfo` (INFO used to
    /// call `list().len()`, which copied addr/name/last_cmd and the push
    /// channel handle on every scrape).
    pub fn len(&self) -> usize {
        self.clients.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.clients.read().is_empty()
    }

    /// The client has begun to wait in a blocking command (`Some`), or has
    /// stopped (`None`).
    pub fn set_blocked(
        &self,
        id: u64,
        unblock: Option<oneshot::Sender<crate::commands::blocking::Unblock>>,
        with_timeout: bool,
    ) {
        let blocked = unblock.is_some();
        if let Some(info) = self.clients.write().get_mut(&id) {
            info.unblock_tx = unblock;
            let counted = blocked && with_timeout;
            if counted != info.blocked_with_timeout {
                if counted {
                    IN_TIMEOUT_TABLE.fetch_add(1, Ordering::Relaxed);
                } else {
                    IN_TIMEOUT_TABLE.fetch_sub(1, Ordering::Relaxed);
                }
                info.blocked_with_timeout = counted;
            }
        }
        if blocked {
            crate::pause::went_on(id);
        }
    }

    /// `CLIENT UNBLOCK`: end the client's wait, if it is waiting.
    pub fn unblock(&self, id: u64, how: crate::commands::blocking::Unblock) -> bool {
        let sender = self
            .clients
            .write()
            .get_mut(&id)
            .and_then(|info| info.unblock_tx.take());
        sender.is_some_and(|tx| tx.send(how).is_ok())
    }

    /// `clients_in_timeout_table` (#158): clients blocked with a timeout.
    pub fn in_timeout_table() -> u64 {
        IN_TIMEOUT_TABLE.load(Ordering::Relaxed)
    }

    /// What the clients' query buffers hold, the replicas' apart:
    /// `(mem_clients_normal, mem_clients_slaves)` (#152).
    pub fn buffer_totals(&self) -> (u64, u64) {
        let (mut normal, mut replicas) = (0u64, 0u64);
        for info in self.clients.read().values() {
            let (len, free) = info.query_buffer.values();
            if info.flags.contains('S') {
                replicas += len + free;
            } else {
                normal += len + free;
            }
        }
        (normal, replicas)
    }

    pub fn register_push_tx(&self, id: u64, tx: mpsc::Sender<Vec<u8>>) {
        if let Some(info) = self.clients.write().get_mut(&id) {
            info.push_tx = Some(tx);
        }
    }

    /// `n` commands run by a script client `id` ran: see
    /// [`Activity::add_commands`].
    pub fn add_commands(&self, id: u64, n: u64) {
        if let Some(info) = self.clients.read().get(&id) {
            info.activity.add_commands(n);
        }
    }

    /// Send the kill signal to a client. Returns true if the client was found
    /// and the signal was sent, false if no such client exists or was already killed.
    pub fn kill(&self, id: u64) -> bool {
        let mut clients = self.clients.write();
        let should_remove = if let Some(info) = clients.get_mut(&id) {
            if let Some(tx) = info.kill_tx.take() {
                let _ = tx.send(());
                true
            } else {
                false
            }
        } else {
            false
        };
        if should_remove {
            clients.remove(&id);
        }
        should_remove
    }

    pub fn update(&self, id: u64, f: impl FnOnce(&mut ClientInfo)) {
        if let Some(info) = self.clients.write().get_mut(&id) {
            f(info);
        }
    }

    pub fn list(&self) -> Vec<ClientInfo> {
        // ClientInfo no longer derives Clone (kill_tx isn't Clone).
        // Build list lines directly instead of cloning.
        self.clients
            .read()
            .values()
            .map(|c| ClientInfo {
                id: c.id,
                addr: c.addr.clone(),
                laddr: c.laddr.clone(),
                user: c.user.clone(),
                name: c.name.clone(),
                db: c.db,
                flags: c.flags.clone(),
                connected_at: c.connected_at,
                activity: Arc::clone(&c.activity),
                query_buffer: Arc::clone(&c.query_buffer),
                no_evict: c.no_evict,
                asking: c.asking,
                readonly: c.readonly,
                tracking: c.tracking,
                tracking_bcast: c.tracking_bcast,
                tracking_prefixes: c.tracking_prefixes.clone(),
                tracking_redirect: c.tracking_redirect,
                tracking_redirect_broken: c.tracking_redirect_broken,
                push_tx: c.push_tx.clone(),
                kill_tx: None,
                unblock_tx: None,
                blocked_with_timeout: false,
                tracking_optin: c.tracking_optin,
                tracking_optout: c.tracking_optout,
                tracking_caching_next: c.tracking_caching_next,
                tracking_noloop: c.tracking_noloop,
                lib_name: c.lib_name.clone(),
                lib_ver: c.lib_ver.clone(),
                resp: c.resp,
                replica_listening_port: c.replica_listening_port,
                replica_takes_digest: c.replica_takes_digest,
                subscriptions: c.subscriptions,
            })
            .collect()
    }

    pub fn get(&self, id: u64) -> Option<ClientInfo> {
        self.clients.read().get(&id).map(|c| ClientInfo {
            id: c.id,
            addr: c.addr.clone(),
            laddr: c.laddr.clone(),
            user: c.user.clone(),
            name: c.name.clone(),
            db: c.db,
            flags: c.flags.clone(),
            connected_at: c.connected_at,
            activity: Arc::clone(&c.activity),
            query_buffer: Arc::clone(&c.query_buffer),
            no_evict: c.no_evict,
            asking: c.asking,
            readonly: c.readonly,
            kill_tx: None,
            unblock_tx: None,
            blocked_with_timeout: false,
            tracking: c.tracking,
            tracking_bcast: c.tracking_bcast,
            tracking_prefixes: c.tracking_prefixes.clone(),
            tracking_redirect: c.tracking_redirect,
            tracking_redirect_broken: c.tracking_redirect_broken,
            push_tx: c.push_tx.clone(),
            tracking_optin: c.tracking_optin,
            tracking_optout: c.tracking_optout,
            tracking_caching_next: c.tracking_caching_next,
            tracking_noloop: c.tracking_noloop,
            lib_name: c.lib_name.clone(),
            lib_ver: c.lib_ver.clone(),
            resp: c.resp,
            replica_listening_port: c.replica_listening_port,
            replica_takes_digest: c.replica_takes_digest,
            subscriptions: c.subscriptions,
        })
    }

    /// Whether the client has issued `READONLY` (cluster replica reads).
    #[inline]
    pub fn get_readonly(&self, id: u64) -> bool {
        self.clients
            .read()
            .get(&id)
            .map(|c| c.readonly)
            .unwrap_or(false)
    }

    #[inline]
    pub fn get_resp(&self, id: u64) -> u8 {
        self.clients.read().get(&id).map(|c| c.resp).unwrap_or(2)
    }

    /// Hot-path accessor for the cluster routing layer. Returns just the
    /// asking flag without cloning the entire ClientInfo. Called on every
    /// command, so it must avoid allocation.
    pub fn get_asking(&self, id: u64) -> bool {
        self.clients
            .read()
            .get(&id)
            .map(|c| c.asking)
            .unwrap_or(false)
    }

    /// `CLIENT LIST`: the lines of the clients `filter` matches, oldest
    /// connection first.
    pub fn list_lines(&self, filter: &ClientFilter, me: u64) -> String {
        let clients = self.clients.read();
        let mut matching: Vec<&ClientInfo> = clients
            .values()
            .filter(|client| filter.matches(client, me))
            .collect();
        matching.sort_by_key(|client| client.id);
        matching
            .iter()
            .map(|client| client.to_list_line())
            .collect()
    }

    /// `CLIENT KILL`: close the clients `filter` matches. Returns how many,
    /// and whether the client that asked is one of them — its connection
    /// closes once it has had its reply.
    /// A client has left the registry: if it had tracking on, there is one
    /// tracking client fewer. Counted here, where the client goes, and not
    /// at the end of its connection's loop — which a connection that ends
    /// on an error, or is killed, does not reach with the client still
    /// listed.
    fn no_longer_tracking(gone: Option<ClientInfo>) {
        if gone.is_some_and(|client| client.tracking) {
            crate::tracking::TRACKING_CLIENT_COUNT.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn kill_matching(&self, filter: &ClientFilter, me: u64) -> (usize, bool) {
        let mut clients = self.clients.write();
        let ids: Vec<u64> = clients
            .values()
            .filter(|client| filter.matches(client, me))
            .map(|client| client.id)
            .collect();
        let mut killed = 0;
        for id in &ids {
            if let Some(client) = clients.get_mut(id)
                && let Some(tx) = client.kill_tx.take()
            {
                // The one asking still has a reply to be sent: its
                // connection closes once that is written.
                if *id == me {
                    client
                        .activity
                        .closing_after_reply
                        .store(true, Ordering::Relaxed);
                }
                let _ = tx.send(());
                killed += 1;
            }
            // And it stays listed until its connection has closed.
            if *id != me {
                Self::no_longer_tracking(clients.remove(id));
            }
        }
        (killed, ids.contains(&me))
    }

    /// The clients with tracking on.
    pub fn tracking_ids(&self) -> Vec<u64> {
        self.clients
            .read()
            .values()
            .filter(|client| client.tracking)
            .map(|client| client.id)
            .collect()
    }

    pub fn find_by_addr(&self, addr: &str) -> Option<u64> {
        self.clients
            .read()
            .values()
            .find(|c| c.addr == addr)
            .map(|c| c.id)
    }
}

pub fn new_client_registry() -> ClientRegistry {
    Arc::new(ClientRegistryInner::new())
}

impl Default for ClientRegistryInner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn after(commands: &[&[&str]]) -> String {
        let activity = Activity::default();
        for command in commands {
            let cmd: Vec<&[u8]> = command.iter().map(|word| word.as_bytes()).collect();
            activity.record(&cmd, 3);
        }
        assert_eq!(activity.db(), 3);
        activity.last_cmd()
    }

    #[test]
    fn the_last_command_is_the_one_shown() {
        assert_eq!(Activity::default().last_cmd(), "NULL");
        assert_eq!(after(&[&["GET", "key"]]), "get");
        assert_eq!(after(&[&["Client", "LIST"]]), "client|list");
        assert_eq!(after(&[&["PING"]]), "ping");
    }

    /// A shorter name after a longer one: nothing of the longer one is left
    /// showing, though its later bytes are still where they were written.
    #[test]
    fn a_shorter_command_after_a_longer_one() {
        assert_eq!(
            after(&[
                &["GEORADIUSBYMEMBER_RO", "a-long-key-name-here"],
                &["GET", "k"]
            ]),
            "get"
        );
        assert_eq!(
            after(&[&["CLIENT", "NO-EVICT-AND-MORE"], &["CONFIG", "GET"]]),
            "config|get"
        );
        assert_eq!(after(&[&["CLIENT", "TRACKINGINFO"], &["CLIENT"]]), "client");
        // Names are kept to their first 24 bytes; a subcommand is of 16 at
        // most.
        assert_eq!(
            after(&[&["ABCDEFGHIJKLMNOPQRSTUVWXYZ0123"]]),
            "abcdefghijklmnopqrstuvwx"
        );
        assert_eq!(
            after(&[&["CLIENT", "ABCDEFGHIJKLMNOP"]]),
            "client|abcdefghijklmnop"
        );
        assert_eq!(after(&[&["CLIENT", "ABCDEFGHIJKLMNOPQ"]]), "client");
    }

    /// A client that has just been created is idle only for the instant since.
    #[test]
    fn a_new_client_has_hardly_been_idle() {
        let activity = Activity::default();
        activity.touch();
        assert!(activity.idle_seconds() <= 1);
    }

    /// Idle time is the gap between the last interaction and now, and it
    /// grows as that gap widens.
    #[test]
    fn idle_time_grows_with_the_clock() {
        let activity = Activity::default();
        activity.touch();
        let at = activity.interaction_at();
        assert_eq!(activity.idle_seconds_at(at), 0);
        assert_eq!(activity.idle_seconds_at(at + 5), 5);
        assert_eq!(activity.idle_seconds_at(at + 30), 30);
    }

    /// An interaction from now clears the idle time.
    #[test]
    fn interacting_resets_the_idle_clock() {
        let activity = Activity::default();
        activity.touch();
        let at = activity.interaction_at();
        assert_eq!(activity.idle_seconds_at(at + 42), 42);
        // A later interaction moves the recorded point to that later time.
        activity.last_interaction.store(at + 42, Ordering::Relaxed);
        assert_eq!(activity.idle_seconds_at(at + 42), 0);
        assert_eq!(activity.idle_seconds_at(at + 45), 3);
    }

    /// Reading the idle time does not itself count as an interaction, so a
    /// `CLIENT LIST` of a client leaves its timer running.
    #[test]
    fn reading_the_idle_time_does_not_reset_it() {
        let activity = Activity::default();
        activity.touch();
        let at = activity.interaction_at();
        // Several reads at the same clock reading keep the same interaction
        // point, so the reported idle never falls back to zero by being read.
        assert_eq!(activity.interaction_at(), at);
        assert_eq!(activity.idle_seconds_at(at + 7), 7);
        assert_eq!(activity.interaction_at(), at);
        assert_eq!(activity.idle_seconds_at(at + 7), 7);
        assert_eq!(activity.interaction_at(), at);
    }

    /// A socket read is the interaction: `touch` restarts the idle clock.
    /// `record` — the per-command bookkeeping — must not, so a pipeline of
    /// commands that arrived in one read does not keep pushing the clock.
    #[test]
    fn a_socket_read_resets_the_idle_clock_but_a_command_does_not() {
        let activity = Activity::default();
        activity.touch();
        let read_at = activity.interaction_at();
        assert_eq!(activity.idle_seconds_at(read_at + 20), 20);

        // Commands arrive from one read: recording them leaves the clock alone.
        activity.record(&[b"PING"], 0);
        activity.record(&[b"GET", b"k"], 0);
        assert_eq!(activity.interaction_at(), read_at);
        assert_eq!(activity.idle_seconds_at(read_at + 20), 20);

        // The next socket read is the interaction, and it restarts the clock.
        activity
            .last_interaction
            .store(read_at + 20, Ordering::Relaxed);
        assert_eq!(activity.idle_seconds_at(read_at + 20), 0);
        assert_eq!(activity.idle_seconds_at(read_at + 23), 3);
    }
}
