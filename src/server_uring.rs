use crate::commands;
use crate::pubsub::PubSubHub;
use crate::resp;
use crate::store;
use crate::vector;
use crate::watch::WatchNotifier;
use commands::ctx::Ctx;
use parking_lot::RwLock;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use tokio_uring::buf::BoundedBuf;
use tokio_uring::net::TcpStream;

/// Called when a replica sends PSYNC on the io_uring path.
///
/// The outbound (hub → replica) and inbound (REPLCONF ACK) directions run as two
/// concurrent, event-driven consumers sharing one `Rc<TcpStream>` (io_uring's read/write
/// take `&self`, so this is safe within tokio_uring's single-threaded runtime). Each side
/// issues exactly one `read`/`recv` at a time and only starts the next after the previous
/// one actually completes — never on a timer. An earlier version re-issued a fresh 1ms-
/// timeout `read()` on every tick to poll for ACKs; because tokio_uring cannot truly cancel
/// an in-flight op (the buffer must be held until the kernel's completion arrives — see
/// tokio-uring's DESIGN.md), that produced ~1000 abandoned, never-completing reads per
/// second per idle replica, each pinning its buffer forever: unbounded master memory growth
/// with a replica attached, independent of write traffic.
#[allow(clippy::too_many_arguments)]
pub async fn handle_replica_connection_uring(
    stream: Rc<TcpStream>,
    store: store::Store,
    index_store: vector::IndexStore,
    hub_tx: crate::replication::HubTx,
    repl_state: Arc<crate::replication::ReplState>,
    peer_addr: String,
    legacy_sync: bool,
    // The port the replica announced with `REPLCONF listening-port`, if any.
    listening_port: Option<u16>,
    // What the replica asked to continue from (`PSYNC <replid> <offset>`).
    psync: Option<crate::replication::PsyncRequest>,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::sync::mpsc;

    // ── 1. The stream queue. Unbounded: bounded queues fill before forwarding runs.
    let (replica_tx, replica_rx) = mpsc::unbounded_channel::<crate::replication::ReplPayload>();
    let replica_id = crate::replication::next_replica_id();

    // Shared with the hub: it increments this on enqueue, the forwarder below
    // decrements it after each successful flush. Lets the hub evict us if we fall
    // far enough behind instead of queueing an unbounded backlog — see
    // `replication::mod::start_hub`.
    let pending_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // ── 2. A replica that only lost its connection is sent what it missed.
    // Failing that, take the cut: register with the hub and serialise the
    // dataset at one instant, so every write is in exactly one of {payload,
    // stream}. See `replication::begin_full_sync`.
    let (peer_ip, peer_port) =
        crate::replication::replica_client::replica_endpoint(&peer_addr, listening_port);
    let endpoint = format!("{peer_ip}:{peer_port}");
    let registration = crate::replication::ReplicaRegistration {
        replica_id,
        addr: peer_addr.clone(),
        peer_ip,
        peer_port,
        tx: replica_tx,
        pending_bytes: Arc::clone(&pending_bytes),
    };
    let full_sync = match crate::replication::request_continuation(
        &hub_tx,
        &repl_state,
        psync.as_ref(),
        registration,
    )
    .await
    {
        Ok(()) => None,
        Err(registration) => {
            match tokio::task::spawn_blocking({
                let store = Arc::clone(&store);
                let repl_state = Arc::clone(&repl_state);
                let hub_tx = hub_tx.clone();
                move || {
                    crate::replication::begin_full_sync(
                        &store,
                        &index_store,
                        &repl_state,
                        &hub_tx,
                        registration,
                    )
                }
            })
            .await
            {
                Ok(Ok(v)) => Some(v),
                Ok(Err(e)) => {
                    let _ = hub_tx.send(crate::replication::HubMsg::Deregister(replica_id));
                    return Err(e.into());
                }
                Err(e) => {
                    let _ = hub_tx.send(crate::replication::HubMsg::Deregister(replica_id));
                    return Err(e.into());
                }
            }
        }
    };

    // ── 3. `+CONTINUE`, or `+FULLRESYNC` (omitted for legacy `SYNC`) ──────
    let greeting = match &full_sync {
        None => Some(format!("+CONTINUE {}\r\n", repl_state.repl_id())),
        Some(sync) if !legacy_sync => Some(sync.announcement(&repl_state.repl_id())),
        Some(_) => None,
    };
    if let Some(greeting) = greeting {
        let (res, _) = stream.write_all(greeting.into_bytes()).await;
        if let Err(e) = res {
            let _ = hub_tx.send(crate::replication::HubMsg::Deregister(replica_id));
            return Err(e.into());
        }
    }

    // ── 4. Spawn the outbound forwarder on its own Rc<TcpStream> handle. Gated on
    // `rdb_enqueued_rx` so propagated writes never interleave with the RDB bytes below.
    // Blocks on `recv()` (event-driven) — no polling, no re-issued ops.
    let (rdb_enqueued_tx, rdb_enqueued_rx) = tokio::sync::oneshot::channel::<()>();
    let stream_fwd = Rc::clone(&stream);
    let forward_task = tokio_uring::spawn(async move {
        if rdb_enqueued_rx.await.is_err() {
            return;
        }
        let mut replica_rx = replica_rx;
        // Fewer replica-socket writes / wakeups than one write per propagated command.
        const BATCH_MAX_BYTES: usize = 4 * 1024 * 1024;
        loop {
            let first = match replica_rx.recv().await {
                Some(p) => p,
                None => break,
            };
            let mut batch = Vec::with_capacity(first.len().min(BATCH_MAX_BYTES));
            batch.extend_from_slice(first.as_ref());
            loop {
                if batch.len() >= BATCH_MAX_BYTES {
                    break;
                }
                match replica_rx.try_recv() {
                    Ok(p) => batch.extend_from_slice(p.as_ref()),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        pending_bytes
                            .fetch_sub(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        let _ = stream_fwd.write_all(batch).await;
                        return;
                    }
                }
            }
            pending_bytes.fetch_sub(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
            if stream_fwd.write_all(batch).await.0.is_err() {
                break;
            }
        }
    });

    // ── 5. Send the RDB dump, if there is one, then release the forwarder ─
    if let Some(sync) = full_sync {
        if let Some(replica) = repl_state.replica(replica_id) {
            replica.set_state(crate::replication::ReplicaState::SendBulk);
        }
        let header = format!("${}\r\n", sync.payload.len()).into_bytes();
        let (res, _) = stream.write_all(header).await;
        if let Err(e) = res {
            let _ = hub_tx.send(crate::replication::HubMsg::Deregister(replica_id));
            drop(rdb_enqueued_tx);
            return Err(e.into());
        }
        let (res, _) = stream.write_all(sync.payload).await;
        if let Err(e) = res {
            let _ = hub_tx.send(crate::replication::HubMsg::Deregister(replica_id));
            drop(rdb_enqueued_tx);
            return Err(e.into());
        }
        log_info!("Replication: uring: sent full RDB; streaming queued writes from hub");
    }

    let _ = rdb_enqueued_tx.send(());

    // ── 6. Inbound ACK reader, racing the forwarder ───────────────────────
    // Exactly one `read()` in flight at a time, re-issued only after it actually
    // completes. Whichever side ends first (peer gone, write failure, hub tx dropped)
    // wins the select and tears the whole connection down; the loser's in-flight op
    // (at most one, only at teardown) is cancelled once — not thousands of times a
    // second the way the old timeout-poll loop cancelled its reads.
    let ack_reader = async {
        let mut ack_buf = Vec::with_capacity(128);
        let mut ack_parse_start = 0usize;
        let mut read_buf = vec![0u8; 4096];
        loop {
            let (res, buf) = stream.read(read_buf).await;
            read_buf = buf;
            match res {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    ack_buf.extend_from_slice(&read_buf[..n]);
                    let mut parse_offset = ack_parse_start;
                    while let Some((consumed, cmd)) =
                        crate::resp::parse::parse_resp_command(&ack_buf[parse_offset..])
                    {
                        if cmd.len() == 3
                            && cmd[0].eq_ignore_ascii_case(b"REPLCONF")
                            && cmd[1].eq_ignore_ascii_case(b"ACK")
                            && let Ok(s) = std::str::from_utf8(cmd[2])
                            && let Ok(offset) = s.parse::<u64>()
                        {
                            let _ = hub_tx
                                .send(crate::replication::HubMsg::AckOffset { replica_id, offset });
                        }
                        parse_offset += consumed;
                    }
                    ack_parse_start = parse_offset;
                    if ack_buf.len() - ack_parse_start
                        > crate::replication::replica_client::ACK_PENDING_MAX
                    {
                        log_warn!(
                            "Replication: replica {replica_id} sent what is not REPLCONF ACK; disconnecting it"
                        );
                        break;
                    }
                    if ack_parse_start > 0 && ack_parse_start > ack_buf.len() / 2 {
                        ack_buf.drain(0..ack_parse_start);
                        ack_parse_start = 0;
                    }
                }
            }
        }
    };

    tokio::select! {
        _ = ack_reader => {}
        _ = forward_task => {}
        // A replica that has gone silent: see `ReplState::replica_timed_out`.
        _ = repl_state.replica_timed_out(replica_id) => {
            log_warn!(
                "Replication: disconnecting timedout replica {endpoint}: no REPLCONF ACK for {} s (repl-timeout)",
                repl_state.repl_timeout().as_secs()
            );
        }
    }

    let _ = hub_tx.send(crate::replication::HubMsg::Deregister(replica_id));
    log_info!("Replication: uring: replica disconnected");
    Ok(())
}

#[inline(always)]
fn now_us() -> u64 {
    // For measuring how long a command took, so: a clock that does not go
    // backwards. This was the wall clock, and the difference of two readings
    // of it — a panic in a debug build, and in a release build a command
    // logged as having taken five hundred thousand years, whenever the
    // system clock was set back between them.
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_micros() as u64
}

#[inline(always)]
fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Check if a command name (raw bytes, any case) requires connection-level
/// handling that can't go through the normal execute_command path.
/// Uses a first-byte filter so we never allocate a String on the fast path.
#[inline(always)]
/// Await a parked blocking command on the io_uring path.
///
/// Mirrors `server::park_blocking`; separate only because the write differs —
/// `tokio_uring`'s `write_all` takes ownership of the buffer and hands it back,
/// and the fd is shared with the push forwarder so the write must take
/// `write_guard`.
///
/// `Ok(())` means the delivery is stored and the caller should re-dispatch the
/// same command; `Err(reply)` means reply with this instead and move on.
#[allow(clippy::too_many_arguments)]
async fn park_blocking_uring(
    mut wait: crate::commands::blocking::BlockingWait,
    slot: &mut crate::commands::blocking::BlockingSlot,
    stream: &Rc<TcpStream>,
    write_guard: &Rc<tokio::sync::Mutex<()>>,
    out_buf: &mut Vec<u8>,
    subscriptions: Option<&crate::pubsub::Subscriptions>,
    client: (&crate::client_registry::ClientRegistry, u64),
    interrupted: &mut crate::commands::blocking::Interrupted,
) -> Result<(), Vec<u8>> {
    if slot.in_resume() && !wait.may_repark() {
        // A resumed handler parked again — it forgot its resume check. Reply as
        // if the wait timed out rather than looping forever.
        slot.clear_resume();
        return Err(crate::resp::write::resp_null_array());
    }

    // Flush replies to anything pipelined ahead of this command before going to
    // sleep, so a client that sent `SET a 1` and `BLPOP k 0` in one packet is
    // not left waiting on the `+OK`.
    if !out_buf.is_empty() {
        let payload = std::mem::take(out_buf);
        crate::stats::TOTAL_NET_OUTPUT_BYTES
            .fetch_add(payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
        crate::stats::record_write();
        let _w = write_guard.lock().await;
        let (res, mut returned) = stream.write_all(payload).await;
        drop(_w);
        returned.clear();
        *out_buf = returned;
        if res.is_err() {
            // Socket is gone — don't park on a dead connection.
            return Err(Vec::new());
        }
    }
    // The replies to SUBSCRIBE are out: messages may follow them.
    if let Some(subs) = subscriptions {
        subs.flushed();
    }

    let block_state = wait.take_state();
    let (client_registry, client_id) = client;
    let (unblock_tx, mut unblock_rx) = tokio::sync::oneshot::channel();
    client_registry.set_blocked(client_id, Some(unblock_tx), wait.has_timeout());
    let fd = std::os::fd::AsRawFd::as_raw_fd(&**stream);
    let delivery = {
        let waiting = wait.wait();
        tokio::pin!(waiting);
        let mut unblockable = true;
        let mut look = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                delivery = &mut waiting => break delivery,
                // Whether the client is still there. A read cannot be raced
                // against the wait on this loop — a submitted read cannot be
                // taken back — so the socket is looked at instead, ten times
                // a second: a client that has gone (or been killed, which
                // shuts the socket down) must not be left in line for an
                // element nobody will receive.
                _ = look.tick() => {
                    if peer_has_gone(fd) {
                        interrupted.gone = true;
                        break None;
                    }
                }
                how = &mut unblock_rx, if unblockable => match how {
                    Ok(how) => {
                        interrupted.unblocked_with_error =
                            how == crate::commands::blocking::Unblock::Error;
                        break None;
                    }
                    Err(_) => unblockable = false,
                },
            }
        }
    };
    client_registry.set_blocked(client_id, None, false);
    // However it ended, the command is dispatched again: with what it was
    // waiting for, or as if its time had run out — which is also how it
    // leaves the line it was standing in.
    slot.set_pending(delivery, block_state);
    Ok(())
}

/// Whether the other end of `fd` has closed, or the socket has been shut
/// down. Reads nothing: what the client has sent stays where it is.
fn peer_has_gone(fd: std::os::fd::RawFd) -> bool {
    let mut byte = 0u8;
    // SAFETY: a one-byte buffer that outlives the call, on a descriptor the
    // caller holds open; MSG_PEEK leaves the data in place and MSG_DONTWAIT
    // returns at once.
    let n = unsafe {
        libc::recv(
            fd,
            &mut byte as *mut u8 as *mut libc::c_void,
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    match n {
        0 => true,
        n if n > 0 => false,
        _ => !matches!(
            std::io::Error::last_os_error().kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
        ),
    }
}

fn needs_slow_path(cmd0: &[u8]) -> bool {
    let first = cmd0.first().copied().unwrap_or(0) | 0x20; // to lowercase
    // Only commands starting with these letters can possibly need interception.
    // s: SUBSCRIBE, SSUBSCRIBE
    // p: PSUBSCRIBE, PSYNC
    // m: MULTI
    // d: DISCARD
    // w: WATCH
    // u: UNWATCH
    // e: EXEC
    // a: AUTH
    if !matches!(
        first,
        b's' | b'p' | b'm' | b'd' | b'w' | b'u' | b'e' | b'a' | b'h' | b'r'
    ) {
        return false;
    }
    // Second filter: byte-wise case-insensitive comparison (no UTF-8 validation).
    ascii_eq_ignore_case(cmd0, b"SUBSCRIBE")
        || ascii_eq_ignore_case(cmd0, b"PSUBSCRIBE")
        || ascii_eq_ignore_case(cmd0, b"SSUBSCRIBE")
        // See the matching note in server.rs: RESP3 clients handle the
        // UNSUBSCRIBE family inline on the slow path instead of in the RESP2
        // subscriber loop. Length-compares first, so hot commands are unaffected.
        || ascii_eq_ignore_case(cmd0, b"UNSUBSCRIBE")
        || ascii_eq_ignore_case(cmd0, b"PUNSUBSCRIBE")
        || ascii_eq_ignore_case(cmd0, b"SUNSUBSCRIBE")
        || ascii_eq_ignore_case(cmd0, b"PSYNC")
        || ascii_eq_ignore_case(cmd0, b"SYNC")
        || ascii_eq_ignore_case(cmd0, b"MULTI")
        || ascii_eq_ignore_case(cmd0, b"DISCARD")
        || ascii_eq_ignore_case(cmd0, b"WATCH")
        || ascii_eq_ignore_case(cmd0, b"UNWATCH")
        || ascii_eq_ignore_case(cmd0, b"EXEC")
        || ascii_eq_ignore_case(cmd0, b"AUTH")
        || ascii_eq_ignore_case(cmd0, b"RESET")
        || ascii_eq_ignore_case(cmd0, b"HELLO")
        || ascii_eq_ignore_case(cmd0, b"WAITAOF")
        || ascii_eq_ignore_case(cmd0, b"WAIT")
        || ascii_eq_ignore_case(cmd0, b"MONITOR")
}

/// Eligibility check for MULTI/EXEC's lock-held fast-apply path (see the EXEC arm in
/// `handle_client`). Returns the queued `(key, value)` pairs only when every queued command
/// is an unconditional `SET key value` (exactly 3 args) — the shape a WATCH+CAS retry loop
/// produces — there's at least one WATCHed key (nothing to protect otherwise), and neither
/// MONITOR nor CLIENT TRACKING is active (the fast path skips those side effects entirely
/// rather than risk getting them subtly wrong, so it declines instead when they matter).
/// Anything else falls back to the existing per-command dispatch loop, unchanged.
fn exec_fast_apply_sets(
    watched_keys_empty: bool,
    queue: &[Vec<Vec<u8>>],
) -> Option<Vec<(String, Vec<u8>)>> {
    if watched_keys_empty
        || crate::monitor::MONITOR_COUNT.load(std::sync::atomic::Ordering::Relaxed) > 0
        || crate::tracking::TRACKING_CLIENT_COUNT.load(std::sync::atomic::Ordering::Relaxed) > 0
    {
        return None;
    }
    queue
        .iter()
        .map(|cmd| {
            if cmd.len() == 3 && cmd[0].eq_ignore_ascii_case(b"SET") {
                std::str::from_utf8(&cmd[1])
                    .ok()
                    .map(|k| (k.to_string(), cmd[2].clone()))
            } else {
                None
            }
        })
        .collect()
}

/// A connection's buffer that has grown past this is given back once it is
/// empty, rather than kept.
const BIG_BUFFER: usize = 256 * 1024;

/// How much room a read is given at a time. Like Redis's `PROTO_IOBUF_LEN`,
/// a fresh read asks for a full 32 KiB so a pipelined batch or a large bulk
/// arrives in as few syscalls as possible; the buffer is only ever reserved
/// in these units, so this is also the floor a reclaimed buffer returns to.
const READ_BUFFER: usize = 32 * 1024;
/// A read is only submitted when the buffer has at least this much spare
/// capacity left. Below it, another `READ_BUFFER`-sized chunk is reserved
/// first, so a read never lands in a sliver of free space.
const READ_MIN: usize = 8 * 1024;

/// A connection's subscriptions, made at its first SUBSCRIBE, with the task
/// that writes its messages. On this thread: the stream is not `Send`. It
/// shares the fd with the connection loop and the push task, hence the guard.
fn open_subscriptions(
    pubsub: &PubSubHub,
    client_registry: &crate::client_registry::ClientRegistry,
    client_id: u64,
    stream: &Rc<TcpStream>,
    write_guard: &Rc<tokio::sync::Mutex<()>>,
) -> crate::pubsub::Subscriptions {
    let (mut subs, mut outbox) =
        crate::pubsub::connect(pubsub, std::sync::Arc::clone(client_registry), client_id);
    let stream = Rc::clone(stream);
    let write_guard = Rc::clone(write_guard);
    let registry = std::sync::Arc::clone(client_registry);
    subs.forwarded_by(tokio_uring::spawn(async move {
        while let Some(batch) = outbox.next().await {
            let _w = write_guard.lock().await;
            // With the guard held: see `Batch::encode`.
            let frames = batch.encode(registry.get_resp(client_id));
            if frames.is_empty() {
                continue;
            }
            crate::stats::TOTAL_NET_OUTPUT_BYTES
                .fetch_add(frames.len() as u64, std::sync::atomic::Ordering::Relaxed);
            crate::stats::record_write();
            let (res, _) = stream.write_all(frames).await;
            if res.is_err() {
                break;
            }
        }
    }));
    subs
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_client(
    stream: TcpStream,
    store: store::Store,
    lastsave: Arc<RwLock<i64>>,
    aof_tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    _aof_enabled: bool,
    config: Arc<crate::config::Config>,
    started_at: Arc<Instant>,
    index_store: vector::IndexStore,
    dict_store: vector::DictStore,
    wait_table: Arc<crate::wait_table::WaitTable>,
    ts_index: store::SharedTsIndex,
    pubsub: PubSubHub,
    hub_tx: crate::replication::HubTx,
    repl_state: Arc<crate::replication::ReplState>,
    watch_registry: crate::watch::WatchRegistry,
    client_registry: crate::client_registry::ClientRegistry,
    peer_addr: String,
    acl_store: crate::acl::AclStore,
    acl_log: crate::acl::SharedAclLog,
    slowlog: crate::slowlog::SharedSlowLog,
    latency: crate::latency::SharedLatencyTracker,
    compaction_tx: crate::vector::CompactionTx,
    cluster_state: Arc<crate::cluster::ClusterState>,
    monitor_registry: crate::monitor::SharedMonitorRegistry,
    tracking_table: crate::tracking::TrackingTable,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;
    if let Some(denied) = crate::protected_mode::refusal(&acl_store, &peer_addr) {
        let _ = stream.write_all(denied.to_vec()).await;
        return Ok(());
    }

    // ── Client registry bookkeeping ──────────────────────────────────────
    let client_id = crate::client_registry::next_client_id();
    let (mut client_info, mut kill_rx) =
        crate::client_registry::ClientInfo::new(client_id, peer_addr.clone());
    client_info.laddr = format!("{}:{}", config.bind, config.port);
    let activity = Arc::clone(&client_info.activity);
    let query_buffer = Arc::clone(&client_info.query_buffer);
    // `CLIENT REPLY`.
    let mut reply_gate = crate::client_registry::ReplyGate::default();
    client_registry.register(client_info);

    // However the connection ends — the client leaving, an error, a kill —
    // what was kept for it goes: the keys it was tracking, and its entry in
    // the registry (which is where a tracking client stops being counted).
    struct ClientGuard {
        id: u64,
        registry: crate::client_registry::ClientRegistry,
        tracking: crate::tracking::TrackingTable,
    }
    impl Drop for ClientGuard {
        fn drop(&mut self) {
            self.tracking.unregister_client(self.id);
            self.registry.unregister(self.id);
        }
    }
    let _guard = ClientGuard {
        id: client_id,
        registry: Arc::clone(&client_registry),
        tracking: Arc::clone(&tracking_table),
    };

    // ── Connection state ─────────────────────────────────────────────────
    let mut parse_buf: Vec<u8> = Vec::with_capacity(READ_BUFFER);
    let mut private_query_buffer = false;
    let mut last_query_buffer_activity = Instant::now();
    let query_buffer_idle_resize_after =
        std::time::Duration::from_millis(config.query_buffer_idle_resize_after_ms);
    let mut query_buffer_peak = 0usize;
    query_buffer.update(0, 0, private_query_buffer);
    let mut parse_start = 0usize;
    // How far the request still arriving has been read.
    let mut progress = resp::parse::Progress::default();
    let mut out_buf: Vec<u8> = Vec::with_capacity(65536);
    let mut spare_buf: Vec<u8> = Vec::with_capacity(65536);
    // Push channel for out-of-band invalidation messages (CLIENT TRACKING).
    let (push_tx, mut push_rx) =
        tokio::sync::mpsc::channel::<Vec<u8>>(crate::client_registry::PUSH_QUEUE_FRAMES);
    client_registry.register_push_tx(client_id, push_tx.clone());

    // Out-of-band pushes (CLIENT TRACKING invalidations, RESP3 pub/sub
    // deliveries) are written by their own task rather than being drained by
    // the read loop.
    //
    // Draining them in the read loop — as this path used to — only flushes
    // after the client sends its *next* command. For an invalidation that is a
    // latency bug; for a pub/sub subscriber it is fatal, because an idle
    // subscriber never sends anything and so never receives anything.
    //
    // The obvious alternative, `select!`-ing over `stream.read()`, is not safe
    // here: tokio_uring cannot cancel an in-flight op, so a losing read arm
    // leaks its buffer until the kernel completes it. That is precisely the bug
    // documented on `handle_replica_connection_uring` above (unbounded memory
    // growth per idle connection). A separate task sidesteps it entirely — the
    // read is never raced.
    //
    // Both writers therefore share the fd, so writes are serialised through
    // `write_guard`. Without it two concurrent `write_all` calls can interleave
    // mid-frame (write_all loops internally on partial writes) and corrupt the
    // stream. Uncontended it is a plain atomic swap, and it is taken once per
    // flush — not per command.
    let stream = Rc::new(stream);
    let write_guard: Rc<tokio::sync::Mutex<()>> = Rc::new(tokio::sync::Mutex::new(()));
    let push_task = {
        let stream = Rc::clone(&stream);
        let write_guard = Rc::clone(&write_guard);
        tokio_uring::spawn(async move {
            while let Some(msg) = push_rx.recv().await {
                let _w = write_guard.lock().await;
                crate::stats::TOTAL_NET_OUTPUT_BYTES
                    .fetch_add(msg.len() as u64, std::sync::atomic::Ordering::Relaxed);
                crate::stats::record_write();
                let (res, _) = stream.write_all(msg).await;
                if res.is_err() {
                    break;
                }
            }
        })
    };
    // Stops the forwarder touching the fd once this connection hands off to a
    // replica/monitor/subscriber loop, or ends.
    struct PushTaskGuard(tokio::task::JoinHandle<()>);
    impl Drop for PushTaskGuard {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _push_task = PushTaskGuard(push_task);

    // ── CLIENT KILL ─────────────────────────────────────────────────────────
    //
    // The tokio loop races `kill_rx` against the socket read in a `select!`.
    // That is not available here: tokio_uring cannot cancel an in-flight op, so
    // a losing read arm would leak its buffer until the kernel completes it.
    // Checking `kill_rx` before each read — which is all this path used to do —
    // only fires once the client sends something, so killing an *idle* client
    // did nothing at all and it kept answering PING.
    //
    // Shutting the socket down instead makes the in-flight read complete on its
    // own, which needs no cancellation: the loop then sees `Ok(0)` and exits by
    // the ordinary close path. Redis sends no reply to a killed client either.
    //
    // Except to one that killed itself (`CLIENT KILL … SKIPME no`), which is
    // owed the reply to that command: for it only the reading side is shut,
    // so the reply is written and the read after it is the one that ends.
    let kill_task = {
        let kill_fd = std::os::fd::AsRawFd::as_raw_fd(&*stream);
        let activity = Arc::clone(&activity);
        tokio_uring::spawn(async move {
            // Resolves with Err when the sender is dropped at deregistration,
            // i.e. the connection ended on its own — nothing to do then.
            if (&mut kill_rx).await.is_ok() {
                let how = if activity.closes_after_reply() {
                    libc::SHUT_RD
                } else {
                    libc::SHUT_RDWR
                };
                // SAFETY: `kill_fd` belongs to `stream`, which is kept alive by
                // the `Rc` held in this function for as long as this task can
                // run — the guard below aborts it before that `Rc` is dropped.
                unsafe {
                    libc::shutdown(kill_fd, how);
                }
            }
        })
    };
    struct KillTaskGuard(tokio::task::JoinHandle<()>);
    impl Drop for KillTaskGuard {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _kill_task = KillTaskGuard(kill_task);

    // This connection's subscriptions, while it has any. `None` otherwise, so
    // that a connection that never uses pub/sub allocates nothing for it.
    let mut subscriptions: Option<crate::pubsub::Subscriptions> = None;
    // How a blocking command's wait was cut short. See `park_blocking_uring`.
    let mut interrupted = crate::commands::blocking::Interrupted::default();

    let mut selected_db: usize = 0;
    // Tracks whether the current authenticated user has a key/command restriction.
    // Drives RESTRICTED_KEY_CLIENTS/RESTRICTED_CMD_CLIENTS so handler.rs can
    // fast-exit on the common path. Kept in sync via acl::track_restriction_counters.
    // Declared before authenticated_user below because the implicit-default-user
    // path also needs to seed these from the "default" user's actual ACL rules.
    let mut key_restricted = false;
    let mut cmd_restricted = false;
    // The ACL as of when those flags were worked out; see `acl::ACL_EPOCH`.
    // Read first, so a change racing with the lookup below is noticed.
    let mut acl_epoch_seen = crate::acl::ACL_EPOCH.load(std::sync::atomic::Ordering::Relaxed);
    // None = not authenticated, Some(name) = authenticated as this user
    let mut authenticated_user: Option<String> = if crate::acl::default_user_is_open(&acl_store) {
        // Connections start implicitly authenticated as "default" when that
        // user needs no password — asked of the user table, where a password
        // from `--requirepass`, from the ACL file or from `ACL SETUSER` all
        // end up, and not of `--requirepass` alone -- but "default" is a real
        // ACL user and can carry
        // its own command/key restrictions (`ACL SETUSER default ... -flushall`).
        // Route through the same counter bookkeeping AUTH uses, or a restricted
        // default user's rules are silently never enforced for connections that
        // never explicitly call AUTH.
        if let Some(user) = acl_store.read().get("default") {
            crate::acl::track_restriction_counters(user, &mut key_restricted, &mut cmd_restricted);
        }
        Some("default".to_string())
    } else {
        None
    };

    // Transaction state — only touched when MULTI/EXEC/WATCH are used.
    use crate::commands::transaction::TxState;
    let mut tx = TxState::new();
    // A command of the batch being answered may have queued an AOF
    // record: its reply waits for it (see `aof::written`).
    let mut aof_wait = false;
    let dirty_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Negotiated RESP protocol version, updated by HELLO. Kept as local connection
    // state (not read from client_registry per command) so execute_command's
    // per-command null-encoding check doesn't hit the shared client_registry lock
    // on every single command across every worker thread.
    let mut resp_version: u8 = 2;

    enum ConnectionHandoff {
        Replica {
            legacy_sync: bool,
            /// What the replica asked to continue from, if anything.
            psync: Option<crate::replication::PsyncRequest>,
        },
    }
    // `MONITOR` mode, while the connection is in it.
    let mut monitoring: Option<crate::monitor::Monitoring> = None;
    // What a script run for this connection is given: see where it is made.
    #[cfg(feature = "lua")]
    let mut lua_env: Option<crate::lua_vm_blocking::ClientExecEnv> = None;

    loop {
        let was_private_at_read = private_query_buffer;
        // No `kill_rx` check here any more — the watcher task above shuts the
        // socket down, so a kill surfaces as `Ok(0)` from the read below even
        // when the client is idle.
        //
        // The read is never raced. tokio_uring cannot cancel an in-flight op,
        // so a losing `select!` arm would leak the buffer it owns until the
        // kernel completes it (see the note above). Reading straight into
        // `parse_buf` — the same zero-copy path an ordinary client uses — keeps
        // the buffer owned by this loop for exactly the duration of the read.
        // Query-buffer shrinking therefore happens *between* batches: right
        // after a read completes (the buffer is then owned by this loop, with
        // no operation in flight), before the next read is submitted. An idle
        // client whose oversized buffer is not followed by another read keeps
        // that buffer until the next read opportunity; that is the accepted
        // io_uring tradeoff for not racing the read (Redis shrinks such buffers
        // from its client cron instead).
        if parse_buf.capacity().saturating_sub(parse_buf.len()) < READ_MIN {
            parse_buf.reserve(READ_BUFFER);
        }
        let unparsed = parse_buf.len();
        let (res, buffer) = stream.read(parse_buf.slice(unparsed..)).await;
        parse_buf = buffer.into_inner();

        let n = match res {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                // cleanup runs below via the break path
                let _ = e; // suppress unused warning if needed
                break;
            }
        };
        // Bytes arrived from the socket: the client is active, so its idle
        // clock restarts. Once per read, not once per command — a pipeline
        // that arrives together is one interaction. EOF and read errors
        // `break` above, so they never reach here.
        activity.touch();
        // Between batches, before the activity timestamp is refreshed: if this
        // client was idle for long enough, reclaim an oversized private buffer
        // now. No read is in flight, so rebuilding `parse_buf` is safe.
        //
        // Only buffers past the resize threshold can be reclaimed, so the
        // clock is read only for those; an ordinary client — whose buffer is
        // the small initial one — pays nothing. This keeps `SET`/`GET` at the
        // floor from a `clock_gettime` on every read.
        if private_query_buffer
            && parse_buf.capacity() > resp::parse::QUERY_BUFFER_RESIZE_THRESHOLD
            && parse_buf.capacity().saturating_sub(parse_buf.len())
                > resp::parse::QUERY_BUFFER_MIN_SPARE_FOR_RESIZE
        {
            let resized = resp::parse::resize_private_query_buffer_after(
                &mut parse_buf,
                parse_start,
                &mut private_query_buffer,
                last_query_buffer_activity.elapsed(),
                query_buffer_idle_resize_after,
                query_buffer_peak,
                progress.minimum_capacity(),
            );
            if resized {
                parse_start = 0;
            }
            query_buffer_peak = parse_buf
                .len()
                .saturating_sub(parse_start)
                .max(progress.minimum_capacity());
        }
        if was_private_at_read {
            last_query_buffer_activity = Instant::now();
        }
        crate::stats::TOTAL_NET_INPUT_BYTES
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        crate::stats::record_read();
        activity.note_read(n);

        query_buffer_peak = query_buffer_peak.max(parse_buf.len().saturating_sub(parse_start));
        debug_assert_eq!(parse_buf.len(), unparsed + n);
        query_buffer.update(
            parse_buf.len().saturating_sub(parse_start),
            parse_buf.capacity().saturating_sub(parse_buf.len()),
            private_query_buffer,
        );

        let mut parse_offset = parse_start;
        let commands_before = activity.commands_run();
        let mut connection_handoff: Option<ConnectionHandoff> = None;
        let mut quit_requested = false;
        // Outside the parse loop: a park in one turn hands its delivery to
        // the re-dispatch on the next turn.
        let mut blocking_slot = crate::commands::blocking::BlockingSlot::default();

        // What the client sent that no request can begin with, if it did.
        let mut protocol_error: Option<String> = None;
        let query_buffer_bytes = parse_buf.len();
        let query_buffer_free = parse_buf.capacity().saturating_sub(parse_buf.len());
        let was_private_query_buffer = private_query_buffer;
        while let Some((consumed, cmd)) =
            match progress.next_request(&mut parse_buf, parse_offset, authenticated_user.is_some())
            {
                Ok(request) => request,
                Err(e) => {
                    protocol_error = Some(e);
                    None
                }
            }
        {
            private_query_buffer |= progress.take_private_query_buffer();
            if cmd.is_empty() {
                parse_offset += consumed;
                query_buffer.update(
                    query_buffer_bytes.saturating_sub(parse_offset),
                    query_buffer_free,
                    private_query_buffer,
                );
                continue;
            }
            query_buffer.update(
                query_buffer_bytes.saturating_sub(parse_offset + consumed),
                query_buffer_free,
                private_query_buffer,
            );
            // Re-read every command: CONFIG SET appendonly yes must take effect
            // on this connection, not only on ones accepted after the toggle.
            // Tokio's loop already loads AOF_ENABLED per command; this parameter
            // was the boot-time snapshot and left post-toggle writes unlogged.
            let aof_enabled = crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed);
            // ── QUIT ─────────────────────────────────────────────────────────
            // Reply +OK and close the connection. Handled in the connection
            // loop rather than the dispatch table because closing the socket
            // is a connection-level concern `execute_command` can't express.
            // See the matching comment in `src/server.rs` — QUIT was listed as
            // a special command in both allowlists but had no dispatch arm, so
            // it returned `-ERR unknown command` and left the socket open.
            //
            // Anything pipelined after QUIT is intentionally not executed —
            // the connection is closing, which is what Redis does too.
            //
            // The `len() == 4` gate keeps this off the hot path: it rejects
            // GET/SET/DEL in a single compare, before the byte-wise loop.
            if cmd[0].len() == 4 && ascii_eq_ignore_case(cmd[0], b"QUIT") {
                out_buf.extend_from_slice(b"+OK\r\n");
                parse_offset += consumed;
                quit_requested = true;
                break;
            }
            // For `CLIENT LIST`'s `cmd=` and `db=`, and `tot-cmds`. Not again
            // for a blocked command dispatched once more with what it waited
            // for: it is one command.
            if !blocking_slot.resuming() {
                activity.record(&cmd, selected_db);
            }
            if !aof_wait && crate::aof::acks_wait() && crate::aof::may_log(cmd[0]) {
                aof_wait = true;
            }
            // The command before this one is over: its reply is settled, and
            // what it left to tell tracking clients follows it.
            reply_gate.settle(&mut out_buf);
            crate::tracking::after_command(
                &tracking_table,
                &client_registry,
                client_id,
                &activity,
                &mut out_buf,
            );
            reply_gate.begin(&cmd, &mut out_buf);
            // ── ACL changes reach connections that are already open ──────────────
            // One relaxed load per command when nothing has changed.
            if crate::acl::ACL_EPOCH.load(std::sync::atomic::Ordering::Relaxed) != acl_epoch_seen
                && !crate::acl::resync_connection(
                    &acl_store,
                    authenticated_user.as_deref(),
                    &mut acl_epoch_seen,
                    &mut key_restricted,
                    &mut cmd_restricted,
                )
            {
                // The user this connection authenticated as has been deleted.
                quit_requested = true;
                break;
            }
            // ── CLIENT PAUSE enforcement (async-safe) ────────────────────────────
            // Must be done here, not in execute_command, because std::thread::sleep
            // inside a tokio task blocks the worker thread and prevents UNPAUSE
            // from being processed on another connection.
            let _ran =
                if crate::pause::PAUSE_UNTIL_MS.load(std::sync::atomic::Ordering::Relaxed) != 0 {
                    crate::pause::gate(&cmd, &tx.queue, client_id).await
                } else {
                    None
                };
            // ══════════════════════════════════════════════════════════════
            // FAST PATH — zero heap allocations, zero string comparisons
            // for the common case: authenticated, no active transaction,
            // and the command doesn't need connection-level interception.
            //
            // This is what the inner loop looked like in v0.1.10.
            // ══════════════════════════════════════════════════════════════
            // Read for each command — one atomic load — and not once per
            // connection: `CONFIG SET slowlog-log-slower-than` has to reach the
            // connections that are already open, the one that issued it included.
            let slowlog_enabled = slowlog.threshold_us() < u64::MAX;
            // A RESP2 connection with subscriptions may run next to nothing:
            // that is decided on the slow path.
            let subscriber_only = resp_version == 2 && subscriptions.is_some();
            if authenticated_user.is_some()
                && !tx.active
                && !subscriber_only
                && !needs_slow_path(cmd[0])
            {
                #[cfg(feature = "lua")]
                if crate::commands::lua_command_runs_long_sync_vm(cmd.as_slice()) {
                    // Made once for the connection and kept: it is two dozen shared
                    // handles, each counted up to copy and down to drop, and a script
                    // needs none of them copied to run on this thread. Made again if
                    // the connection has become another user since.
                    let user_now = authenticated_user.as_deref().unwrap_or("default");
                    if lua_env
                        .as_ref()
                        .is_some_and(|env| env.authenticated_user != user_now)
                    {
                        lua_env = None;
                    }
                    let env = &*lua_env.get_or_insert_with(|| {
                        crate::lua_vm_blocking::ClientExecEnv::snapshot(
                            &store,
                            &lastsave,
                            crate::lua_vm_blocking::AofBridge::Tokio(aof_tx.clone()),
                            &config,
                            &started_at,
                            &index_store,
                            &dict_store,
                            &wait_table,
                            &ts_index,
                            &pubsub,
                            &hub_tx,
                            &repl_state,
                            &watch_registry,
                            &client_registry,
                            &acl_store,
                            &acl_log,
                            &slowlog,
                            &latency,
                            &compaction_tx,
                            &cluster_state,
                            &monitor_registry,
                            &tracking_table,
                            &peer_addr,
                            client_id,
                            authenticated_user.as_deref().unwrap_or("default"),
                        )
                    });
                    let (response, elapsed_us) = crate::lua_vm_blocking::run_lua_vm_blocking(
                        cmd.as_slice(),
                        &mut selected_db,
                        env,
                        aof_enabled,
                    )
                    .await;
                    if slowlog_enabled && elapsed_us >= slowlog.threshold_us() {
                        let client_name = client_registry
                            .get(client_id)
                            .map(|info| info.name.clone())
                            .unwrap_or_default();
                        slowlog.record(&cmd, elapsed_us, &peer_addr, client_name);
                    }
                    out_buf.extend_from_slice(&response);
                    parse_offset += consumed;
                    continue;
                }

                if !slowlog_enabled {
                    let mut response = commands::execute_command(Ctx {
                        cmd,
                        store: &store,
                        lastsave: &lastsave,
                        selected_db: &mut selected_db,
                        aof_tx: &aof_tx,
                        aof_enabled,
                        config: &config,
                        started_at: &started_at,
                        index_store: &index_store,
                        dict_store: &dict_store,
                        wait_table: &wait_table,
                        ts_index: &ts_index,
                        pubsub: &pubsub,
                        hub_tx: &hub_tx,
                        repl_state: &repl_state,
                        watch_registry: watch_registry.as_ref(),
                        client_id,
                        client_registry: &client_registry,
                        resp: resp_version,
                        acl_store: &acl_store,
                        acl_log: &acl_log,
                        slowlog: &slowlog,
                        latency: &latency,
                        compaction_tx: &compaction_tx,
                        cluster: &cluster_state,
                        bypass_cluster_check: false,
                        apply_replication: false,
                        skip_write_side_effects: false,
                        peer_addr: &peer_addr,
                        monitor_registry: &monitor_registry,
                        authenticated_user: authenticated_user.as_deref().unwrap_or("default"),
                        tracking_table: &tracking_table,
                        blocking: Some(blocking_slot.prime()),
                    });
                    if let Some(wait) = blocking_slot.take_parked() {
                        if std::mem::take(&mut aof_wait) {
                            crate::aof::written(&aof_tx).await;
                        }
                        match park_blocking_uring(
                            wait,
                            &mut blocking_slot,
                            &stream,
                            &write_guard,
                            &mut out_buf,
                            subscriptions.as_ref(),
                            (&client_registry, client_id),
                            &mut interrupted,
                        )
                        .await
                        {
                            // `parse_offset` deliberately not advanced: the same
                            // command is re-dispatched, now carrying the delivery.
                            Ok(()) => continue,
                            Err(reply) => response = reply,
                        }
                    }

                    if interrupted.unblocked_with_error {
                        interrupted.unblocked_with_error = false;
                        response = crate::commands::blocking::UNBLOCKED.to_vec();
                    }
                    if interrupted.gone {
                        // It has left the line; there is nobody to reply to.
                        quit_requested = true;
                        break;
                    }
                    out_buf.extend_from_slice(&response);
                    parse_offset += consumed;
                    continue;
                }
                let mut waited_from: Option<u64> = None;
                let mut response = commands::execute_command(Ctx {
                    cmd,
                    store: &store,
                    lastsave: &lastsave,
                    selected_db: &mut selected_db,
                    aof_tx: &aof_tx,
                    aof_enabled,
                    config: &config,
                    started_at: &started_at,
                    index_store: &index_store,
                    dict_store: &dict_store,
                    wait_table: &wait_table,
                    ts_index: &ts_index,
                    pubsub: &pubsub,
                    hub_tx: &hub_tx,
                    repl_state: &repl_state,
                    watch_registry: watch_registry.as_ref(),
                    client_id,
                    client_registry: &client_registry,
                    resp: resp_version,
                    acl_store: &acl_store,
                    acl_log: &acl_log,
                    slowlog: &slowlog,
                    latency: &latency,
                    compaction_tx: &compaction_tx,
                    cluster: &cluster_state,
                    bypass_cluster_check: false,
                    apply_replication: false,
                    skip_write_side_effects: false,
                    peer_addr: &peer_addr,
                    monitor_registry: &monitor_registry,
                    authenticated_user: authenticated_user.as_deref().unwrap_or("default"),
                    tracking_table: &tracking_table,
                    blocking: Some(blocking_slot.prime()),
                });
                if let Some(wait) = blocking_slot.take_parked() {
                    waited_from = Some(now_us());
                    if std::mem::take(&mut aof_wait) {
                        crate::aof::written(&aof_tx).await;
                    }
                    match park_blocking_uring(
                        wait,
                        &mut blocking_slot,
                        &stream,
                        &write_guard,
                        &mut out_buf,
                        subscriptions.as_ref(),
                        (&client_registry, client_id),
                        &mut interrupted,
                    )
                    .await
                    {
                        // See the note at the non-slowlog site above.
                        Ok(()) => continue,
                        Err(reply) => response = reply,
                    }
                }

                if interrupted.unblocked_with_error {
                    interrupted.unblocked_with_error = false;
                    response = crate::commands::blocking::UNBLOCKED.to_vec();
                }
                if interrupted.gone {
                    // It has left the line; there is nobody to reply to.
                    quit_requested = true;
                    break;
                }
                // What the handler measured, and for a command that waited
                // (`BLPOP`) the wait as well: the clock is not read again
                // around every command for this.
                let elapsed_us = crate::cmdstats::last_command_us()
                    + waited_from.map_or(0, |from| now_us().saturating_sub(from));

                if elapsed_us >= slowlog.threshold_us() {
                    let client_name = client_registry
                        .get(client_id)
                        .map(|info| info.name.clone())
                        .unwrap_or_default();
                    // The command is still where it was read: it is read
                    // again for the log, now that it has turned out slow,
                    // where every command used to be copied in case it did.
                    if let Ok(Some((_, slow_cmd))) =
                        resp::parse::next_request(&mut parse_buf, parse_offset)
                    {
                        slowlog.record(&slow_cmd, elapsed_us, &peer_addr, client_name);
                    }
                }
                out_buf.extend_from_slice(&response);
                parse_offset += consumed;
                continue;
            }

            // ══════════════════════════════════════════════════════════════
            // SLOW PATH — only reached for auth, transactions, subscribe,
            // psync, or when not yet authenticated. Allocates cmd_name.
            // ══════════════════════════════════════════════════════════════
            let cmd_name = String::from_utf8_lossy(cmd[0]).to_ascii_uppercase();

            // ── Auth gate ─────────────────────────────────────────────────
            if authenticated_user.is_none()
                && !matches!(cmd_name.as_str(), "AUTH" | "QUIT" | "HELLO" | "RESET")
            {
                // Log unauthenticated access attempt
                let entry = crate::acl::AclLogEntry {
                    id: acl_log.next_id(),
                    reason: "auth".into(),
                    context: "toplevel".into(),
                    object: cmd_name.to_lowercase(),
                    username: "default".into(),
                    age_seconds: 0.0,
                    client_info: peer_addr.clone(),
                    timestamp_created: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                    count: 1,
                };
                acl_log.push(entry);
                out_buf.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
                parse_offset += consumed;
                continue;
            }

            // ── Permission check: the command, its keys, its channels ─────
            if let Some(ref username) = authenticated_user {
                let denied = acl_store
                    .read()
                    .get(username.as_str())
                    .and_then(|user| crate::acl::check_permission(user, &cmd_name, &cmd).err());
                if let Some(denied) = denied {
                    acl_log.record(denied.reason(), denied.object(), username, &peer_addr);
                    // Refused inside MULTI, it is not queued, and EXEC runs nothing, as
                    // for a command refused for any other reason (#159).
                    if tx.active {
                        tx.aborted = true;
                    }
                    out_buf
                        .extend_from_slice(format!("-{}\r\n", denied.message(username)).as_bytes());
                    parse_offset += consumed;
                    continue;
                }
            }

            // ── Subscriptions ─────────────────────────────────────────────
            // A subscribed connection stays in this loop, with a task of its
            // own writing its messages. Under RESP3 that is all there is to
            // it; under RESP2 it may run nothing but the commands below while
            // it has subscriptions. Mirrors the tokio loop in server.rs: inside
            // MULTI they are queued, and EXEC runs them (#23).
            if crate::pubsub::is_subscription_command(cmd_name.as_str()) && !tx.active {
                if let Some(refusal) =
                    crate::pubsub::shard_subscribe_refusal(cmd_name.as_str(), &cmd, &cluster_state)
                {
                    out_buf.extend_from_slice(&refusal);
                    parse_offset += consumed;
                    continue;
                }
                let subs = subscriptions.get_or_insert_with(|| {
                    open_subscriptions(&pubsub, &client_registry, client_id, &stream, &write_guard)
                });
                out_buf.extend_from_slice(&subs.handle(&cmd, resp_version));
                if subs.total() == 0 {
                    subscriptions = None;
                }
                parse_offset += consumed;
                continue;
            }
            if resp_version == 2
                && subscriptions.is_some()
                && let Some(reply) = crate::pubsub::resp2_subscriber_reply(cmd_name.as_str(), &cmd)
            {
                out_buf.extend_from_slice(&reply);
                parse_offset += consumed;
                continue;
            }

            // ── Connection handoffs (break parse loop so `parse_buf` can be drained) ──
            // A replica whose dataset may already be this master's is first
            // offered the fingerprint (see `replication::digest`), and the
            // connection stays a client until it asks again.
            // A replica does not take replicas of its own (#144).
            if (cmd_name == "PSYNC" || cmd_name == "SYNC") && repl_state.is_slave() {
                out_buf.extend_from_slice(&crate::resp::write::resp_error(
                    crate::replication::NOT_A_MASTER,
                ));
                parse_offset += consumed;
                continue;
            }
            if cmd_name == "PSYNC"
                && let Some(offer) = repl_state.digest_offer(&client_registry, client_id, &cmd)
            {
                out_buf.extend_from_slice(&offer);
                parse_offset += consumed;
                continue;
            }
            if cmd_name == "PSYNC" || cmd_name == "SYNC" {
                parse_offset += consumed;
                connection_handoff = Some(ConnectionHandoff::Replica {
                    legacy_sync: cmd_name == "SYNC",
                    psync: crate::replication::PsyncRequest::parse(&cmd),
                });
                break;
            }
            if cmd_name == "MONITOR" {
                if monitoring.is_none() {
                    monitoring = Some(crate::monitor::Monitoring::start(
                        &monitor_registry,
                        push_tx.clone(),
                    ));
                    client_registry.update(client_id, |info| info.flags = "O".to_string());
                }
                out_buf.extend_from_slice(b"+OK\r\n");
                parse_offset += consumed;
                continue;
            }

            // `MULTI` or `EXEC`, as the client wrote it: for MONITOR, below.
            let transaction_word: Option<Vec<u8>> =
                matches!(cmd_name.as_str(), "MULTI" | "EXEC").then(|| cmd[0].to_vec());

            // ── Transaction + Auth commands ───────────────────────────────
            let response = match cmd_name.as_str() {
                "MULTI" => {
                    if tx.active {
                        b"-ERR MULTI calls can not be nested\r\n".to_vec()
                    } else {
                        tx.active = true;
                        b"+OK\r\n".to_vec()
                    }
                }

                "DISCARD" => {
                    if !tx.active {
                        b"-ERR DISCARD without MULTI\r\n".to_vec()
                    } else {
                        watch_registry.as_ref().unwatch_all(
                            &tx.watched_keys
                                .iter()
                                .map(|_| Arc::clone(&dirty_flag))
                                .collect::<Vec<_>>(),
                        );
                        tx.reset();
                        dirty_flag.store(false, std::sync::atomic::Ordering::Release);
                        b"+OK\r\n".to_vec()
                    }
                }

                "WATCH" => {
                    if tx.active {
                        b"-ERR WATCH inside MULTI is not allowed\r\n".to_vec()
                    } else if cmd.len() < 2 {
                        b"-ERR wrong number of arguments for 'watch' command\r\n".to_vec()
                    } else {
                        for key_bytes in &cmd[1..] {
                            if let Ok(key) = std::str::from_utf8(key_bytes) {
                                // A key that has already expired is watched
                                // as the absent key it is: its removal, when
                                // it comes, is not a change.
                                commands::helpers::lazy_expire_key(&store, selected_db, key);
                                tx.watched_keys.push((selected_db, key.to_string()));
                                watch_registry.as_ref().watch(
                                    selected_db,
                                    key,
                                    Arc::clone(&dirty_flag),
                                );
                            }
                        }
                        b"+OK\r\n".to_vec()
                    }
                }

                "UNWATCH" => {
                    watch_registry.as_ref().unwatch_all(
                        &tx.watched_keys
                            .iter()
                            .map(|_| Arc::clone(&dirty_flag))
                            .collect::<Vec<_>>(),
                    );
                    tx.watched_keys.clear();
                    dirty_flag.store(false, std::sync::atomic::Ordering::Release);
                    b"+OK\r\n".to_vec()
                }

                // RESET: the connection as it was when it was opened. The
                // transaction and the watches are dropped, subscriptions and
                // tracking ended, the database is 0, the protocol RESP2, the
                // name gone — and the user is `default` again, if `default`
                // needs no password, or nobody.
                "RESET" => {
                    watch_registry.as_ref().unwatch_all(
                        &tx.watched_keys
                            .iter()
                            .map(|_| Arc::clone(&dirty_flag))
                            .collect::<Vec<_>>(),
                    );
                    tx.reset();
                    dirty_flag.store(false, std::sync::atomic::Ordering::Release);
                    selected_db = 0;
                    resp_version = 2;
                    subscriptions = None;
                    if client_registry
                        .get(client_id)
                        .map(|i| i.tracking)
                        .unwrap_or(false)
                    {
                        tracking_table.disable_tracking();
                    }
                    tracking_table.unregister_client(client_id);
                    monitoring = None;
                    client_registry.update(client_id, |info| {
                        info.resp = 2;
                        info.name = String::new();
                        info.tracking = false;
                        info.tracking_bcast = false;
                        info.tracking_optin = false;
                        info.tracking_optout = false;
                        info.tracking_noloop = false;
                        info.tracking_caching_next = None;
                        info.tracking_prefixes.clear();
                        info.tracking_redirect = None;
                        info.tracking_redirect_broken = false;
                        info.flags = "N".to_string();
                        info.user = "default".to_string();
                    });
                    authenticated_user = if crate::acl::default_user_is_open(&acl_store) {
                        if let Some(user) = acl_store.read().get("default") {
                            crate::acl::track_restriction_counters(
                                user,
                                &mut key_restricted,
                                &mut cmd_restricted,
                            );
                        }
                        Some("default".to_string())
                    } else {
                        crate::acl::clear_restriction_counters(
                            &mut key_restricted,
                            &mut cmd_restricted,
                        );
                        None
                    };
                    b"+RESET\r\n".to_vec()
                }

                "EXEC" => {
                    // One watching transaction at a time, from its check to
                    // its last command. See `WATCHED_EXEC`.
                    let _one_at_a_time = if tx.active && !tx.watched_keys.is_empty() {
                        Some(commands::transaction::WATCHED_EXEC.lock().await)
                    } else {
                        None
                    };
                    // A watched key whose time ran out while it was watched
                    // has been modified, whether or not anything has got
                    // round to removing it. Removing it now says so.
                    for (db, key) in &tx.watched_keys {
                        commands::helpers::lazy_expire_key(&store, *db, key);
                    }
                    let refusal = if !tx.active {
                        None
                    } else if let Some(busy) =
                        commands::transaction::exec_refusal(&cmd, &tx.queue, &repl_state)
                    {
                        // Before anything else, as for any command: a
                        // script has the server.
                        Some(busy)
                    } else if tx.aborted {
                        Some(
                            b"-EXECABORT Transaction discarded because of previous errors.\r\n"
                                .to_vec(),
                        )
                    } else {
                        crate::cluster::routing::exec_refusal(
                            &crate::cluster::routing::Caller {
                                cluster: &cluster_state,
                                registry: &client_registry,
                                client_id,
                                store: &store,
                                db: selected_db,
                            },
                            &tx.queue,
                        )
                    };
                    if !tx.active {
                        b"-ERR EXEC without MULTI\r\n".to_vec()
                    } else if let Some(refusal) = refusal {
                        // Nothing runs: the transaction is discarded whole.
                        watch_registry.as_ref().unwatch_all(
                            &tx.watched_keys
                                .iter()
                                .map(|_| Arc::clone(&dirty_flag))
                                .collect::<Vec<_>>(),
                        );
                        tx.reset();
                        dirty_flag.store(false, std::sync::atomic::Ordering::Release);
                        refusal
                    } else {
                        // From the check of the watched keys to the last queued
                        // command, the transaction is alone: no other command —
                        // read or write — runs in between. See `command_gate`.
                        let _alone = crate::command_gate::enter_alone();
                        // And as one write: a snapshot, a backup or a full sync cuts
                        // before it or after it, never between two of its commands
                        // (#148). The writes it runs are inside this one.
                        let _whole = crate::write_gate::enter();
                        // And to replicas and the AOF as one: its writes in the
                        // stream between a `MULTI` and an `EXEC`, its records
                        // written together (#172). Dropped first, while the
                        // gates are still held.
                        let _propagation = crate::replication::PropagationGroup::begin(
                            &repl_state,
                            &hub_tx,
                            &aof_tx,
                        );
                        if dirty_flag.load(std::sync::atomic::Ordering::Acquire) {
                            watch_registry.as_ref().unwatch_all(
                                &tx.watched_keys
                                    .iter()
                                    .map(|_| Arc::clone(&dirty_flag))
                                    .collect::<Vec<_>>(),
                            );
                            tx.reset();
                            dirty_flag.store(false, std::sync::atomic::Ordering::Release);
                            // RESP3 has a dedicated null type (`_`); a RESP2-style `*-1` null
                            // array is not guaranteed to be recognized as null by a strict RESP3
                            // parser (redis-py's, e.g., parses it as an empty array instead),
                            // which breaks WATCH-abort detection for RESP3 clients entirely.
                            if resp_version == 3 {
                                crate::resp::write::resp_null_resp3()
                            } else {
                                crate::resp::write::resp_null_array()
                            }
                        } else if let Some(sets) =
                            exec_fast_apply_sets(tx.watched_keys.is_empty(), &tx.queue)
                                // The fast path writes without going through the
                                // per-command ACL check. A user with any
                                // restriction takes the generic path, where each
                                // queued command is checked against the rules in
                                // force now — not the ones in force when it was
                                // queued.
                                .filter(|_| !key_restricted && !cmd_restricted)
                        {
                            // ── Lock-held fast-apply path ────────────────────────────────────
                            // The generic path checks `dirty_flag` unlocked, then dispatches the
                            // queued write through the normal per-command path, which re-acquires
                            // the shard lock internally — there's a real gap between "checked
                            // clean" and "committed" during which another connection's write (on
                            // a different worker thread) can land, get lost by neither side ever
                            // observing the other. That's the lost-update race this closes for the
                            // common single/simple-multi-key CAS shape: acquire every affected
                            // shard's write lock up front (sorted by address so two concurrent
                            // fast-apply EXECs can't deadlock on each other), verify `dirty_flag`
                            // *while holding them*, and only then mutate — so no other writer to
                            // any of these shards can possibly interleave between the check and
                            // the commit. We can't just hold the lock and call the normal
                            // per-command dispatch path here: it would try to re-acquire the same
                            // (non-reentrant) shard lock and deadlock against itself. So this path
                            // applies the mutation directly, then replays the exact same
                            // replication/AOF/watch-notify/monitor/tracking side effects via
                            // `apply_write_side_effects` (shared with the normal path, not a
                            // hand-duplicated copy) so nothing observable differs except the race
                            // being closed.
                            use std::sync::atomic::Ordering as AtomicOrdering;

                            let mut shard_ptrs: Vec<usize> = Vec::new();
                            let mut shards: Vec<&store::Shard> = Vec::new();
                            for (db_idx, key) in tx.watched_keys.iter() {
                                let shard = store::get_shard(&store[*db_idx], key);
                                let ptr = Arc::as_ptr(shard) as usize;
                                if !shard_ptrs.contains(&ptr) {
                                    shard_ptrs.push(ptr);
                                    shards.push(shard);
                                }
                            }
                            for (key, _) in sets.iter() {
                                let shard = store::get_shard(&store[selected_db], key);
                                let ptr = Arc::as_ptr(shard) as usize;
                                if !shard_ptrs.contains(&ptr) {
                                    shard_ptrs.push(ptr);
                                    shards.push(shard);
                                }
                            }
                            let mut order: Vec<usize> = (0..shards.len()).collect();
                            order.sort_by_key(|&i| shard_ptrs[i]);

                            // Hold the write gate across the mutation and its side effects,
                            // like any other write (see `write_gate`). Taken before the
                            // shard locks: gate first, shards second, everywhere.
                            let _write_gate = crate::write_gate::enter();
                            // …and, ahead of the shard locks too, the order of
                            // these keys' writes in the stream (`write_order`).
                            let _write_order = crate::write_order::lock_keys(
                                selected_db,
                                sets.iter().map(|(key, _)| key.as_bytes()),
                            );
                            let mut guards: Vec<(usize, _)> = order
                                .iter()
                                .map(|&i| (shard_ptrs[i], shards[i].write()))
                                .collect();

                            // During a handoff (`pause::HANDOFF`) this transaction
                            // got past the pause before it was set: it is given up, as
                            // if a watched key had changed, and the client's retry is
                            // answered by whoever owns the keys then.
                            if dirty_flag.load(AtomicOrdering::Acquire) || crate::pause::handoff() {
                                drop(guards);
                                watch_registry.as_ref().unwatch_all(
                                    &tx.watched_keys
                                        .iter()
                                        .map(|_| Arc::clone(&dirty_flag))
                                        .collect::<Vec<_>>(),
                                );
                                tx.reset();
                                dirty_flag.store(false, AtomicOrdering::Release);
                                if resp_version == 3 {
                                    crate::resp::write::resp_null_resp3()
                                } else {
                                    crate::resp::write::resp_null_array()
                                }
                            } else {
                                // Each SET is a use, and a modification, of its
                                // key, as the same SET is outside a transaction
                                // (it went unrecorded here).
                                let _access = crate::eviction::recording().then(|| {
                                    crate::store::AccessScope::open(crate::eviction::write_stamp(
                                        std::time::Instant::now(),
                                    ))
                                });
                                for (key, value) in sets.iter() {
                                    let shard = store::get_shard(&store[selected_db], key);
                                    let ptr = Arc::as_ptr(shard) as usize;
                                    if let Some((_, guard)) =
                                        guards.iter_mut().find(|(p, _)| *p == ptr)
                                    {
                                        drop(guard.set_str_expiring(&**key, value, None));
                                    }
                                    // Notify while STILL HOLDING this key's shard lock. Doing this
                                    // after `drop(guards)` below would reopen the exact race this
                                    // path exists to close: another fast-apply EXEC could acquire
                                    // the now-free lock and check `dirty_flag` before this notify
                                    // sets it, observe clean, and overwrite this write. notify()
                                    // only touches the (separate) watch-registry lock, so calling
                                    // it here cannot deadlock against the shard lock we're holding.
                                    watch_registry.as_ref().notify(selected_db, key);
                                }
                                // A plain SET takes the key's time to live away, in
                                // the same step as it writes the value.
                                drop(guards);

                                let count = sets.len();
                                let mut exec_out = format!("*{count}\r\n").into_bytes();
                                for (key, value) in sets.iter() {
                                    let raw_cmd =
                                        [b"SET".to_vec(), key.clone().into_bytes(), value.clone()];
                                    let cmd_ctx = Ctx {
                                        cmd: raw_cmd.iter().map(|v| v.as_slice()).collect(),
                                        store: &store,
                                        lastsave: &lastsave,
                                        selected_db: &mut selected_db,
                                        aof_tx: &aof_tx,
                                        aof_enabled,
                                        config: &config,
                                        started_at: &started_at,
                                        index_store: &index_store,
                                        dict_store: &dict_store,
                                        wait_table: &wait_table,
                                        ts_index: &ts_index,
                                        pubsub: &pubsub,
                                        hub_tx: &hub_tx,
                                        repl_state: &repl_state,
                                        watch_registry: watch_registry.as_ref(),
                                        client_id,
                                        client_registry: &client_registry,
                                        resp: resp_version,
                                        acl_store: &acl_store,
                                        acl_log: &acl_log,
                                        slowlog: &slowlog,
                                        latency: &latency,
                                        compaction_tx: &compaction_tx,
                                        cluster: &cluster_state,
                                        bypass_cluster_check: false,
                                        apply_replication: false,
                                        skip_write_side_effects: false,
                                        peer_addr: &peer_addr,
                                        monitor_registry: &monitor_registry,
                                        authenticated_user: authenticated_user
                                            .as_deref()
                                            .unwrap_or("default"),
                                        tracking_table: &tracking_table,
                                        blocking: None,
                                    };
                                    crate::keyspace_events::notify(
                                        cmd_ctx.pubsub,
                                        *cmd_ctx.selected_db,
                                        crate::keyspace_events::KS_STRING,
                                        "set",
                                        key,
                                    );
                                    commands::handler::apply_write_side_effects(
                                        &cmd_ctx, "SET", b"+OK\r\n",
                                    );
                                    exec_out.extend_from_slice(b"+OK\r\n");
                                }

                                watch_registry.as_ref().unwatch_all(
                                    &tx.watched_keys
                                        .iter()
                                        .map(|_| Arc::clone(&dirty_flag))
                                        .collect::<Vec<_>>(),
                                );
                                tx.reset();
                                dirty_flag.store(false, AtomicOrdering::Release);
                                exec_out
                            }
                        } else {
                            let queued = std::mem::take(&mut tx.queue);
                            let count = queued.len();
                            let mut exec_out = format!("*{count}\r\n").into_bytes();

                            // Hoist outside the loop — name won't change mid-transaction
                            let client_name_for_slowlog = client_registry
                                .get(client_id)
                                .map(|info| info.name.clone())
                                .unwrap_or_default();

                            // A plain FLUSHALL here is not counted as lazily freed.
                            let _queued = crate::lazyfree::running_queued();
                            for queued_cmd in queued {
                                // SUBSCRIBE and its kind, queued: see the tokio loop (#23).
                                if queued_cmd.first().is_some_and(|name| {
                                    crate::pubsub::is_subscription_command(
                                        &String::from_utf8_lossy(name).to_ascii_uppercase(),
                                    )
                                }) {
                                    let args: Vec<&[u8]> =
                                        queued_cmd.iter().map(|b| b.as_slice()).collect();
                                    let subs = subscriptions.get_or_insert_with(|| {
                                        open_subscriptions(
                                            &pubsub,
                                            &client_registry,
                                            client_id,
                                            &stream,
                                            &write_guard,
                                        )
                                    });
                                    exec_out.extend_from_slice(&subs.handle(&args, resp_version));
                                    if subs.total() == 0 {
                                        subscriptions = None;
                                    }
                                    continue;
                                }
                                let raw_cmd = queued_cmd.clone();
                                let _exec_slices: Vec<&[u8]> =
                                    queued_cmd.iter().map(|b| b.as_slice()).collect();

                                #[cfg(feature = "lua")]
                                let (resp, elapsed_us) = {
                                    let cmd_start = std::time::Instant::now();
                                    let r = commands::execute_command(Ctx {
                                        cmd: _exec_slices,
                                        store: &store,
                                        lastsave: &lastsave,
                                        selected_db: &mut selected_db,
                                        aof_tx: &aof_tx,
                                        aof_enabled,
                                        config: &config,
                                        started_at: &started_at,
                                        index_store: &index_store,
                                        dict_store: &dict_store,
                                        wait_table: &wait_table,
                                        ts_index: &ts_index,
                                        pubsub: &pubsub,
                                        hub_tx: &hub_tx,
                                        repl_state: &repl_state,
                                        watch_registry: watch_registry.as_ref(),
                                        client_id,
                                        client_registry: &client_registry,
                                        resp: resp_version,
                                        acl_store: &acl_store,
                                        acl_log: &acl_log,
                                        slowlog: &slowlog,
                                        latency: &latency,
                                        compaction_tx: &compaction_tx,
                                        cluster: &cluster_state,
                                        bypass_cluster_check: false,
                                        apply_replication: false,
                                        skip_write_side_effects: false,
                                        peer_addr: &peer_addr,
                                        monitor_registry: &monitor_registry,
                                        authenticated_user: authenticated_user
                                            .as_deref()
                                            .unwrap_or("default"),
                                        tracking_table: &tracking_table,
                                        blocking: None,
                                    });
                                    (r, cmd_start.elapsed().as_micros() as u64)
                                };

                                #[cfg(not(feature = "lua"))]
                                let (resp, elapsed_us) = {
                                    let cmd_start = std::time::Instant::now();
                                    let queued_borrowed: Vec<&[u8]> =
                                        queued_cmd.iter().map(|b| b.as_slice()).collect();
                                    let r = commands::execute_command(Ctx {
                                        cmd: queued_borrowed,
                                        store: &store,
                                        lastsave: &lastsave,
                                        selected_db: &mut selected_db,
                                        aof_tx: &aof_tx,
                                        aof_enabled,
                                        config: &config,
                                        started_at: &started_at,
                                        index_store: &index_store,
                                        dict_store: &dict_store,
                                        wait_table: &wait_table,
                                        ts_index: &ts_index,
                                        pubsub: &pubsub,
                                        hub_tx: &hub_tx,
                                        repl_state: &repl_state,
                                        watch_registry: watch_registry.as_ref(),
                                        client_id,
                                        client_registry: &client_registry,
                                        resp: resp_version,
                                        acl_store: &acl_store,
                                        acl_log: &acl_log,
                                        slowlog: &slowlog,
                                        latency: &latency,
                                        compaction_tx: &compaction_tx,
                                        cluster: &cluster_state,
                                        bypass_cluster_check: false,
                                        apply_replication: false,
                                        skip_write_side_effects: false,
                                        peer_addr: &peer_addr,
                                        monitor_registry: &monitor_registry,
                                        authenticated_user: authenticated_user
                                            .as_deref()
                                            .unwrap_or("default"),
                                        tracking_table: &tracking_table,
                                        blocking: None,
                                    });
                                    (r, cmd_start.elapsed().as_micros() as u64)
                                };

                                if elapsed_us >= slowlog.threshold_us() {
                                    slowlog.record(
                                        &raw_cmd,
                                        elapsed_us,
                                        &peer_addr,
                                        client_name_for_slowlog.clone(),
                                    );
                                }

                                exec_out.extend_from_slice(&resp);
                            }

                            watch_registry.as_ref().unwatch_all(
                                &tx.watched_keys
                                    .iter()
                                    .map(|_| Arc::clone(&dirty_flag))
                                    .collect::<Vec<_>>(),
                            );
                            tx.reset();
                            dirty_flag.store(false, std::sync::atomic::Ordering::Release);
                            exec_out
                        }
                    }
                }

                "AUTH" => {
                    // AUTH password            — legacy single-password form
                    // AUTH username password   — Redis 6 ACL multi-user form
                    //
                    // We check the acl_store for all cases. The default user's
                    // password is stored there (seeded from --requirepass at startup).
                    // nopass users authenticate with any password.
                    if cmd.len() < 2 {
                        b"-ERR wrong number of arguments for 'auth' command\r\n".to_vec()
                    } else if cmd.len() == 2 && crate::acl::default_user_is_open(&acl_store) {
                        // As Redis: how a client learns that the password it
                        // was given is not in use. It was answered OK (#135).
                        crate::acl::AUTH_WITHOUT_PASSWORD.as_bytes().to_vec()
                    } else {
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

                        let matched = crate::acl::authenticate(
                            &acl_store,
                            &provided_username,
                            &provided_password,
                        );

                        match matched.as_ref() {
                            Some(user) => {
                                // Adjust the restricted-client counters before
                                // overwriting authenticated_user, so we correctly
                                // handle re-auth (e.g. switching from restricted
                                // to unrestricted user on the same connection).
                                crate::acl::track_restriction_counters(
                                    user,
                                    &mut key_restricted,
                                    &mut cmd_restricted,
                                );
                                authenticated_user = Some(user.name.clone());
                                client_registry
                                    .update(client_id, |info| info.user = user.name.clone());
                                // Simple string, not a bulk string: real Redis
                                // replies `+OK` here, and clients that text-match
                                // a leading `+OK` on control connections read
                                // `$2\r\nOK\r\n` as a failed AUTH.
                                crate::resp::write::resp_simple("OK")
                            }
                            None => {
                                let entry = crate::acl::AclLogEntry {
                                    id: acl_log.next_id(),
                                    reason: "auth".into(),
                                    context: "toplevel".into(),
                                    object: provided_username.clone(),
                                    username: provided_username.clone(),
                                    age_seconds: 0.0,
                                    client_info: peer_addr.clone(),
                                    timestamp_created: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis()
                                        as u64,
                                    count: 1,
                                };
                                acl_log.push(entry);
                                b"-WRONGPASS invalid username-password pair or user is disabled.\r\n".to_vec()
                            }
                        }
                    }
                }

                "HELLO" => {
                    // HELLO [protover [AUTH username password] [SETNAME clientname]]

                    let mut i = 1usize; // index into cmd, past "HELLO"

                    let mut proto_reply: i64 = resp_version as i64;

                    // Optional protocol version
                    if let Some(ver_bytes) = cmd.get(i) {
                        let ver = std::str::from_utf8(ver_bytes).unwrap_or("2");
                        // protover 0 is a special case meaning "RESET to defaults"
                        if ver != "0" && ver != "1" && ver != "2" && ver != "3" {
                            out_buf.extend_from_slice(
                                b"-ERR Protocol version is not an integer or out of range\r\n",
                            );
                            parse_offset += consumed;
                            continue;
                        }
                        proto_reply = match ver {
                            "3" => {
                                resp_version = 3;
                                client_registry.update(client_id, |info| info.resp = 3);
                                3
                            }
                            _ => {
                                resp_version = 2;
                                client_registry.update(client_id, |info| info.resp = 2);
                                2
                            }
                        };
                        i += 1;
                    }

                    // Optional AUTH username password
                    if cmd
                        .get(i)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .map(|s| s.eq_ignore_ascii_case("AUTH"))
                        .unwrap_or(false)
                    {
                        i += 1; // consume "AUTH"
                        let (provided_username, provided_password) =
                            match (cmd.get(i), cmd.get(i + 1)) {
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
                                _ => {
                                    out_buf.extend_from_slice(
                                        b"-ERR Syntax error in HELLO option 'auth'\r\n",
                                    );
                                    parse_offset += consumed;
                                    continue;
                                }
                            };

                        // The user named, and no other. (This used to
                        // accept `default` as a name for *any* user whose
                        // password matched.)
                        let matched = crate::acl::authenticate(
                            &acl_store,
                            provided_username,
                            provided_password,
                        );
                        match matched.as_ref() {
                            Some(user) => {
                                crate::acl::track_restriction_counters(
                                    user,
                                    &mut key_restricted,
                                    &mut cmd_restricted,
                                );
                                authenticated_user = Some(user.name.clone());
                                client_registry
                                    .update(client_id, |info| info.user = user.name.clone());
                            }
                            None => {
                                out_buf.extend_from_slice(
                                    b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
                                );
                                parse_offset += consumed;
                                continue;
                            }
                        }
                    }

                    // HELLO is let through the auth gate so that it can
                    // carry AUTH; without it, it needs a connection that is
                    // already authenticated.
                    if authenticated_user.is_none() {
                        out_buf.extend_from_slice(
                            b"-NOAUTH HELLO must be called with the client already \
                              authenticated, otherwise the HELLO <proto> AUTH <user> <pass> \
                              option can be used to authenticate the client and select the \
                              RESP protocol version at the same time\r\n",
                        );
                        parse_offset += consumed;
                        continue;
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
                            client_registry.update(client_id, |info| info.name = name);
                        }
                    }

                    // Build the response — RESP3 clients (e.g. modern redis-py, whose default
                    // changed to protocol 3) expect the HELLO reply itself encoded as a `%`
                    // map once RESP3 is negotiated; RESP2 stays a flat `*` array of pairs.
                    use crate::resp::write::{resp_array, resp_bulk_str, resp_integer, resp_map};
                    let role = repl_state.role();
                    let kv = vec![
                        resp_bulk_str("server"),
                        resp_bulk_str("kividb"),
                        resp_bulk_str("version"),
                        resp_bulk_str(env!("CARGO_PKG_VERSION")),
                        resp_bulk_str("proto"),
                        resp_integer(proto_reply),
                        resp_bulk_str("id"),
                        resp_integer(client_id as i64),
                        resp_bulk_str("mode"),
                        resp_bulk_str("standalone"),
                        resp_bulk_str("role"),
                        resp_bulk_str(&role),
                        resp_bulk_str("modules"),
                        commands::server::module_list_reply(proto_reply as u8),
                    ];
                    let response = if proto_reply == 3 {
                        resp_map(kv)
                    } else {
                        resp_array(kv)
                    };
                    out_buf.extend_from_slice(&response);
                    parse_offset += consumed;
                    continue;
                }

                // In the slow path match in server.rs, add WAITAOF as a special case:
                "WAITAOF" => {
                    if cmd.len() < 4 {
                        b"-ERR wrong number of arguments for 'waitaof' command\r\n".to_vec()
                    } else if !aof_enabled
                        && !crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
                    {
                        // Nothing goes to an AOF, so nothing is on disk by
                        // one: it answered that the write was (#137). On at
                        // the start, or turned on since by `CONFIG SET`.
                        let num_local = std::str::from_utf8(cmd[1])
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                            .unwrap_or(0);
                        if num_local != 0 {
                            b"-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.\r\n"
                                .to_vec()
                        } else {
                            b"*2\r\n:0\r\n:0\r\n".to_vec()
                        }
                    } else {
                        let timeout_ms: u64 =
                            std::str::from_utf8(cmd.get(3).copied().unwrap_or(b"0"))
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0);

                        let target =
                            crate::aof::AOF_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);

                        crate::aof::request_flush();

                        let deadline = tokio::time::Instant::now()
                            + tokio::time::Duration::from_millis(if timeout_ms == 0 {
                                u64::MAX
                            } else {
                                timeout_ms
                            });

                        loop {
                            let fsynced =
                                crate::aof::AOF_FSYNCED.load(std::sync::atomic::Ordering::Relaxed);
                            if fsynced >= target || tokio::time::Instant::now() >= deadline {
                                break if fsynced >= target {
                                    b"*2\r\n:1\r\n:0\r\n".to_vec()
                                } else {
                                    b"*2\r\n:0\r\n:0\r\n".to_vec()
                                };
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                }

                "WAIT" => {
                    if cmd.len() < 3 {
                        b"-ERR wrong number of arguments for 'wait' command\r\n".to_vec()
                    } else {
                        let num_replicas: i64 =
                            std::str::from_utf8(cmd.get(1).copied().unwrap_or(b"0"))
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0);

                        let timeout_ms: u64 =
                            std::str::from_utf8(cmd.get(2).copied().unwrap_or(b"0"))
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0);

                        let connected = repl_state
                            .connected_replicas
                            .load(std::sync::atomic::Ordering::Relaxed);

                        if num_replicas == 0 || connected == 0 {
                            crate::resp::write::resp_integer(connected).to_vec()
                        } else {
                            // See the tokio loop in server.rs: per-replica
                            // targets, taken after a hub barrier.
                            let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel();
                            if hub_tx
                                .send(crate::replication::HubMsg::Barrier(barrier_tx))
                                .is_ok()
                            {
                                let _ = barrier_rx.await;
                            }
                            let targets = repl_state.wait_targets();

                            let deadline = tokio::time::Instant::now()
                                + tokio::time::Duration::from_millis(if timeout_ms == 0 {
                                    u64::MAX
                                } else {
                                    timeout_ms
                                });

                            loop {
                                let acked = crate::replication::ReplState::count_reached(&targets);
                                if acked >= num_replicas || tokio::time::Instant::now() >= deadline
                                {
                                    break crate::resp::write::resp_integer(acked).to_vec();
                                }
                                tokio::task::yield_now().await; // not tokio::time::sleep
                            }
                        }
                    }
                }

                _ if tx.active => {
                    // Inside a transaction — queue, don't execute. In a
                    // cluster the command is routed now: one that belongs to
                    // another node is refused, and takes the transaction
                    // with it.
                    //
                    // So is one that is no command at all, or has the wrong
                    // number of arguments, or arrives while a script has the
                    // server busy.
                    let refusal =
                        commands::transaction::queue_refusal(&cmd_name, &cmd).or_else(|| {
                            crate::cluster::routing::queue_refusal(
                                &crate::cluster::routing::Caller {
                                    cluster: &cluster_state,
                                    registry: &client_registry,
                                    client_id,
                                    store: &store,
                                    db: selected_db,
                                },
                                &cmd,
                            )
                        });
                    match refusal {
                        Some(refusal) => {
                            tx.aborted = true;
                            refusal
                        }
                        None => {
                            tx.queue.push(cmd.iter().map(|s| s.to_vec()).collect());
                            b"+QUEUED\r\n".to_vec()
                        }
                    }
                }

                // Fallthrough: normal command that happened to match
                // needs_slow_path (e.g. AUTH when already authenticated
                // but first byte matched 'a').
                _ => {
                    let raw_cmd = cmd.clone();

                    #[cfg(feature = "lua")]
                    let (result, elapsed_us) =
                        if crate::commands::lua_command_runs_long_sync_vm(cmd.as_slice()) {
                            // Made once for the connection and kept: it is two dozen shared
                            // handles, each counted up to copy and down to drop, and a script
                            // needs none of them copied to run on this thread. Made again if
                            // the connection has become another user since.
                            let user_now = authenticated_user.as_deref().unwrap_or("default");
                            if lua_env
                                .as_ref()
                                .is_some_and(|env| env.authenticated_user != user_now)
                            {
                                lua_env = None;
                            }
                            let env = &*lua_env.get_or_insert_with(|| {
                                crate::lua_vm_blocking::ClientExecEnv::snapshot(
                                    &store,
                                    &lastsave,
                                    crate::lua_vm_blocking::AofBridge::Tokio(aof_tx.clone()),
                                    &config,
                                    &started_at,
                                    &index_store,
                                    &dict_store,
                                    &wait_table,
                                    &ts_index,
                                    &pubsub,
                                    &hub_tx,
                                    &repl_state,
                                    &watch_registry,
                                    &client_registry,
                                    &acl_store,
                                    &acl_log,
                                    &slowlog,
                                    &latency,
                                    &compaction_tx,
                                    &cluster_state,
                                    &monitor_registry,
                                    &tracking_table,
                                    &peer_addr,
                                    client_id,
                                    authenticated_user.as_deref().unwrap_or("default"),
                                )
                            });
                            crate::lua_vm_blocking::run_lua_vm_blocking(
                                cmd.as_slice(),
                                &mut selected_db,
                                env,
                                aof_enabled,
                            )
                            .await
                        } else {
                            let cmd_start = std::time::Instant::now();
                            let r = commands::execute_command(Ctx {
                                cmd,
                                store: &store,
                                lastsave: &lastsave,
                                selected_db: &mut selected_db,
                                aof_tx: &aof_tx,
                                aof_enabled,
                                config: &config,
                                started_at: &started_at,
                                index_store: &index_store,
                                dict_store: &dict_store,
                                wait_table: &wait_table,
                                ts_index: &ts_index,
                                pubsub: &pubsub,
                                hub_tx: &hub_tx,
                                repl_state: &repl_state,
                                watch_registry: watch_registry.as_ref(),
                                client_id,
                                client_registry: &client_registry,
                                resp: resp_version,
                                acl_store: &acl_store,
                                acl_log: &acl_log,
                                slowlog: &slowlog,
                                latency: &latency,
                                compaction_tx: &compaction_tx,
                                cluster: &cluster_state,
                                bypass_cluster_check: false,
                                apply_replication: false,
                                skip_write_side_effects: false,
                                peer_addr: &peer_addr,
                                monitor_registry: &monitor_registry,
                                authenticated_user: authenticated_user
                                    .as_deref()
                                    .unwrap_or("default"),
                                tracking_table: &tracking_table,
                                blocking: None,
                            });
                            (r, cmd_start.elapsed().as_micros() as u64)
                        };

                    #[cfg(not(feature = "lua"))]
                    let (result, elapsed_us) = {
                        let cmd_start = std::time::Instant::now();
                        let r = commands::execute_command(Ctx {
                            cmd,
                            store: &store,
                            lastsave: &lastsave,
                            selected_db: &mut selected_db,
                            aof_tx: &aof_tx,
                            aof_enabled,
                            config: &config,
                            started_at: &started_at,
                            index_store: &index_store,
                            dict_store: &dict_store,
                            wait_table: &wait_table,
                            ts_index: &ts_index,
                            pubsub: &pubsub,
                            hub_tx: &hub_tx,
                            repl_state: &repl_state,
                            watch_registry: watch_registry.as_ref(),
                            client_id,
                            client_registry: &client_registry,
                            resp: resp_version,
                            acl_store: &acl_store,
                            acl_log: &acl_log,
                            slowlog: &slowlog,
                            latency: &latency,
                            compaction_tx: &compaction_tx,
                            cluster: &cluster_state,
                            bypass_cluster_check: false,
                            apply_replication: false,
                            skip_write_side_effects: false,
                            peer_addr: &peer_addr,
                            monitor_registry: &monitor_registry,
                            authenticated_user: authenticated_user.as_deref().unwrap_or("default"),
                            tracking_table: &tracking_table,
                            blocking: None,
                        });
                        (r, cmd_start.elapsed().as_micros() as u64)
                    };

                    if elapsed_us >= slowlog.threshold_us() {
                        let client_name = client_registry
                            .get(client_id)
                            .map(|info| info.name.clone())
                            .unwrap_or_default();

                        slowlog.record(&raw_cmd, elapsed_us, &peer_addr, client_name);
                    }

                    result
                }
            };
            // MONITOR sees a transaction as it ran: `MULTI`, the commands —
            // each shown as it was run — and then `EXEC`.
            if let Some(word) = &transaction_word {
                crate::monitor::feed(
                    &monitor_registry,
                    &[word.as_slice()],
                    selected_db,
                    &peer_addr,
                );
                // And they are commands, to be counted.
                crate::cmdstats::record(
                    &cmd_name,
                    &[word.as_slice()],
                    &response,
                    std::time::Duration::ZERO,
                );
            }
            out_buf.extend_from_slice(&response);
            parse_offset += consumed;
        }
        private_query_buffer |= progress.take_private_query_buffer();
        if private_query_buffer && !was_private_query_buffer {
            last_query_buffer_activity = Instant::now();
        }
        let reserve_hint = progress.take_reserve_hint();
        if private_query_buffer
            && !was_private_query_buffer
            && parse_buf.capacity() < resp::parse::PRIVATE_QUERY_BUFFER_MIN_CAPACITY
        {
            parse_buf.reserve(
                resp::parse::PRIVATE_QUERY_BUFFER_MIN_CAPACITY.saturating_sub(parse_buf.len()),
            );
        }
        if reserve_hint > parse_buf.capacity() {
            parse_buf.reserve(reserve_hint.saturating_sub(parse_buf.len()));
        }
        query_buffer_peak = query_buffer_peak.max(reserve_hint);
        reply_gate.settle(&mut out_buf);
        crate::tracking::after_command(
            &tracking_table,
            &client_registry,
            client_id,
            &activity,
            &mut out_buf,
        );
        // Told what was wrong with it, and then the connection is closed:
        // there is no telling where in this stream the next request starts.
        if let Some(e) = protocol_error {
            log_verbose!(
                "Client id={client_id}: protocol error: {e} — closing the connection. The \
                 client sent something that is not RESP: often a wrong port, TLS on a \
                 plaintext port, or HTTP"
            );
            out_buf.extend_from_slice(format!("-ERR Protocol error: {e}\r\n").as_bytes());
            quit_requested = true;
        }

        parse_start = parse_offset;
        if parse_start == parse_buf.len() {
            parse_buf.clear();
            parse_start = 0;
            // A buffer that grew to hold one large request is given back at the
            // end of the batch that finished it, rather than kept at that size
            // for the life of the connection. This is the batch boundary: the
            // request has been parsed and answered, no read is in flight, and
            // `parse_buf` is owned by this loop — so rebuilding it cannot
            // invalidate a pending io_uring operation. It happens here even if
            // the client then goes idle, which is what the idle-driven resize
            // above cannot reach: that one only runs after the *next* read.
            if parse_buf.capacity() > BIG_BUFFER {
                parse_buf = Vec::with_capacity(READ_BUFFER);
                query_buffer_peak = 0;
            }
        } else if parse_start > 0 && parse_start > parse_buf.len() / 2 {
            let remaining = parse_buf.len() - parse_start;
            parse_buf.copy_within(parse_start.., 0);
            parse_buf.truncate(remaining);
            parse_start = 0;
        }
        query_buffer.update(
            parse_buf.len().saturating_sub(parse_start),
            parse_buf.capacity().saturating_sub(parse_buf.len()),
            private_query_buffer,
        );

        // Flush output — swap with spare to avoid malloc/free per write cycle.
        // Pushes are no longer drained here; the forwarder task owns them, so
        // they reach the client immediately instead of waiting for the next
        // command. The guard keeps the two writers from interleaving.
        activity.note_flush(out_buf.len(), commands_before);
        activity.note_tx(tx.active.then_some(tx.queue.len()), tx.watched_keys.len());
        // Replies to writes go out once their records are in the AOF (#147).
        if std::mem::take(&mut aof_wait) {
            crate::aof::written(&aof_tx).await;
        }
        if !out_buf.is_empty() {
            std::mem::swap(&mut out_buf, &mut spare_buf);
            crate::stats::TOTAL_NET_OUTPUT_BYTES
                .fetch_add(spare_buf.len() as u64, std::sync::atomic::Ordering::Relaxed);
            crate::stats::record_write();
            let _w = write_guard.lock().await;
            let (res, returned) = stream.write_all(spare_buf).await;
            drop(_w);
            spare_buf = returned;
            spare_buf.clear();
            // Nor is one that grew to hold one large reply.
            if spare_buf.capacity() > BIG_BUFFER {
                spare_buf = Vec::with_capacity(65536);
            }
            res?;
        }
        // The replies to SUBSCRIBE are out: messages may follow them.
        if let Some(subs) = &subscriptions {
            subs.flushed();
        }

        // QUIT: the +OK is on the wire now, so drop the connection.
        if quit_requested {
            return Ok(());
        }

        match connection_handoff {
            None => {}
            Some(ConnectionHandoff::Replica { legacy_sync, psync }) => {
                parse_buf.drain(0..parse_start);

                let listening_port = client_registry
                    .get(client_id)
                    .map(|info| info.replica_listening_port)
                    .filter(|p| *p != 0);
                handle_replica_connection_uring(
                    stream,
                    Arc::clone(&store),
                    Arc::clone(&index_store),
                    hub_tx.clone(),
                    Arc::clone(&repl_state),
                    peer_addr.clone(),
                    legacy_sync,
                    listening_port,
                    psync,
                )
                .await?;

                return Ok(());
            }
        }
    }
    // Release key-restriction slot if this connection held one.
    if key_restricted {
        crate::acl::RESTRICTED_KEY_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
    if cmd_restricted {
        crate::acl::RESTRICTED_CMD_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
    // What was kept for tracking goes with `_guard`.
    Ok(())
}
