use crate::commands;
use crate::pubsub::PubSubHub;
use crate::resp;
use crate::store;
use crate::store::Key;
use crate::types::SharedBytes;
use crate::vector;
use crate::watch::WatchNotifier;
use commands::ctx::Ctx;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

/// Hooks shared by plain TCP and TLS streams before `split`.
pub trait PrepareIoStream {
    fn prepare_io(&mut self) -> std::io::Result<()>;
}

impl PrepareIoStream for TcpStream {
    fn prepare_io(&mut self) -> std::io::Result<()> {
        self.set_nodelay(true)
    }
}

#[cfg(feature = "tls")]
impl PrepareIoStream for tokio_rustls::server::TlsStream<TcpStream> {
    fn prepare_io(&mut self) -> std::io::Result<()> {
        self.get_mut().0.set_nodelay(true)
    }
}

#[inline(always)]
fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Same first-byte filter as server_uring — avoids String allocation
/// on the fast path for GET/SET/DEL/INCR and all other normal commands.
#[inline(always)]
fn needs_slow_path(cmd0: &[u8]) -> bool {
    let first = cmd0.first().copied().unwrap_or(0) | 0x20;
    if !matches!(
        first,
        b's' | b'p' | b'm' | b'd' | b'w' | b'u' | b'e' | b'a' | b'h' | b'r'
    ) {
        return false;
    }
    ascii_eq_ignore_case(cmd0, b"SUBSCRIBE")
        || ascii_eq_ignore_case(cmd0, b"PSUBSCRIBE")
        || ascii_eq_ignore_case(cmd0, b"SSUBSCRIBE")
        // The UNSUBSCRIBE family only ever reached the RESP2 subscriber loop
        // before, so it never needed the slow path. RESP3 clients now handle
        // these inline in the main loop, which lives on the slow path. Each
        // check is a length comparison first, so hot commands sharing the same
        // first byte (SET, GET, ...) bail out immediately.
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

/// Await a parked blocking command.
///
/// When a handler parks, the connection loop awaits the delivery and then
/// re-dispatches the *same* command — it simply does not advance `parse_offset`,
/// so the next turn of the parse loop re-reads the same bytes out of `buffer`.
/// That is what lets a single `Ctx` construction site serve both the initial
/// dispatch and the resume, with no extra allocation on the non-blocking path.
///
/// `Ok(())` means the delivery is stored and the caller should re-dispatch the
/// same command; `Err(reply)` means reply with this instead and move on.
#[allow(clippy::too_many_arguments)]
async fn park_blocking<S, R>(
    mut wait: crate::commands::blocking::BlockingWait,
    slot: &mut crate::commands::blocking::BlockingSlot,
    writer: &Arc<Mutex<tokio::io::WriteHalf<S>>>,
    combined: &mut Vec<u8>,
    subscriptions: Option<&crate::pubsub::Subscriptions>,
    waiter: &mut Waiter<'_, R>,
) -> Result<(), Vec<u8>>
where
    S: AsyncWrite + Unpin + Send,
    R: tokio::io::AsyncRead + Unpin,
{
    if slot.in_resume() && !wait.may_repark() {
        // A resumed handler parked again — it forgot its resume check. Reply
        // as if the wait timed out rather than looping forever.
        slot.clear_resume();
        return Err(crate::resp::write::resp_null_array());
    }

    // Flush replies to anything pipelined ahead of this command before we go to
    // sleep. Without this a client sending `SET a 1` and `BLPOP k 0` in one
    // packet would not see the `+OK` until the BLPOP finally returned.
    if write_tcp_batch(writer, combined).await.is_err() {
        // Socket is gone. Don't park on a dead connection — return nothing and
        // let the batch write at the end of the loop tear it down.
        return Err(Vec::new());
    }
    if let Some(subs) = subscriptions {
        subs.flushed();
    }

    let block_state = wait.take_state();
    let (unblock_tx, mut unblock_rx) = tokio::sync::oneshot::channel();
    waiter
        .client_registry
        .set_blocked(waiter.client_id, Some(unblock_tx));
    let delivery = {
        let waiting = wait.wait();
        tokio::pin!(waiting);
        let mut temp = [0u8; 4096];
        let mut unblockable = true;
        loop {
            tokio::select! {
                delivery = &mut waiting => break delivery,
                // The client's side of the connection, while it waits: more
                // commands, which wait their turn — or the end of it. A
                // client that has gone must not be left in line for an
                // element nobody will receive.
                read = waiter.reader.read(&mut temp) => match read {
                    Ok(n) if n > 0 => {
                        crate::stats::TOTAL_NET_INPUT_BYTES.fetch_add(n as u64, Ordering::Relaxed);
                        waiter.carried.extend_from_slice(&temp[..n]);
                    }
                    _ => {
                        waiter.interrupted.gone = true;
                        break None;
                    }
                },
                _ = &mut *waiter.kill_rx => {
                    waiter.interrupted.gone = true;
                    break None;
                }
                how = &mut unblock_rx, if unblockable => match how {
                    Ok(how) => {
                        waiter.interrupted.unblocked_with_error =
                            how == crate::commands::blocking::Unblock::Error;
                        break None;
                    }
                    Err(_) => unblockable = false,
                },
            }
        }
    };
    waiter.client_registry.set_blocked(waiter.client_id, None);
    // However it ended, the command is dispatched again: with what it was
    // waiting for, or as if its time had run out — which is also how it
    // leaves the line it was standing in.
    slot.set_pending(delivery, block_state);
    Ok(())
}

/// What a connection lends its parked command: the means of noticing that
/// the wait should end early.
struct Waiter<'a, R> {
    reader: &'a mut R,
    /// What the client sent while the command waited, to be parsed after it.
    carried: &'a mut Vec<u8>,
    kill_rx: &'a mut crate::client_registry::KillRx,
    client_registry: &'a crate::client_registry::ClientRegistry,
    client_id: u64,
    interrupted: &'a mut crate::commands::blocking::Interrupted,
}

/// A connection's buffer that has grown past this is given back once it is
/// empty, rather than kept.
const BIG_BUFFER: usize = 256 * 1024;

/// Write one response batch straight to TCP (normal client hot path).
#[inline]
async fn write_tcp_batch<S>(
    w: &Arc<Mutex<tokio::io::WriteHalf<S>>>,
    combined: &mut Vec<u8>,
) -> Result<(), std::io::Error>
where
    S: AsyncWrite + Unpin + Send,
{
    if combined.is_empty() {
        return Ok(());
    }
    crate::stats::TOTAL_NET_OUTPUT_BYTES.fetch_add(combined.len() as u64, Ordering::Relaxed);
    let mut g = w.lock().await;
    g.write_all(combined).await?;
    combined.clear();
    // Nor is one that grew to hold one large reply.
    if combined.capacity() > BIG_BUFFER {
        *combined = Vec::with_capacity(8192);
    }
    Ok(())
}

/// Write a subscribed connection's messages as they arrive. Ends when the
/// connection's subscriptions do, which stops it.
fn spawn_pubsub_forwarder<S>(
    writer: Arc<Mutex<tokio::io::WriteHalf<S>>>,
    mut outbox: crate::pubsub::Outbox,
    client_registry: crate::client_registry::ClientRegistry,
    client_id: u64,
) -> tokio::task::JoinHandle<()>
where
    S: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(batch) = outbox.next().await {
            let mut g = writer.lock().await;
            // With the writer held: see `Batch::encode`.
            let frames = batch.encode(client_registry.get_resp(client_id));
            if frames.is_empty() {
                continue;
            }
            crate::stats::TOTAL_NET_OUTPUT_BYTES.fetch_add(frames.len() as u64, Ordering::Relaxed);
            if g.write_all(&frames).await.is_err() {
                break;
            }
        }
    })
}

/// A connection's subscriptions, made at its first SUBSCRIBE, with the task
/// that writes its messages.
fn open_subscriptions<S>(
    pubsub: &crate::pubsub::PubSubHub,
    client_registry: &crate::client_registry::ClientRegistry,
    client_id: u64,
    writer: &Arc<Mutex<tokio::io::WriteHalf<S>>>,
) -> crate::pubsub::Subscriptions
where
    S: AsyncWrite + Unpin + Send + 'static,
{
    let (mut subs, outbox) = crate::pubsub::connect(pubsub, Arc::clone(client_registry), client_id);
    subs.forwarded_by(spawn_pubsub_forwarder(
        Arc::clone(writer),
        outbox,
        Arc::clone(client_registry),
        client_id,
    ));
    subs
}

/// Replica / monitor: channel-fed writer multiplexes onto `writer`.
fn spawn_mux_to_socket<S>(
    writer: Arc<Mutex<tokio::io::WriteHalf<S>>>,
    mut resp_rx: mpsc::Receiver<Vec<u8>>,
) -> tokio::task::JoinHandle<()>
where
    S: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut out_buf = Vec::with_capacity(65536);
        while let Some(r) = resp_rx.recv().await {
            out_buf.extend_from_slice(&r);
            while let Ok(r2) = resp_rx.try_recv() {
                out_buf.extend_from_slice(&r2);
            }
            crate::stats::TOTAL_NET_OUTPUT_BYTES.fetch_add(out_buf.len() as u64, Ordering::Relaxed);
            let mut g = writer.lock().await;
            if g.write_all(&out_buf).await.is_err() {
                break;
            }
            out_buf.clear();
        }
    })
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
        || crate::monitor::MONITOR_COUNT.load(Ordering::Relaxed) > 0
        || crate::tracking::TRACKING_CLIENT_COUNT.load(Ordering::Relaxed) > 0
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

#[allow(clippy::too_many_arguments)]
pub async fn handle_client<S>(
    mut stream: S,
    store: store::Store,
    expiry_store: store::ExpiryStore,
    lastsave: Arc<RwLock<i64>>,
    aof_tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    aof_enabled: bool,
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
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + PrepareIoStream + 'static,
{
    stream.prepare_io()?;
    if let Some(denied) = crate::protected_mode::refusal(&acl_store, &peer_addr) {
        let _ = stream.write_all(denied).await;
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

    let (mut reader, writer) = split(stream);
    let writer = Arc::new(Mutex::new(writer));

    // Push notifications (CLIENT TRACKING invalidations, etc.) need a dedicated
    // forwarder task so they are delivered immediately — not held until the next
    // client command arrives.
    let (push_tx, mut push_rx) = mpsc::channel::<Vec<u8>>(512);
    client_registry.register_push_tx(client_id, push_tx.clone());
    let writer_for_push = Arc::clone(&writer);
    let push_forwarder = tokio::spawn(async move {
        while let Some(msg) = push_rx.recv().await {
            let mut g = writer_for_push.lock().await;
            crate::stats::TOTAL_NET_OUTPUT_BYTES.fetch_add(msg.len() as u64, Ordering::Relaxed);
            if g.write_all(&msg).await.is_err() {
                break;
            }
        }
    });

    // ── Connection state ─────────────────────────────────────────────────
    let mut buffer = Vec::with_capacity(4 * 1024);
    let mut private_query_buffer = false;
    query_buffer.update(0, buffer.capacity(), private_query_buffer);
    let mut last_query_buffer_activity = Instant::now();
    let mut last_query_buffer_resize_check = Instant::now();
    let query_buffer_idle_resize_after =
        std::time::Duration::from_millis(config.query_buffer_idle_resize_after_ms);
    let mut query_buffer_peak = 0usize;
    let mut temp = [0u8; 4096];
    let mut read_start = 0usize;
    // How far the request still arriving has been read.
    let mut progress = resp::parse::Progress::default();
    let mut selected_db: usize = 0;
    // This connection's subscriptions, while it has any. `None` otherwise, so
    // that a connection that never uses pub/sub allocates nothing for it.
    let mut subscriptions: Option<crate::pubsub::Subscriptions> = None;
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

    use crate::commands::transaction::TxState;
    let mut tx = TxState::new();
    let dirty_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut combined = Vec::with_capacity(8192);
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

    // What arrived while a blocking command of this connection waited, and
    // how such a wait was cut short. See `park_blocking`.
    let mut carried: Vec<u8> = Vec::new();
    let mut interrupted = crate::commands::blocking::Interrupted::default();

    loop {
        let was_private_at_read = private_query_buffer;
        if carried.is_empty() {
            // Redis reclaims private query buffers from its client cron, and the
            // maintenance step below is the equivalent. It can only do anything
            // when the buffer is private and has spare capacity to reclaim, so
            // the timer arm is only built then; every other read — including
            // every read of a client that is not sitting on a private buffer —
            // races just the socket and `kill_rx`.
            let should_maintain_query_buffer = private_query_buffer
                && buffer.capacity().saturating_sub(buffer.len())
                    > resp::parse::QUERY_BUFFER_MIN_SPARE_FOR_RESIZE;
            let n = if should_maintain_query_buffer {
                let resize_deadline =
                    if buffer.capacity() > resp::parse::QUERY_BUFFER_RESIZE_THRESHOLD {
                        tokio::time::Instant::from_std(
                            last_query_buffer_resize_check
                                + resp::parse::QUERY_BUFFER_PEAK_SAMPLE_INTERVAL,
                        )
                    } else {
                        tokio::time::Instant::from_std(
                            last_query_buffer_activity
                                + query_buffer_idle_resize_after
                                + tokio::time::Duration::from_millis(10),
                        )
                    };
                tokio::select! {
                    // Normal case: data arrived from the client
                    result = reader.read(&mut temp) => {
                        match result? {
                            0 => break,
                            n => n,
                        }
                    }
                    // CLIENT KILL was called — close this connection immediately
                    _ = &mut kill_rx => {
                        // Send a final error so the client knows it was killed,
                        // then break. Redis sends no response on kill — we match that.
                        break;
                    }
                    // Tokio's socket read is cancellation-safe, so this deadline
                    // can be checked without abandoning an in-flight operation.
                    _ = tokio::time::sleep_until(resize_deadline) => {
                        let resized = resp::parse::resize_private_query_buffer_after(
                            &mut buffer,
                            read_start,
                            &mut private_query_buffer,
                            last_query_buffer_activity.elapsed(),
                            query_buffer_idle_resize_after,
                            query_buffer_peak,
                            progress.minimum_capacity(),
                        );
                        if resized {
                            read_start = 0;
                            query_buffer.update(
                                buffer.len(),
                                buffer.capacity().saturating_sub(buffer.len()),
                                private_query_buffer,
                            );
                        }
                        query_buffer_peak = buffer
                            .len()
                            .saturating_sub(read_start)
                            .max(progress.minimum_capacity());
                        last_query_buffer_resize_check = Instant::now();
                        continue;
                    }
                }
            } else {
                tokio::select! {
                    // Normal case: data arrived from the client
                    result = reader.read(&mut temp) => {
                        match result? {
                            0 => break,
                            n => n,
                        }
                    }
                    // CLIENT KILL was called — close this connection immediately
                    _ = &mut kill_rx => {
                        // Send a final error so the client knows it was killed,
                        // then break. Redis sends no response on kill — we match that.
                        break;
                    }
                }
            };
            if was_private_at_read {
                last_query_buffer_activity = Instant::now();
            }
            crate::stats::TOTAL_NET_INPUT_BYTES
                .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
            buffer.extend_from_slice(&temp[..n]);
        } else {
            buffer.append(&mut carried);
        }
        query_buffer_peak = query_buffer_peak.max(buffer.len().saturating_sub(read_start));
        query_buffer.update(
            buffer.len().saturating_sub(read_start),
            buffer.capacity().saturating_sub(buffer.len()),
            private_query_buffer,
        );

        let mut parse_offset = read_start;
        let mut connection_handoff: Option<ConnectionHandoff> = None;
        let mut quit_requested = false;
        // Outside the parse loop: a park in one turn hands its delivery to the
        // re-dispatch on the next turn.
        let mut blocking_slot = crate::commands::blocking::BlockingSlot::default();

        // What the client sent that no request can begin with, if it did.
        let mut protocol_error: Option<String> = None;
        let query_buffer_bytes = buffer.len();
        let query_buffer_free = buffer.capacity().saturating_sub(buffer.len());
        let was_private_query_buffer = private_query_buffer;
        while let Some((consumed, cmd)) =
            match progress.next_request(&mut buffer, parse_offset, authenticated_user.is_some()) {
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
            // ── QUIT ─────────────────────────────────────────────────────────
            // Reply +OK and close the connection. Handled in the connection
            // loop rather than the dispatch table because closing the socket
            // is a connection-level concern `execute_command` can't express.
            //
            // QUIT appeared in both the pre-auth allowlist and
            // `is_always_allowed_bytes` — the server treated it as special —
            // but had no dispatch arm anywhere, so it fell through to the
            // generic fallback, returned `-ERR unknown command`, and left the
            // connection open. A client pipelining `AUTH … / <cmd> / QUIT` got
            // a spurious trailing `-ERR` and then blocked until its own
            // client-side timeout.
            //
            // Anything pipelined after QUIT is intentionally not executed —
            // the connection is closing, which is what Redis does too.
            //
            // The `len() == 4` gate keeps this off the hot path: it rejects
            // GET/SET/DEL in a single compare, before the byte-wise loop.
            if cmd[0].len() == 4 && ascii_eq_ignore_case(cmd[0], b"QUIT") {
                combined.extend_from_slice(b"+OK\r\n");
                parse_offset += consumed;
                quit_requested = true;
                break;
            }
            // For `CLIENT LIST`'s `cmd=` and `db=`.
            activity.record(&cmd, selected_db);
            // The command before this one is over: its reply is settled, and
            // what it left to tell tracking clients follows it.
            reply_gate.settle(&mut combined);
            crate::tracking::after_command(
                &tracking_table,
                &client_registry,
                client_id,
                &activity,
                &mut combined,
            );
            reply_gate.begin(&cmd, &mut combined);
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
            if crate::pause::PAUSE_UNTIL_MS.load(std::sync::atomic::Ordering::Relaxed) != 0
                && crate::pause::holds(&cmd, &tx.queue)
            {
                let _held = crate::pause::Held::new();
                while crate::pause::holds(&cmd, &tx.queue) {
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }
            }
            // ══════════════════════════════════════════════════════════════
            // FAST PATH — identical to v0.1.10: parse → execute → append.
            // Zero heap allocations for command name, zero string compares.
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
                            &expiry_store,
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
                        crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed),
                    )
                    .await;
                    if slowlog_enabled && elapsed_us >= slowlog.threshold_us() {
                        let elapsed_ms = (elapsed_us / 1000) as u32;
                        latency.record(
                            if elapsed_ms < 1 {
                                "fast-command"
                            } else {
                                "command"
                            },
                            elapsed_ms,
                        );
                        let client_name = client_registry
                            .get(client_id)
                            .map(|info| info.name.clone())
                            .unwrap_or_default();
                        slowlog.record(&cmd, elapsed_us, &peer_addr, client_name);
                    }
                    combined.extend_from_slice(&response);
                    parse_offset += consumed;
                    continue;
                }

                if !slowlog_enabled {
                    let mut response = commands::execute_command(Ctx {
                        cmd,
                        store: &store,
                        expiry_store: &expiry_store,
                        lastsave: &lastsave,
                        selected_db: &mut selected_db,
                        aof_tx: &aof_tx,
                        aof_enabled: crate::aof::AOF_ENABLED
                            .load(std::sync::atomic::Ordering::Relaxed),
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
                        match park_blocking(
                            wait,
                            &mut blocking_slot,
                            &writer,
                            &mut combined,
                            subscriptions.as_ref(),
                            &mut Waiter {
                                reader: &mut reader,
                                carried: &mut carried,
                                kill_rx: &mut kill_rx,
                                client_registry: &client_registry,
                                client_id,
                                interrupted: &mut interrupted,
                            },
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
                    combined.extend_from_slice(&response);
                    parse_offset += consumed;
                    continue;
                }
                let mut waited_from: Option<u64> = None;
                let mut response = commands::execute_command(Ctx {
                    cmd,
                    store: &store,
                    expiry_store: &expiry_store,
                    lastsave: &lastsave,
                    selected_db: &mut selected_db,
                    aof_tx: &aof_tx,
                    aof_enabled: crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed),
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
                    match park_blocking(
                        wait,
                        &mut blocking_slot,
                        &writer,
                        &mut combined,
                        subscriptions.as_ref(),
                        &mut Waiter {
                            reader: &mut reader,
                            carried: &mut carried,
                            kill_rx: &mut kill_rx,
                            client_registry: &client_registry,
                            client_id,
                            interrupted: &mut interrupted,
                        },
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
                    let elapsed_ms = (elapsed_us / 1000) as u32;
                    latency.record(
                        if elapsed_ms < 1 {
                            "fast-command"
                        } else {
                            "command"
                        },
                        elapsed_ms,
                    );
                    let client_name = client_registry
                        .get(client_id)
                        .map(|info| info.name.clone())
                        .unwrap_or_default();
                    // The command is still where it was read: it is read
                    // again for the log, now that it has turned out slow,
                    // where every command used to be copied in case it did.
                    if let Ok(Some((_, slow_cmd))) =
                        resp::parse::next_request(&mut buffer, parse_offset)
                    {
                        slowlog.record(&slow_cmd, elapsed_us, &peer_addr, client_name);
                    }
                }
                combined.extend_from_slice(&response);
                parse_offset += consumed;
                continue;
            }

            // ══════════════════════════════════════════════════════════════
            // SLOW PATH — auth, transactions, subscribe, psync
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
                combined.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
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
                    combined
                        .extend_from_slice(format!("-{}\r\n", denied.message(username)).as_bytes());
                    parse_offset += consumed;
                    continue;
                }
            }

            // ── Subscriptions ─────────────────────────────────────────────
            // A subscribed connection stays in this loop, with a task of its
            // own writing its messages. Under RESP3 that is all there is to
            // it; under RESP2 it may run nothing but the commands below while
            // it has subscriptions. Inside MULTI they are queued like any other
            // command, and EXEC runs them (#23).
            if crate::pubsub::is_subscription_command(cmd_name.as_str()) && !tx.active {
                let subs = subscriptions.get_or_insert_with(|| {
                    open_subscriptions(&pubsub, &client_registry, client_id, &writer)
                });
                combined.extend_from_slice(&subs.handle(&cmd, resp_version));
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
                combined.extend_from_slice(&reply);
                parse_offset += consumed;
                continue;
            }

            // ── Connection handoffs (break parse loop so `buffer` can be drained/moved) ──
            // A replica whose dataset may already be this master's is first
            // offered the fingerprint (see `replication::digest`), and the
            // connection stays a client until it asks again.
            if cmd_name == "PSYNC"
                && let Some(offer) = repl_state.digest_offer(&client_registry, client_id, &cmd)
            {
                combined.extend_from_slice(&offer);
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
                combined.extend_from_slice(b"+OK\r\n");
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
                                commands::helpers::lazy_expire_key(
                                    &store,
                                    &expiry_store,
                                    selected_db,
                                    key,
                                );
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
                        commands::helpers::lazy_expire_key(&store, &expiry_store, *db, key);
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
                            if dirty_flag.load(Ordering::Acquire) || crate::pause::handoff() {
                                drop(guards);
                                watch_registry.as_ref().unwatch_all(
                                    &tx.watched_keys
                                        .iter()
                                        .map(|_| Arc::clone(&dirty_flag))
                                        .collect::<Vec<_>>(),
                                );
                                tx.reset();
                                dirty_flag.store(false, Ordering::Release);
                                if resp_version == 3 {
                                    crate::resp::write::resp_null_resp3()
                                } else {
                                    crate::resp::write::resp_null_array()
                                }
                            } else {
                                for (key, value) in sets.iter() {
                                    let shard = store::get_shard(&store[selected_db], key);
                                    let ptr = Arc::as_ptr(shard) as usize;
                                    if let Some((_, guard)) =
                                        guards.iter_mut().find(|(p, _)| *p == ptr)
                                    {
                                        guard.insert(
                                            Key::from(key),
                                            crate::types::KiviDBValue::String(SharedBytes::from(
                                                value.as_slice(),
                                            )),
                                        );
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
                                // Release before touching expiry_store, matching SET's own
                                // handler: it's a separate lock, no need to hold both at once.
                                drop(guards);

                                for (key, _) in sets.iter() {
                                    let exp_shard =
                                        store::get_expiry_shard(&expiry_store[selected_db], key);
                                    if exp_shard.read().contains_key(key.as_str()) {
                                        exp_shard.write().remove(key.as_str());
                                    }
                                }

                                let count = sets.len();
                                let mut exec_out = format!("*{count}\r\n").into_bytes();
                                for (key, value) in sets.iter() {
                                    let raw_cmd =
                                        [b"SET".to_vec(), key.clone().into_bytes(), value.clone()];
                                    let cmd_ctx = Ctx {
                                        cmd: raw_cmd.iter().map(|v| v.as_slice()).collect(),
                                        store: &store,
                                        expiry_store: &expiry_store,
                                        lastsave: &lastsave,
                                        selected_db: &mut selected_db,
                                        aof_tx: &aof_tx,
                                        aof_enabled: crate::aof::AOF_ENABLED
                                            .load(std::sync::atomic::Ordering::Relaxed),
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
                                dirty_flag.store(false, Ordering::Release);
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
                                // SUBSCRIBE and its kind, queued: run as they run
                                // outside a transaction, their reply this command's
                                // place in EXEC's, as in Redis (#23).
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
                                            &writer,
                                        )
                                    });
                                    exec_out.extend_from_slice(&subs.handle(&args, resp_version));
                                    if subs.total() == 0 {
                                        subscriptions = None;
                                    }
                                    continue;
                                }
                                let raw_cmd = queued_cmd.clone();
                                #[cfg(feature = "lua")]
                                let exec_slices: Vec<&[u8]> =
                                    queued_cmd.iter().map(|b| b.as_slice()).collect();

                                #[cfg(feature = "lua")]
                                let (resp, elapsed_us) = {
                                    let cmd_start = std::time::Instant::now();
                                    let r = commands::execute_command(Ctx {
                                        cmd: exec_slices,
                                        store: &store,
                                        expiry_store: &expiry_store,
                                        lastsave: &lastsave,
                                        selected_db: &mut selected_db,
                                        aof_tx: &aof_tx,
                                        aof_enabled: crate::aof::AOF_ENABLED
                                            .load(std::sync::atomic::Ordering::Relaxed),
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
                                    let r = commands::execute_command(Ctx {
                                        cmd: queued_cmd.iter().map(|b| b.as_slice()).collect(),
                                        store: &store,
                                        expiry_store: &expiry_store,
                                        lastsave: &lastsave,
                                        selected_db: &mut selected_db,
                                        aof_tx: &aof_tx,
                                        aof_enabled: crate::aof::AOF_ENABLED
                                            .load(std::sync::atomic::Ordering::Relaxed),
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
                                    let elapsed_ms = (elapsed_us / 1000) as u32;
                                    latency.record(
                                        if elapsed_ms < 1 {
                                            "fast-command"
                                        } else {
                                            "command"
                                        },
                                        elapsed_ms,
                                    );
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
                                resp::write::resp_simple("OK")
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
                    //
                    // Protover sets per-connection RESP (`client_registry.resp`) and is echoed
                    // in the `proto` field (Redis-compatible HELLO map semantics).

                    let mut i = 1usize; // index into cmd, past "HELLO"

                    let mut proto_reply: i64 = resp_version as i64;

                    // Optional protocol version
                    if let Some(ver_bytes) = cmd.get(i) {
                        let ver = std::str::from_utf8(ver_bytes).unwrap_or("2");
                        // protover 0 is a special case meaning "RESET to defaults"
                        if ver != "0" && ver != "1" && ver != "2" && ver != "3" {
                            combined.extend_from_slice(
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
                                // "0", "1", "2" — Redis resets/alternate protos to RESP2 wire.
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
                                    combined.extend_from_slice(
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
                                combined.extend_from_slice(
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
                        combined.extend_from_slice(
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
                    combined.extend_from_slice(&response);
                    parse_offset += consumed;
                    continue;
                }

                // In the slow path match in server.rs, add WAITAOF as a special case:
                "WAITAOF" => {
                    if cmd.len() < 4 {
                        b"-ERR wrong number of arguments for 'waitaof' command\r\n".to_vec()
                    } else if !aof_enabled {
                        b"*2\r\n:1\r\n:0\r\n".to_vec()
                    } else {
                        let num_local: i64 =
                            std::str::from_utf8(cmd.get(1).copied().unwrap_or(b"0"))
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0);
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
                            if fsynced >= target {
                                if num_local > 0 {
                                    break b"*2\r\n:1\r\n:0\r\n".to_vec();
                                } else {
                                    break b"*2\r\n:0\r\n:0\r\n".to_vec();
                                }
                            }
                            if tokio::time::Instant::now() >= deadline {
                                break b"*2\r\n:0\r\n:0\r\n".to_vec();
                            }
                            // Async sleep — yields to tokio scheduler, lets AOF writer task run
                            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
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
                            // What "caught up" means is settled per replica:
                            // each must acknowledge everything *it* had been
                            // sent as of now. The barrier makes "as of now"
                            // exact — it resolves only after every write
                            // already handed to the hub has been fanned out.
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
                                if acked >= num_replicas {
                                    break crate::resp::write::resp_integer(acked).to_vec();
                                }
                                if tokio::time::Instant::now() >= deadline {
                                    break crate::resp::write::resp_integer(acked).to_vec();
                                }
                                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
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
                                    &expiry_store,
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
                                crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed),
                            )
                            .await
                        } else {
                            let cmd_start = std::time::Instant::now();
                            let r = commands::execute_command(Ctx {
                                cmd,
                                store: &store,
                                expiry_store: &expiry_store,
                                lastsave: &lastsave,
                                selected_db: &mut selected_db,
                                aof_tx: &aof_tx,
                                aof_enabled: crate::aof::AOF_ENABLED
                                    .load(std::sync::atomic::Ordering::Relaxed),
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
                            expiry_store: &expiry_store,
                            lastsave: &lastsave,
                            selected_db: &mut selected_db,
                            aof_tx: &aof_tx,
                            aof_enabled: crate::aof::AOF_ENABLED
                                .load(std::sync::atomic::Ordering::Relaxed),
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
                        let elapsed_ms = (elapsed_us / 1000) as u32;
                        latency.record(
                            if elapsed_ms < 1 {
                                "fast-command"
                            } else {
                                "command"
                            },
                            elapsed_ms,
                        );
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
            combined.extend_from_slice(&response);
            parse_offset += consumed;
        }
        private_query_buffer |= progress.take_private_query_buffer();
        if private_query_buffer && !was_private_query_buffer {
            last_query_buffer_activity = Instant::now();
        }
        let reserve_hint = progress.take_reserve_hint();
        if private_query_buffer
            && !was_private_query_buffer
            && buffer.capacity() < resp::parse::PRIVATE_QUERY_BUFFER_MIN_CAPACITY
        {
            buffer.reserve(
                resp::parse::PRIVATE_QUERY_BUFFER_MIN_CAPACITY.saturating_sub(buffer.len()),
            );
        }
        if reserve_hint > buffer.capacity() {
            buffer.reserve(reserve_hint.saturating_sub(buffer.len()));
        }
        query_buffer_peak = query_buffer_peak.max(reserve_hint);
        reply_gate.settle(&mut combined);
        crate::tracking::after_command(
            &tracking_table,
            &client_registry,
            client_id,
            &activity,
            &mut combined,
        );
        // Told what was wrong with it, and then the connection is closed:
        // there is no telling where in this stream the next request starts.
        if let Some(e) = protocol_error {
            log_verbose!(
                "Client id={client_id}: protocol error: {e} — closing the connection. The \
                 client sent something that is not RESP: often a wrong port, TLS on a \
                 plaintext port, or HTTP"
            );
            combined.extend_from_slice(format!("-ERR Protocol error: {e}\r\n").as_bytes());
            quit_requested = true;
        }

        read_start = parse_offset;

        // Only compact when we've consumed a meaningful chunk AND there's
        // data left to preserve. This amortizes the memmove cost.
        if read_start > 0 && read_start > buffer.len() / 2 {
            buffer.drain(0..read_start);
            read_start = 0;
        }
        // A buffer that grew to hold one large request is not kept at that
        // size for the life of the connection.
        query_buffer.update(
            buffer.len().saturating_sub(read_start),
            buffer.capacity().saturating_sub(buffer.len()),
            private_query_buffer,
        );

        // One TCP write per read syscall batch (pipelining).
        if write_tcp_batch(&writer, &mut combined).await.is_err() {
            break;
        }
        // The replies to SUBSCRIBE are out: messages may follow them.
        if let Some(subs) = &subscriptions {
            subs.flushed();
        }

        // QUIT: the +OK is on the wire now, so drop the connection.
        if quit_requested {
            break;
        }

        match connection_handoff {
            None => {}
            Some(ConnectionHandoff::Replica { legacy_sync, psync }) => {
                push_forwarder.abort();

                let (resp_tx, resp_rx) = mpsc::channel::<Vec<u8>>(16384);
                let write_task = spawn_mux_to_socket(Arc::clone(&writer), resp_rx);

                buffer.drain(0..read_start);

                let listening_port = client_registry
                    .get(client_id)
                    .map(|info| info.replica_listening_port)
                    .filter(|p| *p != 0);
                client_registry.update(client_id, |info| info.push_tx = None);

                crate::replication::replica_client::handle_replica_connection(
                    reader,
                    resp_tx,
                    write_task,
                    store,
                    expiry_store,
                    lastsave,
                    aof_tx,
                    crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed),
                    config,
                    started_at,
                    index_store,
                    dict_store,
                    wait_table,
                    ts_index,
                    pubsub,
                    hub_tx,
                    repl_state,
                    peer_addr.clone(),
                    legacy_sync,
                    buffer,
                    listening_port,
                    psync,
                )
                .await?;
                return Ok(());
            }
        }
    }

    // If this connection held a key-restriction slot, release it.
    if key_restricted {
        crate::acl::RESTRICTED_KEY_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
    if cmd_restricted {
        crate::acl::RESTRICTED_CMD_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    push_forwarder.abort();

    // What was kept for tracking goes with `_guard`.
    Ok(())
}
