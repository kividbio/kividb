// src/replication/replica_client.rs
//
// Replica handshake and replication stream.
//
// Sequence:
//   1. TCP connect to master
//   2. AUTH [user] <pass>              →  +OK        (only with masterauth)
//   3. PING                            →  +PONG
//   4. REPLCONF listening-port <port>  →  +OK
//   5. REPLCONF capa psync2            →  +OK
//   6. PSYNC <repl_id> <offset+1>      →  +CONTINUE: the stream carries on
//      (PSYNC ? -1 the first time)         from where this replica is; skip to 9
//                                      →  +FULLRESYNC <repl_id> <offset>
//   7. Read the payload: "$<len>\r\n" then <len> raw bytes → staging store
//   8. Dataset guard, then swap staging in
//   9. Command stream: parse RESP inline, execute each write command
//
// Every step is bounded by `repl-timeout`. On any failure the session ends,
// the link is reported down, and the outer loop retries.

use crate::config::Config;
use crate::replication::{FullSyncGuard, HubTx, LinkState, ReplState};
use crate::resp::parse::parse_resp_command;
use crate::store::{self, SharedTsIndex, Store};
use crate::vector::{CompactionTx, DictStore, IndexStore};
use crate::wait_table::WaitTable;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

/// Owns the replica link state for the *current* replication task only.
///
/// Link state lives in one shared `ReplState`, but during a re-point
/// (`REPLICAOF` issued while already a slave) two sessions briefly coexist:
/// the newly-spawned task connects and starts streaming from the new master
/// before the superseded task notices its epoch is stale and returns. Without
/// the generation check here, the stale task's `Drop` fires *after* the new
/// task is up and marks the link down — so a replica that is fully caught up
/// and streaming from its new master reports `master_link_status:down`
/// indefinitely (found live: both sessions completed their handshake
/// successfully per the connection log, yet the flag stuck at `false`).
///
/// Every transition therefore only touches the shared state while this
/// guard's generation is still the live one. A superseded task's guard is a
/// no-op instead of overwriting whatever the current generation has set.
struct SlaveUpstreamGuard {
    rs: Arc<ReplState>,
    generation: u64,
}

impl SlaveUpstreamGuard {
    /// A connection attempt is starting. The link is *not* up yet: it is up
    /// once the dataset has been synced and the stream is flowing, which is
    /// [`Self::mark_synced`].
    fn connect(repl_state: &Arc<ReplState>, generation: u64) -> Self {
        let guard = Self {
            rs: Arc::clone(repl_state),
            generation,
        };
        guard.set(LinkState::Connecting);
        guard
    }

    fn set(&self, state: LinkState) {
        if self.rs.generation() == self.generation {
            self.rs.set_link_state(state);
        }
    }

    fn mark_syncing(&self) {
        self.set(LinkState::Sync);
    }

    fn mark_synced(&self) {
        self.set(LinkState::Connected);
    }
}

impl Drop for SlaveUpstreamGuard {
    fn drop(&mut self) {
        self.set(LinkState::Connect);
    }
}

/// Everything a replication client needs, bundled so it can be started from
/// anywhere: `--replicaof` at boot, `REPLICAOF`, `CLUSTER REPLICATE`, and the
/// cluster bus when a failover re-points this node.
#[derive(Clone)]
pub struct ReplicaStarter {
    pub store: Store,
    pub index_store: IndexStore,
    pub dict_store: DictStore,
    pub wait_table: Arc<WaitTable>,
    pub ts_index: SharedTsIndex,
    pub config: Arc<Config>,
    pub compaction_tx: CompactionTx,
    pub repl_state: Arc<ReplState>,
    pub hub_tx: HubTx,
    /// The server's own pub/sub hub, client registry, tracking table and
    /// monitor registry. Replicated writes are applied against these so that
    /// clients connected to the *replica* see their effects — keyspace
    /// notifications, `PUBLISH`es relayed from the master, client-side-cache
    /// invalidations, `MONITOR` lines. Applied against private stand-ins (as
    /// they used to be) all of that went nowhere.
    pub pubsub: crate::pubsub::PubSubHub,
    pub client_registry: crate::client_registry::ClientRegistry,
    pub tracking_table: crate::tracking::TrackingTable,
    pub monitor_registry: crate::monitor::SharedMonitorRegistry,
}

impl ReplicaStarter {
    pub fn from_ctx(ctx: &crate::commands::ctx::Ctx<'_>) -> Self {
        Self {
            pubsub: ctx.pubsub.clone(),
            client_registry: Arc::clone(ctx.client_registry),
            tracking_table: Arc::clone(ctx.tracking_table),
            monitor_registry: Arc::clone(ctx.monitor_registry),
            store: Arc::clone(ctx.store),
            index_store: Arc::clone(ctx.index_store),
            dict_store: Arc::clone(ctx.dict_store),
            wait_table: Arc::clone(ctx.wait_table),
            ts_index: Arc::clone(ctx.ts_index),
            config: Arc::clone(ctx.config),
            compaction_tx: ctx.compaction_tx.clone(),
            repl_state: Arc::clone(ctx.repl_state),
            hub_tx: ctx.hub_tx.clone(),
        }
    }

    /// Become a replica of `host:port`, starting now.
    ///
    /// The role change is synchronous: by the time this returns the node is a
    /// replica — read-only, reporting `role:slave` and the new master's
    /// address — even if that master is unreachable. It used to stay a
    /// writable `master` until the first sync succeeded, so a node told to
    /// follow a master that was down kept accepting writes it would later
    /// throw away.
    ///
    /// The client itself runs on its own thread and runtime, so it competes
    /// with neither the tokio workers nor the io_uring ones, and can be
    /// started from places that have no runtime at all.
    pub fn start(&self, host: String, port: u16) {
        let generation = self.repl_state.begin_replication_epoch();
        self.repl_state.set_master_addr(&host, port);
        self.repl_state.become_slave();
        self.repl_state.set_link_state(LinkState::Connect);

        let starter = self.clone();
        let spawned = std::thread::Builder::new()
            .name("kividb-replica".into())
            .spawn(move || {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt.block_on(start_replica(starter, host, port, generation)),
                    Err(e) => log_error!("Replication: could not start replication runtime: {e}"),
                }
            });
        if let Err(e) = spawned {
            log_error!("Replication: could not start replication thread: {e}");
        }
    }
}

/// Why a replication session ended without an I/O error.
enum SessionEnd {
    /// The master closed the connection.
    MasterClosed,
    /// A newer `REPLICAOF` or a promotion took over; this task must stop.
    Superseded,
}

#[derive(Debug)]
enum SessionError {
    /// Something went wrong talking to the master; retry soon.
    Link(String),
    /// The dataset guard refused the full resync. The local dataset is intact.
    Refused(String),
    /// See [`SessionEnd::Superseded`].
    Superseded,
}

impl From<std::io::Error> for SessionError {
    fn from(e: std::io::Error) -> Self {
        SessionError::Link(e.to_string())
    }
}

impl From<String> for SessionError {
    fn from(e: String) -> Self {
        SessionError::Link(e)
    }
}

/// How often a waiting replication task re-checks whether it has been
/// superseded. Bounds how long a task blocked on an unreachable master can
/// outlive the `REPLICAOF` that replaced it.
const EPOCH_CHECK_INTERVAL: Duration = Duration::from_millis(200);

/// Delay between connection attempts. One second, as in Redis: a master that
/// comes back should be picked up promptly.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// Delay after the dataset guard refuses a sync. Retrying sooner would only
/// re-download a payload that will be refused again; the operator has to act.
const REFUSED_RETRY_DELAY: Duration = Duration::from_secs(10);

/// Run the replication client until this node is promoted or re-pointed.
///
/// `my_generation` is the replication epoch [`ReplicaStarter::start`] claimed
/// for this task. Any later `REPLICAOF` or promotion bumps the epoch, and this
/// task stops at its next check — that is what makes re-pointing a replica
/// work while its role stays `"slave"` throughout.
pub async fn start_replica(
    starter: ReplicaStarter,
    master_host: String,
    master_port: u16,
    my_generation: u64,
) {
    let repl_state = Arc::clone(&starter.repl_state);
    let mut failures: u64 = 0;

    loop {
        if repl_state.generation() != my_generation {
            log_info!(
                "Replication: Superseded by a newer REPLICAOF (epoch {my_generation} → {}) — \
                 stopping replication client for {master_host}:{master_port}",
                repl_state.generation()
            );
            return;
        }
        if !repl_state.is_slave() {
            log_info!("Replication: No longer a replica — stopping replication client");
            return;
        }

        // A master that stays down is retried every second; say so once, then
        // once a minute, rather than once per attempt.
        let verbose = failures == 0 || failures.is_multiple_of(60);
        if verbose {
            log_info!("Replication: Connecting to master {master_host}:{master_port}...");
        }

        let delay = match run_replication_session(
            &starter,
            &master_host,
            master_port,
            my_generation,
            verbose,
        )
        .await
        {
            Ok(SessionEnd::Superseded) | Err(SessionError::Superseded) => {
                log_info!(
                    "Replication: Replication session for {master_host}:{master_port} superseded — \
                     stopping replication client"
                );
                return;
            }
            Ok(SessionEnd::MasterClosed) => {
                log_warn!("Replication: Master closed the connection — reconnecting");
                failures = 0;
                RECONNECT_DELAY
            }
            Err(SessionError::Link(e)) => {
                if verbose {
                    log_warn!(
                        "Replication: Replication link to {master_host}:{master_port} failed: {e} — \
                         retrying every {}s",
                        RECONNECT_DELAY.as_secs()
                    );
                }
                failures += 1;
                RECONNECT_DELAY
            }
            Err(SessionError::Refused(reason)) => {
                // Loud on purpose: this node is deliberately not in sync, and
                // nothing else about it will say why.
                let first = repl_state
                    .full_syncs_refused
                    .fetch_add(1, Ordering::Relaxed)
                    == 0;
                *repl_state.last_sync_refusal.write() = Some(reason.clone());
                if first || verbose {
                    log_error!(
                        "Replication: FULL RESYNC REFUSED from {master_host}:{master_port}: \
                         {reason}. The local dataset has been kept and this replica is NOT in \
                         sync. If the master's dataset really is the one to keep, run \
                         `CONFIG SET replica-full-sync-guard off` on this replica (it retries \
                         every {}s); otherwise promote this node with `REPLICAOF NO ONE`.",
                        REFUSED_RETRY_DELAY.as_secs()
                    );
                }
                failures += 1;
                REFUSED_RETRY_DELAY
            }
        };

        // Sleep in short steps so a promotion or re-point stops this task
        // promptly instead of after the full delay — and so an operator who
        // has just relaxed the guard does not wait out a refusal backoff.
        let guard_setting = |rs: &ReplState| {
            (
                rs.full_sync_guard(),
                rs.full_sync_guard_ratio.load(Ordering::Relaxed),
            )
        };
        let guard_at_sleep = guard_setting(&repl_state);
        let wake = Instant::now() + delay;
        while Instant::now() < wake {
            if delay == REFUSED_RETRY_DELAY && guard_setting(&repl_state) != guard_at_sleep {
                break;
            }
            tokio::time::sleep(EPOCH_CHECK_INTERVAL.min(delay)).await;
            if repl_state.generation() != my_generation {
                log_info!("Replication: Superseded during reconnect backoff — stopping");
                return;
            }
            if !repl_state.is_slave() {
                log_info!("Replication: Role changed during reconnect backoff — stopping");
                return;
            }
        }
    }
}

/// Await `fut`, giving up if it takes longer than `timeout` or if this task is
/// superseded in the meantime.
///
/// This is what keeps a replica from waiting forever on a master that has
/// vanished without closing the connection — powered off, partitioned, or
/// behind a firewall that now drops its packets. Such a peer produces no error
/// at all; the only signal is the absence of a reply.
async fn guarded<T, E: std::fmt::Display>(
    fut: impl std::future::Future<Output = Result<T, E>>,
    timeout: Duration,
    repl_state: &ReplState,
    my_generation: u64,
    what: &str,
) -> Result<T, SessionError> {
    // Checked before each wait as well as during it: on a link that keeps
    // delivering, the timer below never fires, and a session that had been
    // superseded would read a whole payload before noticing.
    if repl_state.generation() != my_generation {
        return Err(SessionError::Superseded);
    }
    tokio::pin!(fut);
    let deadline = Instant::now() + timeout;
    loop {
        tokio::select! {
            res = &mut fut => {
                return res.map_err(|e| SessionError::Link(format!("{what}: {e}")));
            }
            _ = tokio::time::sleep(EPOCH_CHECK_INTERVAL) => {
                if repl_state.generation() != my_generation {
                    return Err(SessionError::Superseded);
                }
                if Instant::now() >= deadline {
                    return Err(SessionError::Link(format!(
                        "{what}: no response from master within {}s",
                        timeout.as_secs()
                    )));
                }
            }
        }
    }
}

/// Ask the kernel to notice a dead peer too, as a second line of defence.
///
/// The application-level timeout needs the master to send heartbeats, which a
/// master running an older build does not. These options need no cooperation:
/// keepalive probes an otherwise idle connection, and on Linux
/// `TCP_USER_TIMEOUT` caps how long written-but-unacknowledged data (our
/// `REPLCONF ACK`s) may sit before the socket is failed — without it that cap
/// is the kernel's retransmission limit, about 15 minutes.
fn tune_replication_socket(stream: &TcpStream, timeout: Duration) {
    let sock = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(15))
        .with_interval(Duration::from_secs(5));
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let keepalive = keepalive.with_retries(3);
    let _ = sock.set_tcp_keepalive(&keepalive);
    #[cfg(target_os = "linux")]
    let _ = sock.set_tcp_user_timeout(Some(timeout));
    #[cfg(not(target_os = "linux"))]
    let _ = timeout;
}

async fn run_replication_session(
    starter: &ReplicaStarter,
    host: &str,
    port: u16,
    my_generation: u64,
    verbose: bool,
) -> Result<SessionEnd, SessionError> {
    let ReplicaStarter {
        store,
        index_store,
        dict_store,
        wait_table,
        ts_index,
        config,
        compaction_tx,
        repl_state,
        ..
    } = starter;

    let timeout = repl_state.repl_timeout();
    let upstream = SlaveUpstreamGuard::connect(repl_state, my_generation);

    let addr = format!("{host}:{port}");
    let stream = guarded(
        TcpStream::connect(&addr),
        timeout,
        repl_state,
        my_generation,
        "connect",
    )
    .await?;
    stream.set_nodelay(true)?;
    tune_replication_socket(&stream, timeout);

    // Over TLS with `tls-replication` (or `tls-cluster`): see `node_tls`.
    type Reader = Box<dyn tokio::io::AsyncRead + Unpin + Send>;
    type Writer = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;
    let (reader, mut writer): (Reader, Writer) = if crate::node_tls::replication() {
        let tls = guarded(
            crate::node_tls::connect_async(host, stream),
            timeout,
            repl_state,
            my_generation,
            "TLS handshake",
        )
        .await?;
        let (r, w) = tokio::io::split(tls);
        (Box::new(r), Box::new(w))
    } else {
        let (r, w) = stream.into_split();
        (Box::new(r), Box::new(w))
    };
    let mut reader = BufReader::new(reader);

    // ── Phase 0: AUTH, when the master requires it ───────────────────────
    let (master_user, master_auth) = repl_state.master_credentials();
    if !master_auth.is_empty() {
        // As the default user when no user is given: `AUTH <password>`
        // is refused by a master that has no password (#135).
        let user = if master_user.is_empty() {
            "default"
        } else {
            master_user.as_str()
        };
        let parts = vec!["AUTH", user, master_auth.as_str()];
        send_command(&mut writer, &parts).await?;
        let reply = guarded(
            read_line(&mut reader),
            timeout,
            repl_state,
            my_generation,
            "AUTH",
        )
        .await?;
        if !reply.starts_with("+OK") {
            return Err(SessionError::Link(format!(
                "master rejected AUTH: {}",
                reply.trim_start_matches('-')
            )));
        }
    }

    // ── Phase 1: PING ────────────────────────────────────────────────────
    writer.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let pong = guarded(
        read_line(&mut reader),
        timeout,
        repl_state,
        my_generation,
        "PING",
    )
    .await?;
    if pong.starts_with("-NOAUTH") {
        return Err(SessionError::Link(
            "master requires authentication — set `masterauth` (and `masteruser` for an ACL \
             user) on this replica"
                .into(),
        ));
    }
    if !pong.starts_with("+PONG") && !pong.starts_with("$4") {
        return Err(SessionError::Link(format!("expected PONG, got: {pong}")));
    }
    // If the master sent a bulk PONG ($4\r\nPONG), consume the second line
    if pong.starts_with("$4") {
        let _ = guarded(
            read_line(&mut reader),
            timeout,
            repl_state,
            my_generation,
            "PING",
        )
        .await?;
    }

    // ── Phase 2: REPLCONF listening-port ────────────────────────────────
    // The port clients reach *this* node on, so the master's `INFO` and `ROLE`
    // list the replica by an address someone can actually connect to rather
    // than this connection's ephemeral source port. As in Redis, that is the
    // TLS port when the replica replicates over TLS, and it is when there is
    // no plaintext port (port 0); otherwise the plaintext port. Both as they
    // are now: `CONFIG SET` moves them.
    let listening_port = if config.replica_announced_port != 0 {
        config.replica_announced_port
    } else {
        use crate::listen::{Kind, current_port, server_of};
        let plain = current_port(server_of(config), Kind::Plain);
        let tls = current_port(server_of(config), Kind::Tls);
        if tls != 0 && (plain == 0 || crate::node_tls::replication()) {
            tls
        } else {
            plain
        }
    };
    send_command(
        &mut writer,
        &["REPLCONF", "listening-port", &listening_port.to_string()],
    )
    .await?;
    expect_ok(
        &mut reader,
        "REPLCONF listening-port",
        timeout,
        repl_state,
        my_generation,
    )
    .await?;

    // ── Phase 3: REPLCONF capa psync2 ───────────────────────────────────
    send_command(&mut writer, &["REPLCONF", "capa", "psync2"]).await?;
    expect_ok(
        &mut reader,
        "REPLCONF capa",
        timeout,
        repl_state,
        my_generation,
    )
    .await?;

    // ── Phase 3b: which release the master is ───────────────────────────
    // A `FT.DROPINDEX … DD` in the stream is run as the master that sent it
    // ran it: a 1.2.0 release candidate deleted the documents and left each
    // replica to delete its own; 1.1 and before kept them (see
    // `replication::dd_deleted_by`). Releases since send the deletes and
    // never `DD`. A master that does not say is taken to have kept them.
    send_command(&mut writer, &["INFO", "server"]).await?;
    let head = guarded(
        read_line(&mut reader),
        timeout,
        repl_state,
        my_generation,
        "INFO server",
    )
    .await?;
    let mut master_version = None;
    if let Some(len) = head.strip_prefix('$').and_then(|n| n.parse::<usize>().ok()) {
        let mut body = vec![0u8; len + 2];
        guarded(
            reader.read_exact(&mut body),
            timeout,
            repl_state,
            my_generation,
            "INFO server",
        )
        .await?;
        master_version = String::from_utf8_lossy(&body)
            .lines()
            .find_map(|line| line.strip_prefix("kividb_version:"))
            .map(|version| version.trim().to_string());
    }
    repl_state.master_dd_deletes.store(
        master_version
            .as_deref()
            .is_some_and(crate::replication::dd_deleted_by),
        std::sync::atomic::Ordering::Relaxed,
    );

    // ── Phase 4: PSYNC ───────────────────────────────────────────────────
    // If this node's dataset is a known position in a master's stream — it
    // was following one and lost the connection, or was restarted cleanly —
    // ask for the stream from there. The master continues if it is that
    // master and still has what follows in its backlog; otherwise it answers
    // with a full resync, exactly as it does to `PSYNC ? -1`.
    //
    // A master that cannot continue may offer the fingerprint of its dataset
    // instead (see `replication::digest`): if this node's dataset has the
    // same one, it holds exactly what that master started with, and asks to
    // be continued from there. Masters that do not know the capability
    // answer it with an error, which is fine.
    send_command(&mut writer, &["REPLCONF", "capa", "dataset-digest"]).await?;
    let _ = guarded(
        read_line(&mut reader),
        timeout,
        repl_state,
        my_generation,
        "REPLCONF capa dataset-digest",
    )
    .await?;
    let mut resume = repl_state.resync_point.lock().clone();
    let (ask_id, ask_offset) = match &resume {
        Some(point) => (point.repl_id.clone(), (point.offset + 1).to_string()),
        None => ("?".to_string(), "-1".to_string()),
    };
    send_command(&mut writer, &["PSYNC", &ask_id, &ask_offset]).await?;
    // The master takes its cut before replying, which on a large dataset is
    // not instant — but it is still bounded by the same timeout.
    let mut fullresync = guarded(
        read_line(&mut reader),
        timeout,
        repl_state,
        my_generation,
        "PSYNC",
    )
    .await?;
    if let Some(offer) = fullresync.trim().strip_prefix("+DIGEST ") {
        let mut parts = offer.split_whitespace();
        let offered = parts.next().map(str::to_string).zip(
            parts
                .next()
                .and_then(|hex| u64::from_str_radix(hex, 16).ok()),
        );
        let ours = {
            let store = store.clone();
            tokio::task::spawn_blocking(move || crate::replication::digest::dataset_digest(&store))
                .await
                .ok()
        };
        let (ask_id, ask_offset) = match offered {
            Some((id, digest)) if ours == Some(digest) && id.len() == 40 => {
                log_info!(
                    "Replication: This dataset is the master's at the start of its stream {id} \
                     — asking to be continued from there"
                );
                resume = Some(crate::replication::ResyncPoint {
                    repl_id: id.clone(),
                    offset: 0,
                    db: 0,
                });
                (id, "1".to_string())
            }
            _ => {
                resume = None;
                ("?".to_string(), "-1".to_string())
            }
        };
        send_command(&mut writer, &["PSYNC", &ask_id, &ask_offset]).await?;
        fullresync = guarded(
            read_line(&mut reader),
            timeout,
            repl_state,
            my_generation,
            "PSYNC",
        )
        .await?;
    }
    let tok: Vec<&str> = fullresync.split_whitespace().collect();
    let cmd0 = tok
        .first()
        .map(|s| s.trim_start_matches('+').to_ascii_uppercase())
        .unwrap_or_default();

    // The stream as it will be read below: what has arrived and is not yet
    // applied, how many bytes have arrived in all, and which database the
    // next command is for.
    let mut repl_buf: Vec<u8> = Vec::with_capacity(4096);
    let mut temp = [0u8; 16 * 1024];
    let mut selected_db = 0usize;
    let mut offset: u64;

    if cmd0 == "CONTINUE" {
        let Some(point) = resume else {
            return Err(SessionError::Link(
                "master answered +CONTINUE to a request for a full resync".into(),
            ));
        };
        selected_db = point.db;
        let prelude_len = {
            let buffered = reader.buffer();
            let len = buffered.len();
            repl_buf.extend_from_slice(buffered);
            reader.consume(len);
            len
        };
        offset = point.offset + prelude_len as u64;
        // A master that was this one's sibling, promoted since, continues the
        // old stream under its own id: follow that id from here on (#109).
        if let Some(id) = tok
            .get(1)
            .filter(|id| id.len() == 40 && **id != point.repl_id)
            && let Some(current) = repl_state.resync_point.lock().as_mut()
        {
            log_info!(
                "Replication: The master continues {} as {id}",
                current.repl_id
            );
            current.repl_id = id.to_string();
        }
        log_info!(
            "Replication: Partial resync: continuing from offset {} — no dataset transfer",
            point.offset
        );
    } else {
        if tok.len() < 3 || cmd0 != "FULLRESYNC" {
            return Err(SessionError::Link(format!(
                "expected +FULLRESYNC or +CONTINUE, got: {fullresync}"
            )));
        }
        let master_repl_id = tok[1].to_string();
        let master_offset: u64 = tok[2].parse().map_err(|_| {
            format!(
                "expected numeric replication offset after FULLRESYNC, got `{}`",
                tok[2]
            )
        })?;
        if verbose {
            log_info!("Replication: Full resync from {master_repl_id} offset={master_offset}");
        }
        upstream.mark_syncing();
        // The dataset is about to be replaced: it is no position in any stream
        // until the new one is in.
        repl_state.set_resync_point(None);

        // ── Phase 5: Receive the payload ─────────────────────────────────────
        // Format: $<len>\r\n<len bytes>
        // Note: no trailing \r\n after the payload (unlike normal bulk strings)
        let rdb_header = guarded(
            read_line(&mut reader),
            timeout,
            repl_state,
            my_generation,
            "payload header",
        )
        .await?;
        if !rdb_header.starts_with('$') {
            return Err(SessionError::Link(format!(
                "expected payload bulk header, got: {rdb_header}"
            )));
        }
        let rdb_len: usize = rdb_header[1..]
            .trim()
            .parse()
            .map_err(|_| format!("bad payload length: {rdb_header}"))?;
        if verbose {
            log_info!("Replication: Receiving full-sync payload ({rdb_len} bytes)...");
        }

        // Read in pieces so the timeout measures *silence*, not total transfer
        // time: a large payload on a slow link is fine, a stalled one is not.
        let mut rdb_bytes = vec![0u8; rdb_len];
        let mut filled = 0usize;
        while filled < rdb_len {
            let n = guarded(
                reader.read(&mut rdb_bytes[filled..]),
                timeout,
                repl_state,
                my_generation,
                "payload",
            )
            .await?;
            if n == 0 {
                return Err(SessionError::Link(format!(
                    "master closed the connection {filled} bytes into a {rdb_len}-byte payload"
                )));
            }
            filled += n;
            repl_state.note_master_io();
        }

        // ── Phase 6: Load into staging, check, swap ──────────────────────────
        // Nothing below touches the live dataset until the payload has parsed
        // completely and the guard has passed. A payload that fails to parse used
        // to be logged and then treated as an empty dataset, which was swapped in
        // over the live one.
        //
        // `repl-diskless-load` decides what happens to the old dataset meanwhile.
        // Kept until the new one is loaded (`swapdb`), a full resync needs memory
        // for two datasets — a replica sized for one is killed for it, on every
        // restart. Dropped first (the default, as in Redis), it needs memory for
        // one.
        //
        // Dropping first is done only for a payload that has been *read through
        // to the end* (`verify_snapshot`): every key and value parsed, nothing
        // kept. A transfer the master did not finish never gets this far — the
        // read above fails — and a payload that cannot be read is found out
        // here, with the old dataset still in place. Either way the replica
        // carries on serving what it had. (v1.0.4 did not: a full sync that
        // failed part-way left the replica with no keys at all.)
        // The last look at the epoch before the live dataset is touched, and
        // the lock that keeps the epoch from changing until the master's
        // dataset is in place (see `ReplState::dataset_commit`). A session
        // superseded by a promotion or a re-point stops here, with the local
        // dataset as it was.
        let commit = repl_state.dataset_commit.lock();
        if repl_state.generation() != my_generation || !repl_state.is_slave() {
            return Err(SessionError::Superseded);
        }
        let local_keys = count_keys(store);
        let is_kdb = rdb_bytes.len() >= 8 && rdb_bytes[..8] == *crate::snapshot::KDB_MAGIC;
        let verified_keys =
            if local_keys > 0 && is_kdb && repl_state.flush_before_load.load(Ordering::Relaxed) {
                Some(
                    crate::snapshot::verify_snapshot(rdb_bytes.as_slice()).map_err(|e| {
                        SessionError::Link(format!(
                            "full-sync payload is not a valid KDB image ({e}); the local dataset \
                         is untouched"
                        ))
                    })?,
                )
            } else {
                None
            };
        // Cleared when this guard drops, so an error anywhere below cannot leave
        // the replica answering `LOADING` forever.
        let syncing;
        let staged = match verified_keys {
            Some(incoming_keys) => {
                guard_full_sync(repl_state, local_keys, incoming_keys)?;
                syncing = SyncInProgress::begin(repl_state);
                {
                    let _cut = crate::write_gate::cut();
                    let empty = store::new_store(config.databases);
                    store::swap_kv_with_staging(store, &empty);
                    index_store.write().clear();
                    dict_store.write().clear();
                    crate::store::rebuild_ts_label_index(store, ts_index);
                    wait_table.clear();
                    // `empty` now holds the old dataset, and takes it with
                    // it.
                }
                crate::commands::memory::purge_allocator();
                log_warn!(
                    "Replication: Dropped the {local_keys} local keys before loading {incoming_keys} \
                     from the master"
                );
                load_full_sync_payload(&rdb_bytes, config.databases)?
            }
            None => {
                let staged = load_full_sync_payload(&rdb_bytes, config.databases)?;
                guard_full_sync(repl_state, local_keys, staged.keys)?;
                syncing = SyncInProgress::begin(repl_state);
                staged
            }
        };
        drop(rdb_bytes);

        // The master's libraries, in place of this node's, before the dataset
        // is: a failure here leaves the dataset as it was, and the master's
        // command stream, which may call them, comes after.
        crate::snapshot::restore_functions(staged.functions)
            .map_err(|e| SessionError::Link(format!("full sync: {e}")))?;
        {
            // Stop the world for the swap, the same way a checkpoint does, so a
            // local AOF record cannot straddle it.
            let _cut = crate::write_gate::cut();
            store::swap_kv_with_staging(store, &staged.store);
            index_store.write().clear();
            dict_store.write().clear();
            crate::store::rebuild_ts_label_index(store, ts_index);
            wait_table.clear();
        }
        // The hashes with field TTLs are the ones just received.
        crate::field_expiry::rescan(store);
        *repl_state.last_sync_refusal.write() = None;
        log_info!(
            "Replication: Full sync loaded: {} keys ({} with a TTL), replacing {local_keys} local keys",
            staged.keys,
            staged.keys_with_ttl
        );
        // The swap dropped the entire previous dataset. Those are small allocations
        // that glibc parks in its arenas, so without this the replica keeps the old
        // dataset's footprint resident on top of the new one.
        drop(staged.store);
        crate::commands::memory::purge_allocator();

        // ── Phase 7: Command stream ──────────────────────────────────────────
        // After the payload the master sends a continuous stream of RESP-encoded
        // write commands. Anything already buffered behind the payload is the
        // start of that stream and counts toward the offset.
        let prelude_len = {
            let buffered = reader.buffer();
            let len = buffered.len();
            repl_buf.extend_from_slice(buffered);
            reader.consume(len);
            len
        };
        offset = master_offset.saturating_add(prelude_len as u64);
        // The dataset now in place is the master's at `master_offset`; nothing
        // of the stream has been applied to it yet.
        repl_state.set_resync_point(Some(crate::replication::ResyncPoint {
            repl_id: master_repl_id.clone(),
            offset: master_offset,
            db: 0,
        }));
        // The dataset is the master's from here on; a promotion now finds it
        // whole and stops this session at its next look at the epoch.
        drop(commit);

        // Rebuild what the payload describes but does not contain: search indexes
        // are sent as definitions and rebuilt here from the values. That can take
        // a while, so the socket is drained into `repl_buf` meanwhile — otherwise
        // the master's queue for this replica grows until it is evicted for
        // falling behind, and the resync starts over.
        crate::vector::restore_dictionaries(index_store, staged.dictionaries);
        if !staged.index_schemas.is_empty() {
            let schemas = staged.index_schemas;
            let store_bg = Arc::clone(store);
            let index_bg = Arc::clone(index_store);
            log_info!(
                "Replication: Rebuilding {} search index(es) from the synced dataset",
                schemas.len()
            );
            let rebuild = tokio::task::spawn_blocking(move || {
                crate::vector::register_index_schemas(
                    &index_bg,
                    &schemas,
                    &std::collections::HashMap::new(),
                );
                for schema in &schemas {
                    let def = match index_bg.read().get(&schema.name) {
                        Some(d) => d.clone(),
                        None => continue,
                    };
                    let db_i = def.db_index.min(store_bg.len().saturating_sub(1));
                    crate::vector::backfill_from_db(&store_bg[db_i], &def);
                }
            });
            tokio::pin!(rebuild);
            loop {
                tokio::select! {
                    res = &mut rebuild => {
                        if let Err(e) = res {
                            log_error!("Replication: index rebuild after full sync failed: {e}");
                        }
                        break;
                    }
                    res = reader.get_mut().read(&mut temp) => {
                        let n = res?;
                        if n == 0 {
                            return Ok(SessionEnd::MasterClosed);
                        }
                        repl_buf.extend_from_slice(&temp[..n]);
                        offset += n as u64;
                        repl_state.note_master_io();
                    }
                }
            }
        }

        drop(syncing);

        // Rebase this node's AOF onto the dataset we just received.
        //
        // A full resync replaces the entire store. The AOF does not follow: left
        // alone it would hold whatever this node logged *before* the resync, and
        // the incoming stream would then append on top of it — a log that replays
        // to a dataset this node never had. On a first-time replica the failure is
        // the mirror image: the AOF holds only the stream, and none of the keys the
        // payload delivered, so replaying it loses everything that existed at sync
        // time.
        //
        // Checkpoint writes dump.kdb and resets the AOF to an empty delta, which is
        // what dump-then-AOF recovery expects. The stream then appends onto that
        // delta.
        //
        // Done before entering the stream, not concurrently with it: the swap
        // discards whatever is queued for the writer (correct only while writes are
        // quiesced), so the stream must not be appending yet.
        if crate::aof::AOF_ENABLED.load(Ordering::Relaxed) {
            let store_for_rewrite = Arc::clone(store);
            let index_for_rewrite = Arc::clone(index_store);
            let repl_state_for_rewrite = Arc::clone(repl_state);
            log_info!("Replication: AOF is on — rebasing it onto the synced dataset");
            let rebased = tokio::task::spawn_blocking(move || {
                crate::replication::save_dataset(
                    &store_for_rewrite,
                    &index_for_rewrite,
                    &repl_state_for_rewrite,
                )
            })
            .await;
            if !matches!(rebased, Ok(Ok(()))) {
                // The stream is about to append to an AOF that does not describe
                // the synced dataset. Say so loudly rather than log onto a base
                // that was never rebased.
                log_error!(
                    "Replication: post-sync AOF rebase failed ({rebased:?}); this replica's AOF \
                 does not match its dataset — run BGREWRITEAOF before relying on it"
                );
            }
        }
    }

    // One amortized scaffold for millions of APPLYs — rebuilding this per instruction was tens of μs each.
    let repl_apply = ReplicationApplyEnv::new(starter);
    // `selected_db` persists across commands — SELECT in the stream mutates it.

    // How far the stream has been *applied*: what has arrived, less the
    // incomplete command still sitting in `repl_buf`. This is the offset
    // reported, acknowledged, and continued from.
    let mut applied = offset - repl_buf.len() as u64;
    if !publish_applied(repl_state, my_generation, applied) {
        return Ok(SessionEnd::Superseded);
    }

    let ack_interval = Duration::from_secs(1);
    send_ack(&mut writer, applied, timeout).await?;
    let mut last_ack = Instant::now();

    // The link is up from here: synced, and about to stream.
    upstream.mark_synced();
    repl_state.note_master_io();
    log_info!("Replication: Synced with {addr}. Entering command stream.");

    // Silence detection. A healthy master is never quiet for long — it writes
    // a PING into the stream every `repl-ping-replica-period` — so a link with
    // no bytes for `repl-timeout` is a dead one. The deadline is only armed
    // once a heartbeat has actually been seen, because a master running an
    // older build sends none: timing *that* out would tear down a healthy idle
    // link every minute and force a full resync each time. Against such a
    // master the kernel-level options set in `tune_replication_socket` are
    // what notice a dead peer.
    let mut last_io = Instant::now();
    let mut heartbeats_seen = false;
    // Bytes already in `repl_buf` (the prelude, or what arrived during the
    // index rebuild) must be applied before blocking on the socket.
    let mut pending_input = !repl_buf.is_empty();
    // How far into `repl_buf` a transaction still arriving has been looked
    // through for its `EXEC` (`transaction_at`).
    let mut transaction_scanned = 0usize;

    loop {
        // Nothing is written to `repl_offset` on the way out, here or below:
        // every batch has already published its position, and a session that
        // has been superseded has no position to publish. A promoted node's
        // offset is its own stream's by then, and a re-pointed one's is the
        // new session's. Writing the old position over it took back the
        // writes a promoted master had taken meanwhile; the next full sync
        // restarted the backlog at the shorter offset, and replicas continued
        // before it were later sent the stream from the middle of a command.
        if repl_state.generation() != my_generation {
            log_info!(
                "Replication: Command stream from {addr} superseded (epoch {my_generation}) — \
                 dropping connection"
            );
            return Ok(SessionEnd::Superseded);
        }

        if !pending_input {
            let inner = reader.get_mut();
            // `AsyncReadExt::read` is cancel-safe for a TcpStream — on the timeout
            // branch nothing has been consumed from the socket, so re-issuing the
            // read on the next iteration loses no bytes. `None` = the epoch-check
            // tick fired with no data available; fall through to the ACK heartbeat
            // below rather than `continue`, or an idle replica would stop ACKing.
            let read_n = tokio::select! {
                res = inner.read(&mut temp) => Some(res?),
                _ = tokio::time::sleep(EPOCH_CHECK_INTERVAL) => None,
            };
            match read_n {
                Some(0) => {
                    return Ok(SessionEnd::MasterClosed);
                }
                Some(n) => {
                    repl_buf.extend_from_slice(&temp[..n]);
                    offset += n as u64;
                    last_io = Instant::now();
                    repl_state.note_master_io();
                    pending_input = true;
                }
                None => {
                    let timeout = repl_state.repl_timeout();
                    if heartbeats_seen && last_io.elapsed() > timeout {
                        return Err(SessionError::Link(format!(
                            "no data from master for {}s (repl-timeout) — master is \
                             unreachable",
                            timeout.as_secs()
                        )));
                    }
                }
            }
        }

        let mut applied_commands = false;
        if pending_input {
            pending_input = false;
            // One command, a write the master sent as it is.
            macro_rules! apply {
                ($cmd:expr) => {
                    repl_apply
                        .apply_command(
                            $cmd,
                            store,
                            repl_state,
                            index_store,
                            dict_store,
                            wait_table,
                            ts_index,
                            config,
                            compaction_tx,
                            &mut selected_db,
                            &crate::watch::ForStore(store),
                            addr.as_str(),
                        )
                        .map_err(|e| {
                            // Where the dataset stands is no longer known to
                            // be a point in the stream: resync in full.
                            repl_state.set_resync_point(None);
                            SessionError::Link(e.to_string())
                        })?
                };
            }
            // Commands one by one up to a `MULTI`; then the transaction it
            // opens, as one step, once its `EXEC` has arrived; and so on.
            // A transaction not yet arrived in full stays in `repl_buf`, not
            // applied and not counted: should the link drop now, it is asked
            // for again from its `MULTI` (as Redis keeps a replica's offset
            // at the `MULTI` until the `EXEC`).
            loop {
                let transaction = match transaction_at(&repl_buf, &mut transaction_scanned) {
                    StreamTransaction::Whole(whole) => Some(whole),
                    StreamTransaction::Partial => break,
                    StreamTransaction::None => None,
                };
                // Alone before the lock below, never after: a command can
                // hold the gate while it waits for that lock (`SAVE` does),
                // and would wait for ever behind a transaction waiting for
                // the gate.
                let _alone = transaction.is_some().then(crate::command_gate::enter_alone);
                // The commands and the position move together: whoever holds
                // this lock sees a dataset that is exactly the stream up to the
                // resync point. A save takes it to record that point beside the
                // dataset it writes.
                let _applying = repl_state.apply_lock.lock();
                // Looked at under the lock a promotion takes to read where the
                // stream stands (`become_master`): what was read from the old
                // master after that is not this node's to apply any more, nor
                // to count in an offset that is now its own stream's.
                if repl_state.generation() != my_generation {
                    log_info!(
                        "Replication: Command stream from {addr} superseded (epoch \
                         {my_generation}) — dropping connection"
                    );
                    return Ok(SessionEnd::Superseded);
                }
                let parse_pos = match transaction {
                    Some(whole) => {
                        // As `EXEC` runs on the master: alone, so that no
                        // reader here sees half of it; as one write, so that
                        // no snapshot or backup is cut inside it; and to this
                        // node's AOF as one group, between its own `MULTI`
                        // and `EXEC`.
                        let _whole = crate::write_gate::enter();
                        let _propagation = repl_apply.propagation_group(repl_state);
                        heartbeats_seen |= whole.heartbeat;
                        for cmd in whole.commands {
                            apply!(cmd);
                        }
                        whole.len
                    }
                    None => {
                        let mut parse_pos = 0usize;
                        while let Some((consumed, cmd)) = parse_resp_command(&repl_buf[parse_pos..])
                        {
                            if is_named(&cmd, b"MULTI") {
                                // Applied whole, on the next turn.
                                break;
                            }
                            if is_named(&cmd, b"PING") {
                                // The master's heartbeat. Nothing to apply.
                                heartbeats_seen = true;
                            } else {
                                apply!(cmd.iter().map(|v| v.to_vec()).collect());
                            }
                            parse_pos += consumed;
                        }
                        parse_pos
                    }
                };
                if parse_pos == 0 {
                    break;
                }
                applied_commands = true;
                let read: Vec<u8> = repl_buf[..parse_pos].to_vec();
                repl_buf.drain(0..parse_pos);
                transaction_scanned = 0;
                applied = offset - repl_buf.len() as u64;
                repl_state.repl_offset.store(applied, Ordering::Relaxed);
                if let Some(point) = repl_state.resync_point.lock().as_mut() {
                    point.offset = applied;
                    point.db = selected_db;
                }
                // Kept in this node's backlog, in the master's numbering: if it
                // is promoted, the master's other replicas carry on from it (#109).
                repl_state.record_received(read, applied);
            }
            // What is left is the start of a command still arriving — or, if
            // the master sent the stream from a position that is not where
            // this replica stands, the middle of one. That never parses: the
            // replica would wait for ever, applying nothing and acknowledging
            // the same offset, and look healthy from both sides. Resync in
            // full instead.
            if !crate::resp::parse::can_start_command(&repl_buf)
                || !crate::resp::parse::can_start_command(&repl_buf[transaction_scanned..])
            {
                repl_state.set_resync_point(None);
                return Err(SessionError::Link(format!(
                    "the master's stream does not read as commands from offset {applied} \
                     (it was sent from another position than this replica's) — resyncing in full"
                )));
            }
        }

        // REPLCONF ACK — heartbeat ~1 Hz when idle; **immediate** after applying commands so the
        // master can unblock WAIT without waiting for more replication traffic (otherwise we sit in
        // `read()` forever after the last propagating write and never hit the timer).
        if applied_commands || last_ack.elapsed() >= ack_interval {
            send_ack(&mut writer, applied, repl_state.repl_timeout()).await?;
            last_ack = Instant::now();
        }
    }
}

/// Publish `applied` as this node's replication offset, unless the session
/// that applied it has been superseded — by a promotion, whose stream the
/// offset now counts, or by a re-point, whose session owns it. Under the lock
/// a promotion takes to read where the stream stands, so that the two cannot
/// cross. False when superseded.
fn publish_applied(repl_state: &ReplState, my_generation: u64, applied: u64) -> bool {
    let _applying = repl_state.apply_lock.lock();
    if repl_state.generation() != my_generation {
        return false;
    }
    repl_state.repl_offset.store(applied, Ordering::Relaxed);
    true
}

/// Whether `cmd` is the bare command `name` — `MULTI`, `EXEC`, `PING`.
fn is_named(cmd: &[&[u8]], name: &[u8]) -> bool {
    cmd.len() == 1 && cmd[0].eq_ignore_ascii_case(name)
}

/// What the front of the stream holds.
enum StreamTransaction {
    /// Not a `MULTI`.
    None,
    /// A `MULTI` whose `EXEC` has not arrived yet.
    Partial,
    Whole(WholeTransaction),
}

/// A transaction from the master, `MULTI` to `EXEC`.
struct WholeTransaction {
    /// Bytes of the stream it takes, `MULTI` and `EXEC` included.
    len: usize,
    /// The commands between them.
    commands: Vec<Vec<Vec<u8>>>,
    /// A heartbeat came in the middle of it.
    heartbeat: bool,
}

/// The transaction at the front of `buf`, if one is.
///
/// A large one — a script that wrote a million keys — arrives over many
/// reads, and is looked at again after each. `scanned` keeps how far it has
/// been looked through already (0 for none), so that it is not parsed from
/// its start every time.
fn transaction_at(buf: &[u8], scanned: &mut usize) -> StreamTransaction {
    let multi_len = match parse_resp_command(buf) {
        Some((consumed, cmd)) if is_named(&cmd, b"MULTI") => consumed,
        _ => return StreamTransaction::None,
    };
    let mut pos = (*scanned).max(multi_len);
    let end = loop {
        match parse_resp_command(&buf[pos..]) {
            Some((consumed, cmd)) => {
                let at = pos;
                pos += consumed;
                if is_named(&cmd, b"EXEC") {
                    break at;
                }
            }
            None => {
                *scanned = pos;
                return StreamTransaction::Partial;
            }
        }
    };
    let mut whole = WholeTransaction {
        len: pos,
        commands: Vec::new(),
        heartbeat: false,
    };
    let mut at = multi_len;
    while at < end {
        let Some((consumed, cmd)) = parse_resp_command(&buf[at..]) else {
            break;
        };
        at += consumed;
        if is_named(&cmd, b"PING") {
            // The master's heartbeat, sent while the transaction was on
            // its way. Nothing to apply.
            whole.heartbeat = true;
        } else if !is_named(&cmd, b"MULTI") {
            // A `MULTI` inside one is not something a master sends; Redis
            // refuses it and carries on with the transaction, as here.
            whole
                .commands
                .push(cmd.iter().map(|v| v.to_vec()).collect());
        }
    }
    StreamTransaction::Whole(whole)
}

/// Holds `slave_sync_in_progress` for as long as it lives.
struct SyncInProgress<'a>(&'a ReplState);

impl<'a> SyncInProgress<'a> {
    fn begin(repl_state: &'a ReplState) -> Self {
        repl_state
            .slave_sync_in_progress
            .store(true, Ordering::SeqCst);
        Self(repl_state)
    }
}

impl Drop for SyncInProgress<'_> {
    fn drop(&mut self) {
        self.0.slave_sync_in_progress.store(false, Ordering::SeqCst);
    }
}

/// A full-sync payload, parsed into stores that are not yet live.
struct StagedSync {
    store: Store,
    index_schemas: Vec<crate::vector::IndexSchema>,
    dictionaries: Vec<(String, Vec<String>)>,
    /// The master's function libraries, from a KDB image or a Redis RDB file
    /// that has any.
    functions: Option<Vec<u8>>,
    keys: usize,
    keys_with_ttl: usize,
}

/// Parse a full-sync payload into fresh staging stores.
///
/// The master sends a native KDB image (`KIVIDB01`); a Redis master sends an
/// RDB. Either way a payload that does not parse is an error — the caller
/// must not go on to replace the live dataset with whatever was read so far.
fn load_full_sync_payload(payload: &[u8], databases: usize) -> Result<StagedSync, SessionError> {
    let staging_store = store::new_store(databases);

    let is_kividb = payload.len() >= 8 && payload[..8] == *crate::snapshot::KDB_MAGIC;
    let (dbs, expiries, index_schemas, dictionaries, functions) = if is_kividb {
        let loaded = crate::snapshot::load_snapshot_from(payload).map_err(|e| {
            SessionError::Link(format!("full-sync payload is not a valid KDB image: {e}"))
        })?;
        (
            loaded.dbs,
            loaded.expiries,
            loaded.index_schemas,
            loaded.dictionaries,
            loaded.functions,
        )
    } else {
        let (dbs, expiries, functions) =
            crate::snapshot::load_redis_rdb_from(payload).map_err(|e| {
                SessionError::Link(format!("full-sync payload is not a valid RDB: {e}"))
            })?;
        (dbs, expiries, Vec::new(), Vec::new(), functions)
    };

    let keys = crate::snapshot::restore_into_store(dbs, &staging_store);
    let keys_with_ttl = crate::snapshot::restore_expiries_into_store(expiries, &staging_store);
    Ok(StagedSync {
        store: staging_store,
        index_schemas,
        dictionaries,
        functions,
        keys,
        keys_with_ttl,
    })
}

fn count_keys(store: &Store) -> usize {
    store
        .iter()
        .flat_map(|db| db.iter())
        .map(|shard| shard.read().len())
        .sum()
}

/// Decide whether a full resync may replace the local dataset.
///
/// A full resync is destructive by design — the replica's dataset is replaced
/// with the master's — and that is exactly right when the master is the
/// authority. It is catastrophic when the "master" is a freshly built, empty
/// node that inherited an address this replica was still configured to follow:
/// the replica holds the only copy of the data and is about to overwrite it
/// with nothing. The engine is the one component that can see both sizes, so
/// it refuses, keeps what it has, and says so.
///
/// Returns `Err(Refused)` with the reason; `Ok` means proceed.
fn guard_full_sync(
    repl_state: &ReplState,
    local_keys: usize,
    incoming_keys: usize,
) -> Result<(), SessionError> {
    let ratio = repl_state.full_sync_guard_ratio.load(Ordering::Relaxed);
    match full_sync_refusal(
        repl_state.full_sync_guard(),
        ratio,
        local_keys,
        incoming_keys,
    ) {
        Some(reason) => Err(SessionError::Refused(reason)),
        None => Ok(()),
    }
}

/// The guard's decision, as a pure function of its inputs.
pub(crate) fn full_sync_refusal(
    guard: FullSyncGuard,
    ratio_percent: u64,
    local_keys: usize,
    incoming_keys: usize,
) -> Option<String> {
    if guard == FullSyncGuard::Off || local_keys == 0 {
        return None;
    }
    if incoming_keys == 0 {
        return Some(format!(
            "the master has no keys, and this replica holds {local_keys}"
        ));
    }
    if guard == FullSyncGuard::Shrink
        && (incoming_keys as u128) * 100 < (local_keys as u128) * (ratio_percent as u128)
    {
        return Some(format!(
            "the master holds {incoming_keys} keys, fewer than {ratio_percent}% of the \
             {local_keys} this replica holds"
        ));
    }
    None
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn read_line(
    reader: &mut BufReader<impl AsyncReadExt + Unpin>,
) -> Result<String, std::io::Error> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "master closed the connection",
        ));
    }
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

async fn send_command(
    writer: &mut (impl AsyncWriteExt + Unpin),
    parts: &[&str],
) -> Result<(), std::io::Error> {
    let mut buf = format!("*{}\r\n", parts.len());
    for p in parts {
        buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    writer.write_all(buf.as_bytes()).await
}

/// The most a replica may have sent that is not yet a whole `REPLCONF ACK`.
pub(crate) const ACK_PENDING_MAX: usize = 64 * 1024;

/// `REPLCONF ACK <offset>`, bounded: a peer that has stopped reading must not
/// be able to park this task in a write forever.
async fn send_ack(
    writer: &mut (impl AsyncWriteExt + Unpin),
    offset: u64,
    timeout: Duration,
) -> Result<(), SessionError> {
    let offset_s = offset.to_string();
    let ack = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
        offset_s.len(),
        offset_s
    );
    match tokio::time::timeout(timeout, writer.write_all(ack.as_bytes())).await {
        Ok(res) => Ok(res?),
        Err(_) => Err(SessionError::Link(format!(
            "could not write to master for {}s — master is unreachable",
            timeout.as_secs()
        ))),
    }
}

async fn expect_ok(
    reader: &mut BufReader<impl AsyncReadExt + Unpin>,
    context: &str,
    timeout: Duration,
    repl_state: &ReplState,
    my_generation: u64,
) -> Result<(), SessionError> {
    let line = guarded(
        read_line(reader),
        timeout,
        repl_state,
        my_generation,
        context,
    )
    .await?;
    if line.starts_with("+OK") || line.starts_with("+ok") {
        Ok(())
    } else {
        Err(SessionError::Link(format!(
            "{context}: expected +OK, got: {line}"
        )))
    }
}

/// Built once when entering the master's replication byte stream — not per APPLY.
///
/// Previously every replicated command reconstructed channels, ACL, client registry,
/// latency, etc.; at high master throughput that dominated catch‑up latency after bursts.
pub(crate) struct ReplicationApplyEnv {
    /// Where an applied write's AOF record goes.
    ///
    /// Two callers with opposite requirements share this struct:
    ///
    /// * The **replication stream** must append. A replica that applies its
    ///   master's writes without logging them keeps an empty AOF, so promoting
    ///   it produces a durable-looking node with nothing on disk.
    /// * **AOF replay** must not. It is re-executing the log; appending would
    ///   write every record straight back into the file it is reading.
    aof_tx: Box<dyn crate::commands::ctx::AofSender>,
    /// False for replay (see above). When true, each command still re-checks
    /// `AOF_ENABLED` so a runtime `CONFIG SET appendonly` takes effect without
    /// waiting for the replica to reconnect.
    aof_live: bool,
    /// Draining task consumes stray hub messages if a command mistakenly propagates locally.
    discard_hub_tx: crate::replication::HubTx,
    /// Held only by [`Self::new_inert`], which has no runtime to spawn a drainer
    /// on. Keeping the receiver alive means a stray send buffers instead of
    /// failing; nothing should ever arrive, because every command applied
    /// through this env runs with `apply_replication: true` and so never
    /// reaches the propagation path at all.
    _inert_hub_rx: Option<tokio::sync::mpsc::UnboundedReceiver<crate::replication::HubMsg>>,
    lastsave: Arc<parking_lot::RwLock<i64>>,
    started_at: std::time::Instant,
    pubsub: crate::pubsub::PubSubHub,
    acl_store: crate::acl::AclStore,
    acl_log: crate::acl::SharedAclLog,
    slowlog: crate::slowlog::SharedSlowLog,
    latency: crate::latency::SharedLatencyTracker,
    cluster: std::sync::Arc<crate::cluster::ClusterState>,
    monitor_registry: crate::monitor::SharedMonitorRegistry,
    tracking_table: crate::tracking::TrackingTable,
    client_registry: crate::client_registry::ClientRegistry,
}

impl ReplicationApplyEnv {
    /// A transaction from the master, as one group to this node's AOF (and,
    /// were anything to go to the hub, there too). See
    /// [`crate::replication::PropagationGroup`].
    fn propagation_group<'a>(
        &'a self,
        repl_state: &'a ReplState,
    ) -> crate::replication::PropagationGroup<'a> {
        crate::replication::PropagationGroup::begin(
            repl_state,
            &self.discard_hub_tx,
            self.aof_tx.as_ref(),
        )
    }

    /// For the replication stream: applied writes are appended to this node's
    /// own AOF, and their client-visible side effects go to the server's real
    /// hubs (see [`ReplicaStarter`]).
    fn new(starter: &ReplicaStarter) -> Self {
        let (discard_hub_tx, mut hub_rx) =
            tokio::sync::mpsc::unbounded_channel::<crate::replication::HubMsg>();
        tokio::spawn(async move { while hub_rx.recv().await.is_some() {} });
        let mut env = Self::with_hub(
            discard_hub_tx,
            None,
            Box::new(crate::commands::ctx::GlobalAofSender),
            true,
        );
        env.pubsub = starter.pubsub.clone();
        env.client_registry = Arc::clone(&starter.client_registry);
        env.tracking_table = Arc::clone(&starter.tracking_table);
        env.monitor_registry = Arc::clone(&starter.monitor_registry);
        env
    }

    /// Same environment, but usable with no tokio runtime available, and with
    /// AOF appends disabled.
    ///
    /// AOF replay runs during startup, before (and on the io_uring path,
    /// entirely outside) the runtime that `new` needs for its drain task, so it
    /// keeps the receiver instead of spawning a consumer for it.
    pub(crate) fn new_inert() -> Self {
        let (discard_hub_tx, hub_rx) =
            tokio::sync::mpsc::unbounded_channel::<crate::replication::HubMsg>();
        Self::with_hub(
            discard_hub_tx,
            Some(hub_rx),
            Box::new(crate::commands::ctx::NoopAofSender),
            false,
        )
    }

    fn with_hub(
        discard_hub_tx: crate::replication::HubTx,
        inert_hub_rx: Option<tokio::sync::mpsc::UnboundedReceiver<crate::replication::HubMsg>>,
        aof_tx: Box<dyn crate::commands::ctx::AofSender>,
        aof_live: bool,
    ) -> Self {
        Self {
            aof_tx,
            aof_live,
            discard_hub_tx,
            _inert_hub_rx: inert_hub_rx,
            lastsave: Arc::new(parking_lot::RwLock::new(0i64)),
            started_at: std::time::Instant::now(),
            pubsub: crate::pubsub::PubSubHub::new(),
            acl_store: crate::acl::new_acl_store(""),
            acl_log: crate::acl::new_acl_log(),
            slowlog: crate::slowlog::new_slowlog(u64::MAX, 0),
            latency: crate::latency::new_latency_tracker(),
            cluster: std::sync::Arc::new(crate::cluster::ClusterState::new_disabled()),
            monitor_registry: crate::monitor::new_monitor_registry(),
            tracking_table: crate::tracking::new_tracking_table(),
            client_registry: crate::client_registry::new_client_registry(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_command(
        &self,
        cmd: Vec<Vec<u8>>,
        store: &Store,
        repl_state: &Arc<ReplState>,
        index_store: &IndexStore,
        dict_store: &DictStore,
        wait_table: &Arc<WaitTable>,
        ts_index: &SharedTsIndex,
        config: &Arc<Config>,
        compaction_tx: &CompactionTx,
        selected_db: &mut usize,
        watch_registry: &dyn crate::watch::WatchNotifier,
        peer_addr: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::commands::ctx::Ctx;
        use crate::commands::execute_command;
        let cmd_refs: Vec<&[u8]> = cmd.iter().map(|v| v.as_slice()).collect();

        let out = execute_command(Ctx {
            cmd: cmd_refs,
            store,
            lastsave: &self.lastsave,
            selected_db,
            aof_tx: self.aof_tx.as_ref(),
            // Re-read per command so `CONFIG SET appendonly yes` on a running
            // replica starts logging immediately.
            aof_enabled: self.aof_live
                && crate::aof::AOF_ENABLED.load(std::sync::atomic::Ordering::Relaxed),
            config,
            started_at: &self.started_at,
            index_store,
            dict_store,
            wait_table,
            ts_index,
            pubsub: &self.pubsub,
            hub_tx: &self.discard_hub_tx,
            repl_state,
            watch_registry,
            client_id: 0,
            client_registry: &self.client_registry,
            // Applying a replicated write locally, not replying to a real client.
            resp: 2,
            acl_store: &self.acl_store,
            acl_log: &self.acl_log,
            slowlog: &self.slowlog,
            latency: &self.latency,
            compaction_tx,
            cluster: &self.cluster,
            bypass_cluster_check: true,
            apply_replication: true,
            skip_write_side_effects: false,
            peer_addr,
            monitor_registry: &self.monitor_registry,
            authenticated_user: "default",
            tracking_table: &self.tracking_table,
            blocking: None,
        });

        if !out.is_empty() && out[0] == b'-' {
            let tail = out.len().min(512);
            log_warn!(
                "Replication: Replicated command error (non-fatal): {}",
                String::from_utf8_lossy(&out[..tail])
            );
        }
        Ok(())
    }
}

/// The address to list a replica under: its connection's IP, with the port it
/// announced via `REPLCONF listening-port` when it announced one. Falling back
/// to the connection's source port (as this always used to) reports an
/// ephemeral port nobody can connect to.
pub(crate) fn replica_endpoint(peer_addr: &str, listening_port: Option<u16>) -> (String, u16) {
    let (ip, source_port) = peer_ip_port(peer_addr);
    match listening_port {
        Some(p) if p != 0 => (ip, p),
        _ => (ip, source_port),
    }
}

/// Split `peer_addr` from the accepted TCP connection into INFO `slaveN:ip` / `port` fields.
pub(crate) fn peer_ip_port(peer_addr: &str) -> (String, u16) {
    use std::net::SocketAddr;
    if let Ok(sa) = peer_addr.parse::<SocketAddr>() {
        return match sa {
            SocketAddr::V4(v4) => (v4.ip().to_string(), v4.port()),
            SocketAddr::V6(v6) => (format!("[{}]", v6.ip()), v6.port()),
        };
    }
    if let Some((host, port_s)) = peer_addr.rsplit_once(':')
        && let Ok(p) = port_s.parse::<u16>()
    {
        return (host.to_string(), p);
    }
    (peer_addr.to_string(), 0)
}

/// Called when a client sends PSYNC — this connection is now a replica.
/// We send the full RDB dump then register with the hub to receive
/// ongoing write command propagation.
#[allow(clippy::too_many_arguments)]
pub async fn handle_replica_connection<R>(
    reader: R,
    resp_tx: mpsc::Sender<Vec<u8>>,
    write_task: tokio::task::JoinHandle<()>,
    store: store::Store,
    _lastsave: Arc<parking_lot::RwLock<i64>>,
    _aof_tx: mpsc::UnboundedSender<Vec<u8>>,
    _aof_enabled: bool,
    _config: Arc<crate::config::Config>,
    _started_at: Arc<std::time::Instant>,
    index_store: crate::vector::IndexStore,
    _dict_store: crate::vector::DictStore,
    _wait_table: Arc<crate::wait_table::WaitTable>,
    _ts_index: store::SharedTsIndex,
    _pubsub: crate::pubsub::PubSubHub,
    hub_tx: crate::replication::HubTx,
    repl_state: Arc<crate::replication::ReplState>,
    peer_addr: String,
    // Legacy `SYNC` omits `+FULLRESYNC`; first payload is `$len\r\n` + RDB (Redis test helper attach_to_replication_stream).
    legacy_sync: bool,
    _leftover: Vec<u8>,
    // The port the replica announced with `REPLCONF listening-port`, if any.
    listening_port: Option<u16>,
    // What the replica asked to continue from (`PSYNC <replid> <offset>`).
    psync: Option<crate::replication::PsyncRequest>,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let rollback_hub = |hub: &crate::replication::HubTx, id: u64| {
        let _ = hub.send(crate::replication::HubMsg::Deregister(id));
    };

    let replica_id = crate::replication::next_replica_id();

    // ── 1. The stream queue. The hub starts filling it at the cut below.
    // Must be **unbounded**: the forwarding task doesn't drain until the payload is sent; a bounded
    // queue hits capacity during long dumps and evicts the replica (connected_slaves goes to 0).
    let (replica_tx, mut replica_rx) = mpsc::unbounded_channel::<crate::replication::ReplPayload>();
    let (rdb_enqueued_tx, rdb_enqueued_rx) = tokio::sync::oneshot::channel();
    // Shared with the hub: it increments this on enqueue, we decrement it after each
    // successful flush. Lets the hub evict us if we fall far enough behind instead of
    // queueing an unbounded backlog — see `replication::mod::start_hub`.
    let pending_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let resp_tx_fwd = resp_tx.clone();
    let pending_bytes_fwd = Arc::clone(&pending_bytes);
    let forward_task = tokio::spawn(async move {
        if rdb_enqueued_rx.await.is_err() {
            return;
        }
        // Fewer replica-socket packets / writer wakeups than one Vec per propagated write.
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
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        pending_bytes_fwd.fetch_sub(batch.len() as u64, Ordering::Relaxed);
                        let _ = resp_tx_fwd.send(batch).await.is_err();
                        return;
                    }
                }
            }

            pending_bytes_fwd.fetch_sub(batch.len() as u64, Ordering::Relaxed);
            if resp_tx_fwd.send(batch).await.is_err() {
                break;
            }
        }
    });

    // ── 2. A replica that only lost its connection is sent what it missed.
    // Failing that, take the cut: register with the hub and serialise the
    // dataset at one instant, so every write is in exactly one of {payload,
    // stream}.
    let (peer_ip, peer_port) = replica_endpoint(&peer_addr, listening_port);
    let endpoint = format!("{peer_ip}:{peer_port}");
    let registration = crate::replication::ReplicaRegistration {
        replica_id,
        addr: peer_addr.clone(),
        peer_ip,
        peer_port,
        tx: replica_tx,
        pending_bytes,
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
                let index_store = Arc::clone(&index_store);
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
                    rollback_hub(&hub_tx, replica_id);
                    return Err(e.into());
                }
                Err(e) => {
                    rollback_hub(&hub_tx, replica_id);
                    return Err(e.into());
                }
            }
        }
    };

    // ── 3. `+CONTINUE`; or `+FULLRESYNC` (omitted for legacy pre-PSYNC
    // `SYNC\r\n`) and then the payload.
    let mut frames: Vec<Vec<u8>> = Vec::with_capacity(3);
    match full_sync {
        None => frames.push(format!("+CONTINUE {}\r\n", repl_state.repl_id()).into_bytes()),
        Some(sync) => {
            if !legacy_sync {
                frames.push(sync.announcement(&repl_state.repl_id()).into_bytes());
            }
            frames.push(format!("${}\r\n", sync.payload.len()).into_bytes());
            frames.push(sync.payload);
            if let Some(replica) = repl_state.replica(replica_id) {
                replica.set_state(crate::replication::ReplicaState::SendBulk);
            }
        }
    }
    let full = frames.len() > 1;
    for frame in frames {
        if resp_tx.send(frame).await.is_err() {
            rollback_hub(&hub_tx, replica_id);
            drop(rdb_enqueued_tx);
            return Ok(());
        }
    }

    let _ = rdb_enqueued_tx.send(());
    if full {
        log_info!("Replication: Sent full RDB to replica; streaming queued writes from hub");
    }

    // ── 4. NEW: spawn ACK reader ─────────────────────────────────────────
    // The replica sends REPLCONF ACK <offset> every second.
    // We must drain the socket or the kernel buffer fills and TCP stalls.
    let hub_tx_ack = hub_tx.clone();
    let ack_reader_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut reader = reader;
        let mut buf = Vec::with_capacity(128);
        let mut tmp = [0u8; 128];
        loop {
            match reader.read(&mut tmp).await {
                Ok(0) | Err(_) => break, // replica disconnected
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                    // Parse all complete RESP commands in buf
                    let mut pos = 0;
                    while let Some((consumed, cmd)) =
                        crate::resp::parse::parse_resp_command(&buf[pos..])
                    {
                        // REPLCONF ACK <offset>
                        if cmd.len() == 3
                            && cmd[0].eq_ignore_ascii_case(b"REPLCONF")
                            && cmd[1].eq_ignore_ascii_case(b"ACK")
                            && let Ok(s) = std::str::from_utf8(cmd[2])
                            && let Ok(offset) = s.parse::<u64>()
                        {
                            let _ = hub_tx_ack
                                .send(crate::replication::HubMsg::AckOffset { replica_id, offset });
                        }
                        pos += consumed;
                    }
                    if pos > 0 {
                        buf.drain(0..pos);
                    }
                    // An ACK is a few dozen bytes: a replica that sends this
                    // much that is not one is not a replica.
                    if buf.len() > ACK_PENDING_MAX {
                        log_warn!(
                            "Replication: replica {replica_id} sent what is not REPLCONF ACK; disconnecting it"
                        );
                        break;
                    }
                }
            }
        }
    });

    // ── 5. Wait for write_task (replica disconnect closes resp_tx), or for
    // `forward_task` to end on its own — which also happens when the hub evicts us
    // for exceeding the backlog byte limit (it drops our `tx`, closing the channel
    // `forward_task` reads from). Either way, force the socket writer closed so the
    // whole connection tears down instead of the reader lingering on a live replica
    // the hub has already forgotten about.
    let mut write_task = write_task;
    let mut forward_task = forward_task;
    tokio::select! {
        _ = &mut write_task => {}
        _ = &mut forward_task => {
            drop(resp_tx);
        }
        // A replica that has gone silent: see `ReplState::replica_timed_out`.
        _ = repl_state.replica_timed_out(replica_id) => {
            log_warn!(
                "Replication: disconnecting timedout replica {endpoint}: no REPLCONF ACK for {} s (repl-timeout)",
                repl_state.repl_timeout().as_secs()
            );
            forward_task.abort();
            write_task.abort();
        }
    }
    ack_reader_task.abort();

    // Hub drops channel list + ACK + connected_slaves (paired with hub RegisterChannel fetch_add).
    let _ = hub_tx.send(crate::replication::HubMsg::Deregister(replica_id));
    log_info!("Replication: Replica disconnected");
    Ok(())
}

#[cfg(test)]
mod repl_apply_env_tests {
    use super::ReplicationApplyEnv;
    use crate::replication::ReplState;

    #[tokio::test]
    async fn replication_apply_env_reuses_scaffold_for_many_writes() {
        let cfg = std::sync::Arc::new(crate::config::Config::default_replica());
        let store = crate::store::new_store(cfg.databases);
        let repl_state = std::sync::Arc::new(ReplState::new_master());
        let index = crate::vector::new_index_store();
        let dict = crate::vector::new_dict_store();
        let wait = std::sync::Arc::new(crate::wait_table::WaitTable::new());
        let ts = crate::store::new_ts_index();
        let (compaction_tx, compaction_rx) = crate::vector::new_compaction_channel();
        drop(compaction_rx);
        let (hub_tx, _hub_rx) = tokio::sync::mpsc::unbounded_channel();
        let starter = super::ReplicaStarter {
            store: std::sync::Arc::clone(&store),
            index_store: std::sync::Arc::clone(&index),
            dict_store: std::sync::Arc::clone(&dict),
            wait_table: std::sync::Arc::clone(&wait),
            ts_index: std::sync::Arc::clone(&ts),
            config: std::sync::Arc::clone(&cfg),
            compaction_tx: compaction_tx.clone(),
            repl_state: std::sync::Arc::clone(&repl_state),
            hub_tx,
            pubsub: crate::pubsub::PubSubHub::new(),
            client_registry: crate::client_registry::new_client_registry(),
            tracking_table: crate::tracking::new_tracking_table(),
            monitor_registry: crate::monitor::new_monitor_registry(),
        };
        let env = ReplicationApplyEnv::new(&starter);

        let mut db = 0usize;
        let peer = "replica:test".to_string();

        const N: u32 = 1024;
        for i in 0..N {
            let ks = format!("rk{i}");
            env.apply_command(
                vec![b"SET".to_vec(), ks.as_bytes().to_vec(), b"v".to_vec()],
                &store,
                &repl_state,
                &index,
                &dict,
                &wait,
                &ts,
                &cfg,
                &compaction_tx,
                &mut db,
                &crate::watch::NoopWatchRegistry,
                peer.as_str(),
            )
            .expect("apply SET");
        }

        env.apply_command(
            vec![b"GET".to_vec(), br"rk0".to_vec()],
            &store,
            &repl_state,
            &index,
            &dict,
            &wait,
            &ts,
            &cfg,
            &compaction_tx,
            &mut db,
            &crate::watch::NoopWatchRegistry,
            peer.as_str(),
        )
        .unwrap();

        let shard = crate::store::get_shard(&store[0], "rk0");
        let map = shard.read();
        match map.get("rk0") {
            Some(crate::store::ValueRef::String(b)) => assert_eq!(&b[..], b"v"),
            _ => panic!("unexpected value variant for rk0"),
        }
    }
}

/// Regression coverage for a live bug found via `smoke/replication_blackbox.sh`
/// against a real 3-container re-point: a replica moved from master-a to
/// master-b reported `master_link_status:down` in `INFO replication`
/// indefinitely, even though both the connection log and every other
/// replication-state field showed the re-point had fully succeeded.
///
/// Root cause: link state is shared, but a re-point briefly runs two
/// replication sessions concurrently — the new task connects and starts
/// streaming before the superseded task notices its epoch is stale and
/// returns. The superseded task's `SlaveUpstreamGuard` then drops and (pre-fix)
/// unconditionally cleared the flag, clobbering the new task's state. These
/// tests exercise the guard's generation bookkeeping directly — no networking
/// involved, so they catch the exact race the container-level test found, at
/// unit-test speed.
#[cfg(test)]
mod slave_upstream_guard_tests {
    use super::SlaveUpstreamGuard;
    use crate::replication::{LinkState, ReplState};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    fn up(rs: &ReplState) -> bool {
        rs.slave_link_up.load(Ordering::Acquire)
    }

    /// An open socket is not a
    /// healthy link. The link is only up once the dataset is synced.
    #[test]
    fn connecting_and_syncing_are_not_up() {
        let rs = Arc::new(ReplState::new_master());
        let epoch = rs.begin_replication_epoch();
        let guard = SlaveUpstreamGuard::connect(&rs, epoch);
        assert!(!up(&rs), "a link that is still connecting must report down");
        assert_eq!(rs.link_state(), LinkState::Connecting);

        guard.mark_syncing();
        assert!(!up(&rs), "a link that is still syncing must report down");
        assert_eq!(rs.link_state(), LinkState::Sync);

        guard.mark_synced();
        assert!(up(&rs));
        assert_eq!(rs.link_state(), LinkState::Connected);
    }

    #[test]
    fn a_superseded_task_cannot_claim_the_link() {
        let rs = Arc::new(ReplState::new_master());
        let stale_gen = rs.begin_replication_epoch();
        rs.begin_replication_epoch(); // supersede before connect() even runs
        let guard = SlaveUpstreamGuard::connect(&rs, stale_gen);
        guard.mark_synced();
        assert!(
            !up(&rs),
            "a task that was already superseded must not mark the link up"
        );
    }

    #[test]
    fn drop_marks_the_link_down_when_still_the_current_generation() {
        let rs = Arc::new(ReplState::new_master());
        let epoch = rs.begin_replication_epoch();
        {
            let guard = SlaveUpstreamGuard::connect(&rs, epoch);
            guard.mark_synced();
            assert!(up(&rs));
        }
        assert!(
            !up(&rs),
            "an ordinary disconnect, with no re-point since, must clear the flag"
        );
        assert_eq!(rs.link_state(), LinkState::Connect);
        assert!(rs.master_link_down_since_seconds().is_some());
    }

    /// The actual bug: a re-point's new task connects and marks the link up,
    /// *then* the superseded old task's guard finally drops. The drop must be
    /// a no-op — it must not clobber the newer generation's state.
    #[test]
    fn stale_guard_drop_does_not_clobber_a_newer_generations_link_state() {
        let rs = Arc::new(ReplState::new_master());
        let old_gen = rs.begin_replication_epoch();
        let old_guard = SlaveUpstreamGuard::connect(&rs, old_gen);
        old_guard.mark_synced();
        assert!(up(&rs));

        // Re-point: a new task claims a newer generation and syncs — exactly
        // what REPLICAOF does while already a slave.
        let new_gen = rs.begin_replication_epoch();
        let new_guard = SlaveUpstreamGuard::connect(&rs, new_gen);
        new_guard.mark_synced();
        assert!(up(&rs));

        // The superseded old task's guard finally drops (its loop noticed the
        // epoch mismatch and returned). Before the fix, this unconditionally
        // marked the link down despite the new generation's connection being
        // fully live and streaming.
        drop(old_guard);
        assert!(
            up(&rs),
            "a superseded task's cleanup must not clear a newer generation's live link"
        );
    }
}

/// A full resync from an empty
/// master silently discarded a larger local dataset.
#[cfg(test)]
mod full_sync_guard_tests {
    use super::{full_sync_refusal, load_full_sync_payload};
    use crate::replication::FullSyncGuard;

    #[test]
    fn an_empty_master_is_refused_when_the_replica_holds_data() {
        let reason = full_sync_refusal(FullSyncGuard::Empty, 50, 502, 0).expect("must refuse");
        assert!(reason.contains("502"), "{reason}");
    }

    /// The first sync of a fresh replica, and any sync where there is nothing
    /// local to lose, must go through untouched.
    #[test]
    fn nothing_local_means_nothing_to_protect() {
        for guard in [
            FullSyncGuard::Off,
            FullSyncGuard::Empty,
            FullSyncGuard::Shrink,
        ] {
            assert!(full_sync_refusal(guard, 50, 0, 0).is_none());
            assert!(full_sync_refusal(guard, 50, 0, 1_000).is_none());
        }
    }

    #[test]
    fn off_accepts_everything() {
        assert!(full_sync_refusal(FullSyncGuard::Off, 50, 1_000_000, 0).is_none());
    }

    /// `empty` is the narrow default: a master with *some* data is the
    /// authority, however much smaller it is.
    #[test]
    fn empty_mode_only_refuses_a_keyless_master() {
        assert!(full_sync_refusal(FullSyncGuard::Empty, 50, 1_000, 1).is_none());
        assert!(full_sync_refusal(FullSyncGuard::Empty, 50, 1_000, 0).is_some());
    }

    #[test]
    fn shrink_mode_refuses_below_the_ratio() {
        // 50%: 499 of 1000 is refused, 500 of 1000 is accepted.
        assert!(full_sync_refusal(FullSyncGuard::Shrink, 50, 1_000, 499).is_some());
        assert!(full_sync_refusal(FullSyncGuard::Shrink, 50, 1_000, 500).is_none());
        // A master that is larger is never a problem.
        assert!(full_sync_refusal(FullSyncGuard::Shrink, 50, 1_000, 5_000).is_none());
        // Ratio 0 degenerates to `empty`.
        assert!(full_sync_refusal(FullSyncGuard::Shrink, 0, 1_000, 1).is_none());
        assert!(full_sync_refusal(FullSyncGuard::Shrink, 0, 1_000, 0).is_some());
    }

    /// A payload that does not parse must be an error. It used to be logged
    /// and then treated as an empty dataset — which was swapped in over the
    /// live one.
    #[test]
    fn a_corrupt_payload_is_an_error_not_an_empty_dataset() {
        // Valid magic, then garbage.
        let mut payload = crate::snapshot::KDB_MAGIC.to_vec();
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&3u32.to_le_bytes());
        payload.extend_from_slice(b"key");
        payload.push(250); // no such type tag
        assert!(load_full_sync_payload(&payload, 16).is_err());

        // Not a KDB image and not an RDB either.
        assert!(load_full_sync_payload(b"definitely not a dataset", 16).is_err());
    }

    /// Values, expiries and the key count all survive the payload round trip.
    #[test]
    fn payload_round_trip_keeps_keys_and_ttls() {
        let store = crate::store::new_store(2);
        for (db, key) in [(0usize, "a"), (0, "b"), (1, "c")] {
            crate::store::get_shard(&store[db], key)
                .write()
                .insert_str(key, &b"v"[..]);
        }
        let in_an_hour = crate::commands::helpers::now_ms() + 3_600_000;
        crate::store::get_shard(&store[0], "a")
            .write()
            .set_expires("a", in_an_hour);
        // A time to live for a key that does not exist is none.
        assert!(
            !crate::store::get_shard(&store[0], "ghost")
                .write()
                .set_expires("ghost", in_an_hour)
        );

        let payload = crate::snapshot::full_sync_payload(&store, None).unwrap();
        let staged = load_full_sync_payload(&payload, 2).expect("payload loads");
        assert_eq!(staged.keys, 3);
        assert_eq!(staged.keys_with_ttl, 1);
        assert_eq!(
            crate::store::get_shard(&staged.store[0], "a")
                .read()
                .expires("a"),
            Some(in_an_hour)
        );
        assert_eq!(
            crate::store::get_shard(&staged.store[0], "b")
                .read()
                .expires("b"),
            None
        );
        assert!(
            crate::store::get_shard(&staged.store[1], "c")
                .read()
                .contains_key("c")
        );
    }

    /// A key whose TTL ran out while the payload was in transit is gone on
    /// arrival, not resurrected without a TTL.
    #[test]
    fn keys_already_expired_are_dropped_on_load() {
        let store = crate::store::new_store(1);
        for key in ["live", "dead"] {
            crate::store::get_shard(&store[0], key)
                .write()
                .insert_str(key, &b"v"[..]);
        }
        let past = crate::commands::helpers::now_ms() - 5_000;
        crate::store::get_shard(&store[0], "dead")
            .write()
            .set_expires("dead", past);

        let payload = crate::snapshot::full_sync_payload(&store, None).unwrap();
        let staged = load_full_sync_payload(&payload, 1).expect("payload loads");
        assert_eq!(staged.keys, 1);
        assert!(
            !crate::store::get_shard(&staged.store[0], "dead")
                .read()
                .contains_key("dead")
        );
    }
}

#[cfg(test)]
mod stream_transaction_tests {
    use super::*;

    fn enc(parts: &[&str]) -> Vec<u8> {
        let parts: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
        crate::resp::write::encode_resp_command(&parts)
    }

    fn whole(t: StreamTransaction) -> WholeTransaction {
        match t {
            StreamTransaction::Whole(whole) => whole,
            StreamTransaction::Partial => panic!("partial"),
            StreamTransaction::None => panic!("none"),
        }
    }

    #[test]
    fn a_command_that_is_not_multi_is_not_a_transaction() {
        let mut scanned = 0;
        let buf = enc(&["SET", "a", "1"]);
        assert!(matches!(
            transaction_at(&buf, &mut scanned),
            StreamTransaction::None
        ));
        assert!(matches!(
            transaction_at(b"", &mut scanned),
            StreamTransaction::None
        ));
    }

    #[test]
    fn a_transaction_is_whole_only_once_its_exec_is_there() {
        let mut stream = enc(&["MULTI"]);
        stream.extend_from_slice(&enc(&["SET", "a", "1"]));
        stream.extend_from_slice(&enc(&["PING"]));
        stream.extend_from_slice(&enc(&["SET", "b", "2"]));
        let exec_at = stream.len();
        stream.extend_from_slice(&enc(&["EXEC"]));
        let len = stream.len();
        stream.extend_from_slice(&enc(&["SET", "after", "3"]));

        // Arriving a few bytes at a time: partial until the `EXEC` is
        // whole, and what was looked through is not looked through again.
        let mut scanned = 0;
        for cut in 1..len {
            assert!(
                matches!(
                    transaction_at(&stream[..cut], &mut scanned),
                    StreamTransaction::Partial | StreamTransaction::None
                ),
                "at {cut}"
            );
            assert!(scanned <= exec_at);
        }
        assert_eq!(scanned, exec_at);
        let t = whole(transaction_at(&stream, &mut scanned));
        assert_eq!(t.len, len, "MULTI to EXEC, and nothing after");
        assert!(t.heartbeat);
        assert_eq!(
            t.commands,
            vec![
                vec![b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()],
                vec![b"SET".to_vec(), b"b".to_vec(), b"2".to_vec()],
            ]
        );
    }

    #[test]
    fn an_empty_transaction_is_whole_and_applies_nothing() {
        let mut stream = enc(&["multi"]);
        stream.extend_from_slice(&enc(&["exec"]));
        let t = whole(transaction_at(&stream, &mut 0));
        assert_eq!(t.len, stream.len());
        assert!(t.commands.is_empty());
    }
}
