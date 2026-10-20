use crate::replication::{ReplState, start_hub};

use config::Config;
use parking_lot::RwLock;
#[allow(unused_imports)]
use std::net::SocketAddr;
use std::{sync::Arc, time::Instant};
#[allow(unused_imports)]
use tokio::net::TcpListener;
use tokio::sync::mpsc;

// First, so its logging macros are in scope in every module after it.
#[macro_use]
pub mod loglevel;
mod acl;
mod aof;
pub mod client_registry;
pub mod cluster;
pub mod cmdstats;
pub mod command_gate;
mod commands;
mod config;
pub mod eviction;
pub mod field_expiry;
pub mod keyspace_events;
pub mod keyspec;
pub mod latency;
pub mod lazyfree;
pub mod listen;
#[cfg(feature = "lua")]
mod lua_vm_blocking;
mod member_set;
pub mod memory_use;
pub mod monitor;
pub mod node_tls;
pub mod pause;
pub mod protected_mode;
mod pubsub;
pub mod replication;
mod resp;
mod script_clock;
mod server;
mod shared_bytes;
pub mod shutdown;
pub mod slowlog;
mod snapshot;
mod stats;
mod store;
pub mod tag_locks;
pub mod tracking;
pub mod types;
mod update_check;
mod vector;
pub mod wait_table;
pub mod watch;
pub mod write_gate;
pub mod write_order;

#[cfg(feature = "tls")]
mod tls;

pub use crate::loglevel::{LOG_LEVEL, log};
use crate::shutdown::{SHUTDOWN_MODE, SHUTDOWN_PENDING, mode};
use std::sync::atomic::Ordering::Relaxed;

#[cfg(target_os = "linux")]
mod server_uring;
#[cfg(target_os = "linux")]
use config::IoModel;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, Ordering};

/// Counts what it allocates: `maxmemory` is measured against that. See
/// `memory_use`.
#[global_allocator]
static ALLOCATOR: memory_use::Counting = memory_use::Counting;

pub static RUNTIME_MAXMEMORY: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

fn print_banner(threads: usize, config: &Arc<Config>) {
    let version = env!("CARGO_PKG_VERSION");

    // get system info
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);

    let mem_gb = {
        // read from /proc/meminfo on linux, fallback on mac
        #[cfg(target_os = "linux")]
        {
            std::fs::read_to_string("/proc/meminfo")
                .ok()
                .and_then(|s| {
                    s.lines()
                        .find(|l| l.starts_with("MemTotal"))
                        .and_then(|l| l.split_whitespace().nth(1))
                        .and_then(|n| n.parse::<u64>().ok())
                })
                .map(|kb| kb / 1024 / 1024)
                .unwrap_or(0)
        }
        #[cfg(not(target_os = "linux"))]
        {
            0u64
        }
    };

    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let bind = &config.bind;
    let port = config.port;
    let databases = config.databases;

    let aof_status = if config.aof { "enabled" } else { "disabled" };

    println!(
        r#"                                                                                     
                                                                                                    
                                                   :::.                                     
                                .:-==-:.       :#%@%%%@%%=                                  
                          -*@@%%%######%%%%%=:%%+:::::::=%%:                                
                      :*%%#+-::::::::::::::=*%#:::::::::::#%.                               
                    +%%*::::::::::::::::::::::::::::%@%-::+%#.                              
                  =%%=::::::::::::::::::::::::::::::=#+:::*%@@@#.                           
                .%%+:::::::::::::::::::::::::::::::::::::+%@@@@@@%+.                  ██╗  ██╗ ██╗           ██╗ ███████╗  ███████╗ 
               -%%-::::::::::::::::::::::::::::::::::::*%%*+#%@@@@@%#:                ██║ ██╔╝ ╚═╝           ╚═╝ ╚██╔══██╗ ╚██╔══██╗
              .%%:::::::::::::::::::::::::::::::::%@@%%*.        =#%@@%-              █████╔╝  ██╗ ██╗   ██╗ ██╗  ██║  ██║  ██████╔╝
              +@=::::::::::::::::::::::::::::::::+@#.               .=%@%-            ██╔═██╗  ██║ ╚██╗ ██╔╝ ██║  ██║  ██║  ██╔══██╗
              %#::::::::::::::::::::::::::::::::+%%:                    =%%:          ██║ ╚██╗ ██║  ╚████╔╝  ██║ ███████╔╝ ███████╔╝
              @#:::::::::::::::::::::::::::::::*@%=                       .#+         ╚═╝  ╚═╝ ╚═╝   ╚═══╝   ╚═╝ ╚══════╝  ╚══════╝ 
              %%:::::::::::::::::::::::::::::+%@%-                          .               
              =@*::::::::::::::::::::::::::*%@@*                                            
               *@*::::=+:::::::::::::::-*%@@%%:                                             
                *@#-:##::::::::::-=+*%@@@@%*.                                               
                 :#@%@*::::::*%@@@%%@@@%*:                                                  
                   .*%%=:::*%%%%%%%@@%+                                                     
                     .#@@@%*-   .#%@*.                                                      
                      .#@*        :%@%:                                                     
                        %@*         =%@#                                                    
                         #@#          +%%+                                                  
                          *%%+:...     .*@%*:....                                           
                       *%%%%%%%@%%%%+ %%%%%%%@@%%%%-                                        
                               .+-            :+.                                           
                                                                                                    
    "#
    );

    println!("  KiviDB v{version} - A fast Redis®-compatible store written in Rust");
    println!("  https://kividb.io");
    println!();
    println!("  Licensed under the GNU Affero General Public License, version 3 (AGPLv3)");
    println!();
    println!("  * Version        : {version}");
    let mode = if config.cluster_enabled {
        "cluster"
    } else {
        "standalone"
    };
    println!("  * Mode           : {mode}");
    println!("  * OS             : {os} {arch}");
    if mem_gb > 0 {
        println!("  * Memory         : {mem_gb}GiB available");
    }
    println!("  * CPUs           : {cores} cores detected");
    println!("  * Listening on   : {bind}:{port}");

    #[cfg(feature = "tls")]
    if config.tls_enabled() {
        println!("  * TLS port       : {}:{}", config.bind, config.tls_port);
    }

    // Cluster-specific lines, only when enabled
    if config.cluster_enabled {
        let bus_port = port + 10000;
        println!("  * Cluster bus    : {bind}:{bus_port}");
        println!("  * Node timeout   : {}ms", config.cluster_node_timeout);
        // The node ID isn't known here — it's generated/loaded inside
        // ClusterState::from_disk_or_fresh, which happens AFTER the banner.
        // We could either call it before banner, or print a placeholder
        // and log the actual ID elsewhere. The cleanest approach is to
        // print "Node ID: (pending init)" and rely on a separate log line
        // emitted after ClusterState construction.
        println!("  * Node ID        : (printed after cluster init)");
    }

    println!("  * Worker threads : {threads}");
    println!("  * AOF            : {aof_status}");
    // Name the source recovery will actually use. This line used to read
    // "KDB snapshot (SAVE / BGSAVE)" unconditionally, which was true even with
    // --aof: the AOF was written but never read back, so an operator who
    // enabled it and mounted a volume still lost everything since the last
    // snapshot on a crash.
    println!(
        "  * Persistence    : {}",
        if config.aof {
            "AOF (recovered on start) + KDB snapshot (SAVE / BGSAVE)"
        } else {
            "KDB snapshot (SAVE / BGSAVE)"
        }
    );
    println!("  * Databases      : {databases}");
    println!(
        "  * ACL file       : {}",
        if config.aclfile.is_empty() {
            "none".into()
        } else {
            config.aclfile.clone()
        }
    );
    println!();
}

#[cfg(target_os = "linux")]
fn is_uring_available() -> bool {
    use std::os::unix::io::RawFd;

    #[repr(C)]
    struct IoUringParams {
        sq_entries: u32,
        cq_entries: u32,
        flags: u32,
        sq_thread_cpu: u32,
        sq_thread_idle: u32,
        features: u32,
        wq_fd: u32,
        resv: [u32; 3],
        sq_off: [u8; 40],
        cq_off: [u8; 40],
    }

    let mut params = IoUringParams {
        sq_entries: 0,
        cq_entries: 0,
        flags: 0,
        sq_thread_cpu: 0,
        sq_thread_idle: 0,
        features: 0,
        wq_fd: 0,
        resv: [0; 3],
        sq_off: [0; 40],
        cq_off: [0; 40],
    };

    let ret = unsafe {
        libc::syscall(
            libc::SYS_io_uring_setup,
            1u32,
            &mut params as *mut IoUringParams,
        )
    };

    if ret < 0 {
        return false;
    }

    unsafe { libc::close(ret as RawFd) };
    true
}

// Entry point
fn main() {
    // An error that ends the server is logged like any other line, rather
    // than printed as a Rust `Debug` dump.
    if let Err(e) = run() {
        log_fatal!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Parsed by hand rather than with `Config::parse()`, to keep the matches:
    // the config file must not override an option given on the command line,
    // and only the matches know which those are.
    let matches = <Config as clap::CommandFactory>::command().get_matches();
    let mut cfg =
        <Config as clap::FromArgMatches>::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    let from_cli =
        |option: &str| matches.value_source(option) == Some(clap::parser::ValueSource::CommandLine);
    cfg.bind_explicit =
        matches.value_source("bind") == Some(clap::parser::ValueSource::CommandLine);
    match cfg.apply_config_file(&from_cli) {
        Ok(warnings) => {
            for w in warnings {
                log_warn!("{w}");
            }
        }
        Err(e) => {
            log_fatal!("Bad directive or wrong number of arguments — {e}");
            std::process::exit(1);
        }
    }
    // `dir`: everything on disk is relative to it. The config file's own path
    // is pinned down first, so CONFIG REWRITE still finds it.
    if !cfg.dir.is_empty() {
        if let Ok(absolute) = std::path::absolute(&cfg.configfile) {
            cfg.configfile = absolute.to_string_lossy().into_owned();
        }
        if let Err(e) = std::env::set_current_dir(&cfg.dir) {
            log_fatal!("Can't chdir to '{}': {e}", cfg.dir);
            std::process::exit(1);
        }
    }
    // Refuse a configuration that would start something other than what was
    // asked for — most importantly `--tls-port` on a build with no TLS in it,
    // which used to come up serving plaintext without a word in the log.
    match cfg.validate(cfg!(feature = "tls")) {
        Ok(warnings) => {
            for w in warnings {
                log_warn!("{w}");
            }
        }
        Err(e) => {
            log_fatal!("{e}");
            std::process::exit(1);
        }
    }
    if let Err(e) = crate::node_tls::init(&cfg) {
        log_fatal!("TLS between nodes: {e}");
        std::process::exit(1);
    }
    crate::protected_mode::set(cfg.protected_mode);
    crate::protected_mode::set_bind_explicit(cfg.bind_explicit);
    let threads = cfg.resolve_threads();
    let config = Arc::new(cfg);
    RUNTIME_MAXMEMORY.store(config.maxmemory, std::sync::atomic::Ordering::Relaxed);
    // Without Lua there are no scripts to run beside each other, and the
    // commands are spared looking for a tag to wait on.
    tag_locks::set_enabled(cfg!(feature = "lua") && config.lua_parallel_scripts);
    eviction::configure(&config);
    keyspace_events::KS_FLAGS.store(
        keyspace_events::parse_ks_flags(&config.notify_keyspace_events),
        std::sync::atomic::Ordering::Relaxed,
    );
    let started_at = Instant::now();
    if let Some(l) = log::parse_level(&config.loglevel) {
        LOG_LEVEL.store(l, std::sync::atomic::Ordering::Relaxed);
    }
    loglevel::set_json(config.log_format == "json");
    if let Err(e) = loglevel::set_logfile(&config.logfile) {
        log_fatal!(
            "Can't open the log file '{}': {e}. Check that its directory exists and is \
             writable by this user, or leave logfile unset to log to standard error",
            config.logfile
        );
        std::process::exit(1);
    }

    // Whether a newer release is out on GitHub: the first thing printed.
    // Waits for the reply at most 1.5 s; a later one is dropped. See
    // src/update_check.rs.
    if let Some(pending) = update_check::start(&config) {
        update_check::show(pending);
    }

    print_banner(threads, &config);
    log_info!(
        "KiviDB {} starting: pid {}, {} build, config file {}",
        env!("CARGO_PKG_VERSION"),
        std::process::id(),
        update_check::variant(),
        if std::path::Path::new(&config.configfile).is_file() {
            format!("'{}'", config.configfile)
        } else {
            "none".to_string()
        }
    );

    #[cfg(target_os = "linux")]
    match config.io_model {
        IoModel::Uring => {
            log_info!("I/O model      : io_uring (forced)");
            run_uring(config, started_at)?;
        }
        IoModel::Tokio => {
            log_info!("I/O model      : tokio (forced)");
            run_tokio(config, started_at)?;
        }
        IoModel::Auto => {
            if is_uring_available() {
                log_info!("I/O model      : io_uring");
                run_uring(config, started_at)?;
            } else {
                log_info!("I/O model      : tokio (io_uring unavailable)");
                run_tokio(config, started_at)?;
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        log_info!("I/O model      : tokio");
        run_tokio(Arc::clone(&config), started_at)?;
    }

    Ok(())
}

/// Walk keys to populate TEXT / inverted / FLAT data, and HNSW only when graphs
/// were not restored from the snapshot (see [`vector::backfill_from_db_progress`]
/// `skip_hnsw_if_populated`).
///
/// Caller must set [`vector::SNAPSHOT_INDEX_REBUILD`] true before clients are
/// accepted; the background thread clears it when this returns.
fn backfill_snapshot_search_indexes(
    store: &store::Store,
    index_store: &vector::IndexStore,
    names: &[String],
) {
    let mut n = 0usize;
    for name in names {
        let def_for_fill = {
            let idx = index_store.read();
            match idx.get(name) {
                Some(d) => d.clone(),
                None => {
                    log_warn!("Index `{name}` missing after registration — skipping backfill");
                    continue;
                }
            }
        };
        let db_i = def_for_fill.db_index.min(store.len().saturating_sub(1));
        vector::backfill_from_db_progress(&store[db_i], &def_for_fill, true, true);
        n += 1;
    }
    if n > 0 {
        log_info!("Snapshot index merge pass finished for {n} index(es)");
    }
}

fn spawn_snapshot_index_backfill_thread(
    store: store::Store,
    index_store: vector::IndexStore,
    index_names: Vec<String>,
) {
    std::thread::spawn(move || {
        backfill_snapshot_search_indexes(&store, &index_store, &index_names);
        vector::SNAPSHOT_INDEX_REBUILD.store(false, std::sync::atomic::Ordering::Release);
        log_info!("Search index startup complete — full command set is available");
    });
}

/// Load (or create) this node's cluster state, exiting if the cluster config
/// file exists but cannot be used. See `ClusterState::from_config`.
fn load_cluster_state(config: &Config) -> cluster::ClusterState {
    match cluster::ClusterState::from_config(config) {
        Ok(state) => state,
        Err(e) => {
            log_fatal!("{e}");
            std::process::exit(1);
        }
    }
}

/// Start following a master if this node is configured to: `--replicaof`
/// in standalone mode, or the replica role recorded in `nodes.conf` in cluster
/// mode.
///
/// The cluster case used to be handled inline on each startup path, and both
/// copies were wrong. The io_uring one called `tokio::spawn` from a thread with
/// no runtime, which panics, so a cluster replica could not restart on the
/// default I/O model at all. And both, on finding that the recorded master was
/// missing from the node table, returned `Ok(())` from the function that runs
/// the server — a clean exit, with a log line claiming it was "staying as
/// master temporarily".
fn start_configured_replication(
    config: &Config,
    cluster_state: &cluster::ClusterState,
    starter: &replication::replica_client::ReplicaStarter,
) {
    if let Some((host, port)) = config.replicaof_addr() {
        starter.start(host, port);
        return;
    }
    if !config.cluster_enabled {
        return;
    }
    let Some(master_id) = cluster_state.my_master_id() else {
        return;
    };
    let master_addr = cluster_state
        .nodes
        .read()
        .get(&master_id)
        .map(|m| (m.ip.clone(), m.client_port));
    match master_addr {
        Some((ip, port)) if !ip.is_empty() && port != 0 => {
            log_info!(
                "Cluster: Restored replica role of master {} → restarting replication client",
                cluster::node::node_id_str(&master_id)
            );
            starter.start(ip, port);
        }
        _ => {
            // Still a replica as far as the cluster is concerned: mark the
            // role so this node stays read-only, and let gossip supply the
            // master's address (see `cluster::bus` — it re-points us once the
            // master is known).
            log_info!(
                "Cluster: Recorded master {} has no known address yet — waiting for gossip \
                 before replicating",
                cluster::node::node_id_str(&master_id)
            );
            starter.repl_state.become_slave();
        }
    }
}

fn run_tokio(config: Arc<Config>, started_at: Instant) -> Result<(), Box<dyn std::error::Error>> {
    let threads = config.resolve_threads();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?
        .block_on(async_main_tokio(config, started_at))
}

/// What to tell the operator when the client port cannot be listened on.
fn cant_listen(addr: &SocketAddr, e: &std::io::Error) -> String {
    let hint = match e.kind() {
        std::io::ErrorKind::AddrInUse => {
            "Another process — often another KiviDB or Redis — is using the port. Stop it, or \
             start this server with another --port"
        }
        std::io::ErrorKind::PermissionDenied => {
            "Ports below 1024 need privileges; use a higher --port, or grant \
             CAP_NET_BIND_SERVICE"
        }
        std::io::ErrorKind::AddrNotAvailable => {
            "The bind address is not an address of this machine; check --bind"
        }
        _ => "Check --bind and --port",
    };
    format!("Can't listen on {addr}: {e}. {hint}")
}

/// Make sure nothing else is listening on `addr`, and keep other KiviDBs off
/// it for as long as the returned claim lives (the life of the process).
///
/// Two checks, because the io_uring workers bind with SO_REUSEPORT and the
/// kernel then lets any other socket with that option share the port:
///
/// * A probe socket *without* SO_REUSEPORT. The kernel refuses its bind if
///   any socket at all holds the port, whatever options that one set.
/// * A claim that outlives the probe: an abstract Unix socket named after the
///   address. It closes the gap between the probe and the workers' binds, in
///   which two servers started at once would both pass the probe. The kernel
///   releases it when the process ends, however it ends. Where it cannot be
///   made at all, the probe alone decides.
///
/// A server that has just been killed leaves its listening sockets behind for
/// a moment: io_uring holds them until the kernel has torn its ring down,
/// which happens after the process is gone. A restart straight after a crash
/// would meet them and be refused. So while the claim is ours — no living
/// KiviDB has the port — a probe that finds the port in use is tried again
/// for a few seconds before it is believed.
#[cfg(target_os = "linux")]
fn claim_port(addr: &SocketAddr) -> std::io::Result<Option<std::os::unix::net::UnixListener>> {
    use std::os::linux::net::SocketAddrExt;
    let name = format!("kividb-listen-{addr}");
    let claim = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())
        .and_then(|name| std::os::unix::net::UnixListener::bind_addr(&name));
    let claim = match claim {
        Ok(claim) => Some(claim),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => return Err(e),
        Err(_) => None,
    };
    let patience = if claim.is_some() {
        std::time::Duration::from_secs(3)
    } else {
        std::time::Duration::ZERO
    };
    let started = Instant::now();
    loop {
        let probe = socket2::Socket::new(
            socket2::Domain::for_address(*addr),
            socket2::Type::STREAM,
            None,
        )?;
        // As the workers' sockets: a port in TIME_WAIT after a restart is free.
        probe.set_reuse_address(true)?;
        match probe.bind(&(*addr).into()) {
            Ok(()) => return Ok(claim),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && started.elapsed() < patience => {
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(e) => return Err(e),
        }
    }
}

/// The client listener, or a fatal error that says what to do about it.
#[allow(dead_code)]
async fn bind_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr).await.map_err(|e| {
        let message = cant_listen(&addr, &e);
        std::io::Error::new(e.kind(), message)
    })
}

#[cfg(target_os = "linux")]
fn run_uring(config: Arc<Config>, started_at: Instant) -> Result<(), Box<dyn std::error::Error>> {
    let store: store::Store = store::new_store(config.databases);
    let expiry_store: store::ExpiryStore = store::new_expiry_store(config.databases);
    let index_store = vector::new_index_store();
    let lastsave = Arc::new(RwLock::new(0i64));
    let aof_enabled = config.aof;
    // Initialise the runtime toggle to match the startup flag.
    crate::aof::AOF_ENABLED.store(config.aof, std::sync::atomic::Ordering::Relaxed);
    let started_at = Arc::new(started_at);
    let threads = config.resolve_threads();
    let wait_table = Arc::new(crate::wait_table::WaitTable::new());
    let dict_store = vector::new_dict_store();
    vector::register_dict_store(&index_store, &dict_store);
    let ts_index = store::new_ts_index();
    let pubsub = crate::pubsub::PubSubHub::new();
    let shutdown = Arc::new(AtomicBool::new(false));
    let repl_state = Arc::new(ReplState::from_config(&config));
    let acl_store = acl::new_acl_store(&config.requirepass);
    let acl_log = acl::new_acl_log();
    let slowlog = crate::slowlog::new_slowlog_from_config(&config);
    let latency = crate::latency::new_latency_tracker();
    let monitor_registry = crate::monitor::new_monitor_registry();
    let tracking_table = crate::tracking::new_tracking_table();
    let cluster_state = std::sync::Arc::new(load_cluster_state(&config));

    // HNSW compaction worker — drains background rebuild requests.
    // Single OS thread; no tokio runtime needed.
    let (compaction_tx, compaction_rx) = vector::new_compaction_channel();
    {
        let index_store_for_compaction = Arc::clone(&index_store);
        let store_for_compaction = Arc::clone(&store);
        vector::spawn_compaction_worker(
            index_store_for_compaction,
            store_for_compaction,
            compaction_rx,
        );
    }

    if !config.aclfile.is_empty() {
        match acl::load_acl_file(&config.aclfile, &acl_store, &config.requirepass) {
            Ok(_) => log_info!("ACL file loaded: {}", config.aclfile),
            Err(e) => {
                log_fatal!(
                    "Can't load the ACL file '{}': {e}. Fix the line named, or start \
                     without aclfile",
                    config.aclfile
                );
                std::process::exit(1);
            }
        }
    }
    // start_hub needs a tokio runtime — give it its own single-thread runtime
    // just like the AOF writer, so it never competes with the io_uring workers.
    let hub_tx = {
        let repl_state_hub = Arc::clone(&repl_state);
        let max_backlog_bytes = config.repl_replica_max_backlog_bytes;
        let (tx, rx) = std::sync::mpsc::sync_channel::<crate::replication::HubTx>(1);
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let hub_tx = start_hub(repl_state_hub, max_backlog_bytes);
                    tx.send(hub_tx).unwrap();
                    // Keep this runtime alive forever — the hub task lives here
                    std::future::pending::<()>().await;
                });
        });
        rx.recv().unwrap()
    };
    crate::stats::spawn_instantaneous_ops_sampler();
    crate::commands::helpers::spawn_active_expire_task(
        Arc::clone(&store),
        Arc::clone(&expiry_store),
        Arc::clone(&index_store),
        pubsub.clone(),
    );
    if config.cluster_enabled {
        log_info!(
            "Cluster: Node ID        : {}",
            crate::cluster::node_id_str(&cluster_state.myself())
        );
        log_info!("Cluster: Bus port       : {}", config.port + 10000);
    }
    let watch_registry = crate::watch::new_watch_registry();
    crate::watch::register_for(&store, &watch_registry);
    let client_registry = crate::client_registry::new_client_registry();
    crate::tracking::register_for(&store, &tracking_table, &client_registry);

    log_info!("Shards per DB  : {}", store::num_shards());
    log_info!("I/O model      : io_uring");

    // Local recovery runs on replicas too. A full resync loads into a staging
    // store and swaps it in, so there is nothing to merge with; and a replica
    // that starts empty has no way to notice that the "master" it was pointed
    // at is a blank node about to overwrite the only copy of the data — which
    // is exactly what the full-sync guard needs to see.
    {
        let loaded = crate::aof::recover_local(
            config.aof,
            &store,
            &expiry_store,
            &index_store,
            &dict_store,
            &wait_table,
            &ts_index,
            &config,
            &compaction_tx,
            &repl_state,
        );
        // A node that was a replica when it was last saved, and has loaded
        // exactly that save, can ask its master to continue from there.
        if loaded.exact_dump
            && let Some(point) = crate::replication::load_saved_resync_point()
        {
            log_info!(
                "Replication: Loaded dataset is offset {} of {} — a partial resync will be asked for",
                point.offset,
                point.repl_id
            );
            repl_state
                .repl_offset
                .store(point.offset, std::sync::atomic::Ordering::Relaxed);
            repl_state.set_resync_point(Some(point));
        } else if loaded.exact_dump
            && config.replicaof_addr().is_none()
            && cluster_state.my_master_id().is_none()
            && let Some((repl_id, offset)) = crate::replication::load_saved_master_point()
        {
            // And a master stopped cleanly carries on its stream, so that
            // its replicas need not be sent everything again.
            log_info!(
                "Replication: Loaded dataset is offset {offset} of this node's stream {repl_id} \
                 — replicas stopped cleanly with it can be continued"
            );
            repl_state.resume_as_master(repl_id, offset, &hub_tx);
        } else if config.replicaof_addr().is_none()
            && cluster_state.my_master_id().is_none()
            && store
                .iter()
                .any(|db| db.iter().any(|shard| !crate::read_lock!(shard).is_empty()))
        {
            repl_state.fingerprint_at_start(&store, &expiry_store, &hub_tx);
        }
        if !loaded.index_names.is_empty() {
            vector::SNAPSHOT_INDEX_REBUILD.store(true, std::sync::atomic::Ordering::Release);
            if loaded.hnsw_loaded {
                log_info!(
                    "HNSW graph(s) loaded from snapshot — running text/numeric index pass in background"
                );
            } else {
                log_info!(
                    "Rebuilding {} search index(es) in the background (most commands return LOADING until rebuild finishes)",
                    loaded.index_names.len()
                );
            }
            spawn_snapshot_index_backfill_thread(
                Arc::clone(&store),
                Arc::clone(&index_store),
                loaded.index_names,
            );
        }
    }

    // Spawn a dedicated thread to watch for SIGTERM / SIGINT.
    // On signal: save snapshot and exit immediately.
    // We don't try to gracefully drain worker threads — io_uring accept()
    // cannot be interrupted without kernel-level mechanisms, and adding
    // timeouts to accept hurts throughput. Instead we just save and exit.
    // NEW — handles both OS signals AND command-issued SHUTDOWN
    let shutdown_signal = Arc::clone(&shutdown);
    let signal_snapshot_store = Arc::clone(&store);
    let signal_snapshot_expiry_store = Arc::clone(&expiry_store);
    let signal_snapshot_index_store = Arc::clone(&index_store);
    let signal_repl_state = Arc::clone(&repl_state);
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                // Wait for OS signal OR SHUTDOWN command (polled every 100ms).
                // The poll arm is the only addition — OS signal behaviour is
                // identical to before.
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{SignalKind, signal};
                    let mut sigterm = signal(SignalKind::terminate()).unwrap();
                    loop {
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => {
                                log_info!("SIGINT received — shutting down gracefully");
                                break;
                            }
                            _ = sigterm.recv() => {
                                log_info!("SIGTERM received — shutting down gracefully");
                                break;
                            }
                            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                                // SHUTDOWN_PENDING is set by the command handler.
                                // One atomic load per 100ms — negligible cost.
                                if crate::shutdown::SHUTDOWN_PENDING.load(Ordering::Relaxed) {
                                    log_info!("SHUTDOWN command received — shutting down");
                                    break;
                                }
                            }
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    loop {
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => {
                                log_info!("Ctrl+C received — shutting down gracefully");
                                break;
                            }
                            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                                if crate::shutdown::SHUTDOWN_PENDING.load(Ordering::Relaxed) {
                                    log_info!("SHUTDOWN command received — shutting down");
                                    break;
                                }
                            }
                        }
                    }
                }

                // Signal the io_uring accept workers to stop their loop.
                shutdown_signal.store(true, Ordering::SeqCst);

                // Read the mode the SHUTDOWN command (or OS signal) requested.
                // OS signals leave SHUTDOWN_MODE at its default (SAVE), which is correct.
                let m = crate::shutdown::SHUTDOWN_MODE.load(Ordering::Relaxed);

                if m != crate::shutdown::mode::NOSAVE {
                    log_info!("Writing final snapshot...");
                    // With the replica's position in its master's stream
                    // beside it, so that the restart can ask to continue.
                    let result = crate::replication::save_dataset(
                        &signal_snapshot_store,
                        &signal_snapshot_expiry_store,
                        &signal_snapshot_index_store,
                        &signal_repl_state,
                    );
                    if let Err(e) = result {
                        // Not a clean exit: whatever restarts this node
                        // should know its last writes are not on disk.
                        log_error!(
                            "Final snapshot failed: {e}. Writes since the last successful save \
                             are not in dump.kdb (they are in the AOF, if appendonly is on). \
                             Check free disk space and write permission on the data directory"
                        );
                        std::process::exit(1);
                    }
                    log_info!("Goodbye.");
                } else {
                    log_info!("SHUTDOWN NOSAVE — skipping snapshot. Goodbye.");
                }

                std::process::exit(0);
            });
    });

    // The AOF writer has a thread and a runtime of its own (it is just file
    // I/O), and the io_uring workers queue records for it directly: sending
    // on a tokio channel needs no runtime on the sending side.
    //
    // There used to be a std channel in between, and a thread that did
    // nothing but move records from the one to the other. A record that
    // arrived alone — which is how a script's arrives — woke that thread,
    // which woke the writer; and a std receiver spins and yields before it
    // sleeps, which on a one-CPU machine is the worker's own time. Under
    // BullMQ on one CPU it was about a sixth of every script call, and more
    // than half of the server's system time.
    let (aof_tx, tokio_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    // Reachable from the replication applier, which is built too deep to be
    // handed this channel directly. See `aof::install_global_aof_sender`.
    aof::install_global_aof_sender(Box::new(aof_tx.clone()));
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(aof::start_aof_writer(tokio_rx))
    });

    if aof_enabled {
        aof::spawn_auto_rewrite_thread(
            Arc::clone(&store),
            Arc::clone(&expiry_store),
            Arc::clone(&index_store),
            config.aof_rewrite_percentage,
            config.aof_rewrite_min_size,
        );
    }

    // Replica client — runs on its own thread and runtime (see
    // `ReplicaStarter::start`), so it never competes with the io_uring workers.
    let replica_starter = crate::replication::replica_client::ReplicaStarter {
        store: Arc::clone(&store),
        expiry_store: Arc::clone(&expiry_store),
        index_store: Arc::clone(&index_store),
        dict_store: Arc::clone(&dict_store),
        wait_table: Arc::clone(&wait_table),
        ts_index: Arc::clone(&ts_index),
        config: Arc::clone(&config),
        compaction_tx: compaction_tx.clone(),
        repl_state: Arc::clone(&repl_state),
        hub_tx: hub_tx.clone(),
        pubsub: pubsub.clone(),
        client_registry: Arc::clone(&client_registry),
        tracking_table: Arc::clone(&tracking_table),
        monitor_registry: Arc::clone(&monitor_registry),
    };
    // The cluster bus starts here, once everything it may need to act on —
    // the stores, the replication state, the pub/sub hub — exists.
    cluster_state.start(
        &config,
        cluster::ClusterHooks {
            replica_starter: replica_starter.clone(),
            pubsub: pubsub.clone(),
        },
    );
    start_configured_replication(&config, &cluster_state, &replica_starter);

    let addr: SocketAddr = config.addr().parse::<SocketAddr>()?;

    // The workers share the port with SO_REUSEPORT, and so could a second
    // KiviDB started on it by mistake: it would come up "ready", and the
    // kernel would hand each of the two a share of the clients. Claim the
    // port first, and refuse to start if it is taken — as Redis would.
    let _port_claim = match claim_port(&addr) {
        Ok(claim) => claim,
        Err(e) => {
            log_fatal!("{}", cant_listen(&addr, &e));
            std::process::exit(1);
        }
    };

    // Spawn one OS thread per worker. Each thread runs its own
    // tokio_uring runtime — single-threaded, no cross-thread task stealing.
    // The kernel hands each new connection to one of the listeners by a hash
    // of its addresses (SO_REUSEPORT), which is even only over very many:
    // 150 connections on 32 workers left some workers with three times what
    // others had, and those were the limit while the others idled. So a
    // worker that accepts a connection it has too many for passes it to the
    // worker with the fewest: `loads` is how many each has, and `inboxes`
    // is where each is passed them.
    let loads: Arc<Vec<std::sync::atomic::AtomicUsize>> = Arc::new(
        (0..threads)
            .map(|_| std::sync::atomic::AtomicUsize::new(0))
            .collect(),
    );
    type Passed = (std::net::TcpStream, String);
    let (inboxes, mut inbox_ends): (Vec<_>, Vec<_>) = (0..threads)
        .map(|_| {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Passed>();
            (tx, Some(rx))
        })
        .unzip();
    let inboxes = Arc::new(inboxes);
    let handles: Vec<_> = (0..threads)
        .map(|worker| {
            let loads = Arc::clone(&loads);
            let inboxes = Arc::clone(&inboxes);
            let mut inbox = inbox_ends[worker].take().expect("one inbox a worker");
            let shutdown = Arc::clone(&shutdown);
            let store = Arc::clone(&store);
            let expiry_store = Arc::clone(&expiry_store);
            let lastsave = Arc::clone(&lastsave);
            let aof_tx = aof_tx.clone();
            let config = Arc::clone(&config);
            let started_at = Arc::clone(&started_at);
            let index_store = Arc::clone(&index_store);
            let wait_table = Arc::clone(&wait_table);
            let dict_store = Arc::clone(&dict_store);
            let ts_index = Arc::clone(&ts_index);
            let pubsub = pubsub.clone();
            let hub_tx = hub_tx.clone();
            let repl_state = Arc::clone(&repl_state);
            let watch_registry = Arc::clone(&watch_registry);
            let client_registry = Arc::clone(&client_registry);
            let acl_store = Arc::clone(&acl_store);
            let acl_log = Arc::clone(&acl_log);
            let slowlog = Arc::clone(&slowlog);
            let latency = Arc::clone(&latency);
            let compaction_tx = compaction_tx.clone();
            let cluster_state = Arc::clone(&cluster_state);
            let monitor_registry = Arc::clone(&monitor_registry);
            let tracking_table = Arc::clone(&tracking_table);
            std::thread::spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tokio_uring::start(async move {
                        let socket =
                            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)
                                .unwrap();
                        socket.set_reuse_address(true).unwrap();
                        socket.set_reuse_port(true).unwrap();
                        socket.set_nonblocking(true).unwrap();
                        if let Err(e) = socket.bind(&addr.into()).and_then(|()| socket.listen(1024)) {
                            log_fatal!("{}", cant_listen(&addr, &e));
                            std::process::exit(1);
                        }

                        let listener = tokio_uring::net::TcpListener::from_std(socket.into());
                        // `CONFIG SET port` moves it.
                        let mut listener =
                            crate::listen::UringListener::new(listener, addr.port(), addr);
                        static READY: std::sync::Once = std::sync::Once::new();
                        READY.call_once(|| log_info!("Ready to accept connections on {addr}"));

                        /// One connection in a worker's count, until it is dropped.
                        struct Counted(Arc<Vec<std::sync::atomic::AtomicUsize>>, usize);
                        impl Drop for Counted {
                            fn drop(&mut self) {
                                self.0[self.1].fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                        let loads_of_clients = Arc::clone(&loads);
                        // Starts a client on this worker. Its place in the
                        // worker's count was taken by whoever chose the worker.
                        let start_client = std::rc::Rc::new(
                            move |stream: tokio_uring::net::TcpStream, peer_addr_str: String| {
                            let store = Arc::clone(&store);
                            let expiry_store = Arc::clone(&expiry_store);
                            let lastsave = Arc::clone(&lastsave);
                            let aof_tx = aof_tx.clone();
                            let config = Arc::clone(&config);
                            let started_at = Arc::clone(&started_at);
                            let index_store = Arc::clone(&index_store);
                            let wait_table = Arc::clone(&wait_table);
                            let dict_store = Arc::clone(&dict_store);
                            let ts_index = Arc::clone(&ts_index);
                            let pubsub = pubsub.clone();
                            let hub_tx = hub_tx.clone();
                            let repl_state = Arc::clone(&repl_state);
                            let watch_registry = Arc::clone(&watch_registry);
                            let client_registry = Arc::clone(&client_registry);
                            let acl_store = Arc::clone(&acl_store);
                            let acl_log = Arc::clone(&acl_log);
                            let slowlog = Arc::clone(&slowlog);
                            let latency = Arc::clone(&latency);
                            let compaction_tx = compaction_tx.clone();
                            let cluster_state = Arc::clone(&cluster_state);
                            let monitor_registry = Arc::clone(&monitor_registry);
                            let tracking_table = Arc::clone(&tracking_table);

                            let counted = Counted(Arc::clone(&loads_of_clients), worker);
                            tokio_uring::spawn(async move {
                                let _counted = counted;
                                if let Err(e) = server_uring::handle_client(
                                    stream,
                                    store,
                                    expiry_store,
                                    lastsave,
                                    aof_tx,
                                    aof_enabled,
                                    config,
                                    started_at,
                                    index_store,
                                    dict_store,
                                    wait_table,
                                    ts_index,
                                    pubsub,
                                    hub_tx,
                                    repl_state,
                                    watch_registry,
                                    client_registry,
                                    peer_addr_str,
                                    acl_store,
                                    acl_log,
                                    slowlog,
                                    latency,
                                    compaction_tx,
                                    cluster_state,
                                    monitor_registry,
                                    tracking_table
                                )
                                .await
                                {
                                    let s = e.to_string();
                                    if !s.contains("unexpected end of file")
                                        && !s.contains("Connection reset")
                                    {
                                        log_verbose!("Connection error: {e}");
                                    }
                                }
                            });
                            },
                        );
                        {
                            // What other workers pass to this one.
                            let start_client = std::rc::Rc::clone(&start_client);
                            tokio_uring::spawn(async move {
                                while let Some((stream, peer_addr_str)) = inbox.recv().await {
                                    start_client(
                                        tokio_uring::net::TcpStream::from_std(stream),
                                        peer_addr_str,
                                    );
                                }
                            });
                        }

                        loop {
                            if shutdown.load(Ordering::Relaxed) {
                                return;
                            }

                            let (stream, peer_addr) = match listener.accept().await {
                                Ok(v) => v,
                                Err(e) => {
                                    log_throttled!(10, crate::loglevel::WARN, "Accept error: {e} — if this is \"Too many open files\", raise the open-file limit (ulimit -n, or LimitNOFILE= under systemd)");
                                    continue;
                                }
                            };
                            crate::stats::TOTAL_CONNECTIONS_RECEIVED
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            if shutdown.load(Ordering::Relaxed) {
                                return;
                            }

                            let mut peer_addr_str = peer_addr.to_string();
                            let mine = loads[worker].fetch_add(1, Ordering::Relaxed) + 1;
                            let (least, fewest) = loads
                                .iter()
                                .map(|load| load.load(Ordering::Relaxed))
                                .enumerate()
                                .min_by_key(|(_, load)| *load)
                                .unwrap_or((worker, mine));
                            if least != worker && fewest + 1 < mine {
                                use std::os::fd::{AsRawFd, FromRawFd};
                                // The other worker is given a second handle
                                // on the socket, and this one's is closed.
                                // SAFETY: the stream's descriptor is open, and
                                // the copy is owned by nothing else.
                                let copy = unsafe { libc::dup(stream.as_raw_fd()) };
                                if copy >= 0 {
                                    let passed = unsafe { std::net::TcpStream::from_raw_fd(copy) };
                                    loads[worker].fetch_sub(1, Ordering::Relaxed);
                                    loads[least].fetch_add(1, Ordering::Relaxed);
                                    match inboxes[least].send((passed, peer_addr_str)) {
                                        Ok(()) => continue,
                                        Err(tokio::sync::mpsc::error::SendError((_, peer))) => {
                                            // That worker has gone: it stays here.
                                            loads[least].fetch_sub(1, Ordering::Relaxed);
                                            loads[worker].fetch_add(1, Ordering::Relaxed);
                                            peer_addr_str = peer;
                                        }
                                    }
                                }
                            }
                            start_client(stream, peer_addr_str);
                        }
                    });
                }));
                if result.is_err() {
                    log_warn!(
                        "An io_uring worker failed to start: the kernel or a seccomp profile \
                         (Docker's default one, for example) may not allow io_uring. Start with \
                         --io-model tokio, or allow io_uring"
                    );
                }
            })
        })
        .collect();

    // TLS uses Tokio's `TlsStream` + `server::handle_client` — same as the pure Tokio binary path.
    // io_uring stays on the cleartext port; TLS is accepted on `tls_port` in parallel.
    #[cfg(feature = "tls")]
    if config.tls_enabled() {
        use crate::tls::acceptor;
        use tokio::net::TcpListener;

        let tls_acceptor = match acceptor::build(
            &config.tls_cert_file,
            &config.tls_key_file,
            &config.tls_ca_cert_file,
            &config.tls_auth_clients,
        ) {
            Ok(a) => a,
            Err(e) => {
                log_fatal!("TLS: {e}");
                std::process::exit(1);
            }
        };

        let tls_config = Arc::clone(&config);
        let tls_store = Arc::clone(&store);
        let tls_expiry_store = expiry_store.clone();
        let tls_lastsave = Arc::clone(&lastsave);
        // server::handle_client expects a tokio Sender; bridge from the std SyncSender once here.
        let (tls_aof_tx, mut tls_aof_bridge_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        {
            let std_tx = aof_tx.clone();
            // io_uring `run_uring` has no Tokio runtime on the main thread — cannot use `tokio::spawn` here.
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        while let Some(msg) = tls_aof_bridge_rx.recv().await {
                            // Same contract as AofSender: a record is
                            // never dropped silently. The only failure
                            // here is a gone consumer (shutdown).
                            if std_tx.send(msg).is_err() {
                                crate::aof::record_lost_aof_record();
                                break;
                            }
                        }
                    });
            });
        }
        let tls_started_at = Arc::clone(&started_at);
        let tls_index_store = Arc::clone(&index_store);
        let tls_wait_table = Arc::clone(&wait_table);
        let tls_dict_store = dict_store.clone();
        let tls_ts_index = ts_index.clone();
        let tls_pubsub = pubsub.clone();
        let tls_hub_tx = hub_tx.clone();
        let tls_repl_state = Arc::clone(&repl_state);
        let tls_watch_registry = Arc::clone(&watch_registry);
        let tls_client_registry = Arc::clone(&client_registry);
        let tls_acl_store = Arc::clone(&acl_store);
        let tls_acl_log = Arc::clone(&acl_log);
        let tls_slowlog = Arc::clone(&slowlog);
        let tls_latency = Arc::clone(&latency);
        let tls_compaction_tx = compaction_tx.clone();
        let tls_cluster_state = Arc::clone(&cluster_state);
        let tls_monitor_registry = Arc::clone(&monitor_registry);
        let tls_tracking_table = Arc::clone(&tracking_table);
        let tls_aof_enabled = aof_enabled;

        let listen_acceptor = tls_acceptor.clone();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let tls_addr: SocketAddr =
                        format!("{}:{}", tls_config.bind, tls_config.tls_port)
                            .parse()
                            .expect("invalid TLS bind address");
                    let tls_listener = match TcpListener::bind(tls_addr).await {
                        Ok(l) => l,
                        Err(e) => {
                            log_warn!("TLS: bind failed on io_uring binary path: {e}");
                            return;
                        }
                    };

                    log_info!(
                        "TLS listener   : {}:{}",
                        tls_config.bind,
                        tls_config.tls_port
                    );

                    loop {
                        let (tcp_stream, peer_addr) = match tls_listener.accept().await {
                            Ok(pair) => pair,
                            Err(e) => {
                                log_warn!("TLS: accept error: {e}");
                                continue;
                            }
                        };

                        let tls_stream = match listen_acceptor.clone().accept(tcp_stream).await {
                            Ok(s) => s,
                            Err(e) => {
                                // Full `Debug` — rustls errors often have empty `Display`; memtier may log "(null)" for the same failure.
                                log_verbose!("TLS: handshake failed from {peer_addr}: {e} ({e:?})");
                                continue;
                            }
                        };

                        crate::stats::TOTAL_CONNECTIONS_RECEIVED
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        let peer_addr_str = peer_addr.to_string();

                        let store = Arc::clone(&tls_store);
                        let expiry_store = tls_expiry_store.clone();
                        let lastsave = Arc::clone(&tls_lastsave);
                        let aof_tx = tls_aof_tx.clone();
                        let config = Arc::clone(&tls_config);
                        let started_at = Arc::clone(&tls_started_at);
                        let index_store = Arc::clone(&tls_index_store);
                        let wait_table = Arc::clone(&tls_wait_table);
                        let dict_store = tls_dict_store.clone();
                        let ts_index = tls_ts_index.clone();
                        let pubsub = tls_pubsub.clone();
                        let hub_tx = tls_hub_tx.clone();
                        let repl_state = Arc::clone(&tls_repl_state);
                        let watch_registry = Arc::clone(&tls_watch_registry);
                        let client_registry = Arc::clone(&tls_client_registry);
                        let acl_store = Arc::clone(&tls_acl_store);
                        let acl_log = Arc::clone(&tls_acl_log);
                        let slowlog = Arc::clone(&tls_slowlog);
                        let latency = Arc::clone(&tls_latency);
                        let compaction_tx = tls_compaction_tx.clone();
                        let cluster_state = Arc::clone(&tls_cluster_state);
                        let monitor_registry = Arc::clone(&tls_monitor_registry);
                        let tracking_table = Arc::clone(&tls_tracking_table);

                        tokio::spawn(async move {
                            if let Err(e) = server::handle_client(
                                tls_stream,
                                store,
                                expiry_store,
                                lastsave,
                                aof_tx,
                                tls_aof_enabled,
                                config,
                                started_at,
                                index_store,
                                dict_store,
                                wait_table,
                                ts_index,
                                pubsub,
                                hub_tx,
                                repl_state,
                                watch_registry,
                                client_registry,
                                peer_addr_str,
                                acl_store,
                                acl_log,
                                slowlog,
                                latency,
                                compaction_tx,
                                cluster_state,
                                monitor_registry,
                                tracking_table,
                            )
                            .await
                            {
                                let s = e.to_string();
                                if !s.contains("unexpected end of file")
                                    && !s.contains("Connection reset")
                                {
                                    log_verbose!("Connection error: {e}");
                                }
                            }
                        });
                    }
                });
        });
    }

    for h in handles {
        let _ = h.join();
    }
    drop(aof_tx);
    // The accept workers have stopped because the server is shutting down.
    // The final snapshot is written by the signal thread, which then ends the
    // process — and this thread must wait for it. It used to return here
    // instead: with connections arriving, every worker saw the shutdown flag
    // within moments, `main` returned, and the process exited with status 0
    // while the snapshot was still being written — or before it had begun.
    // An idle server did not show it, because a worker with nothing to
    // accept never looks at the flag.
    if shutdown.load(Ordering::SeqCst) {
        loop {
            std::thread::park();
        }
    }
    Ok(())
}

async fn async_main_tokio(
    config: Arc<Config>,
    started_at: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    log_info!("Shards per DB  : {}", store::num_shards());
    log_info!("Bind address   : {}", config.addr());

    let store: store::Store = store::new_store(config.databases);
    let expiry_store: store::ExpiryStore = store::new_expiry_store(config.databases);
    let lastsave = Arc::new(RwLock::new(0i64));
    let aof_enabled = config.aof;
    // Initialise the runtime toggle to match the startup flag.
    crate::aof::AOF_ENABLED.store(config.aof, std::sync::atomic::Ordering::Relaxed);
    let started_at = Arc::new(started_at);
    let index_store = vector::new_index_store();
    let wait_table = Arc::new(crate::wait_table::WaitTable::new());
    let dict_store = vector::new_dict_store();
    vector::register_dict_store(&index_store, &dict_store);
    let ts_index = store::new_ts_index();
    let pubsub = crate::pubsub::PubSubHub::new();
    // Unbounded: a bounded AOF queue drops durability records under load.
    let (aof_tx, aof_rx) = mpsc::unbounded_channel();
    // Reachable from the replication applier, which is built too deep to be
    // handed this channel directly. See `aof::install_global_aof_sender`.
    aof::install_global_aof_sender(Box::new(aof_tx.clone()));
    let repl_state = Arc::new(ReplState::from_config(&config));
    let hub_tx = start_hub(
        Arc::clone(&repl_state),
        config.repl_replica_max_backlog_bytes,
    );
    crate::stats::spawn_instantaneous_ops_sampler();
    crate::commands::helpers::spawn_active_expire_task(
        Arc::clone(&store),
        Arc::clone(&expiry_store),
        Arc::clone(&index_store),
        pubsub.clone(),
    );
    let watch_registry = crate::watch::new_watch_registry();
    crate::watch::register_for(&store, &watch_registry);
    let client_registry = crate::client_registry::new_client_registry();
    let acl_store = acl::new_acl_store(&config.requirepass);
    let acl_log = acl::new_acl_log();
    let slowlog = crate::slowlog::new_slowlog_from_config(&config);
    let latency = crate::latency::new_latency_tracker();
    let monitor_registry = crate::monitor::new_monitor_registry();
    let tracking_table = crate::tracking::new_tracking_table();
    crate::tracking::register_for(&store, &tracking_table, &client_registry);
    let cluster_state = std::sync::Arc::new(load_cluster_state(&config));

    // HNSW compaction worker — must live before replication (replica applier sends here).
    let (compaction_tx, compaction_rx) = vector::new_compaction_channel();
    {
        let index_store_for_compaction = Arc::clone(&index_store);
        let store_for_compaction = Arc::clone(&store);
        vector::spawn_compaction_worker(
            index_store_for_compaction,
            store_for_compaction,
            compaction_rx,
        );
    }

    if config.cluster_enabled {
        log_info!(
            "Cluster: Node ID        : {}",
            crate::cluster::node_id_str(&cluster_state.myself())
        );
        log_info!("Cluster: Bus port       : {}", config.port + 10000);
    }

    if !config.aclfile.is_empty() {
        match acl::load_acl_file(&config.aclfile, &acl_store, &config.requirepass) {
            Ok(_) => log_info!("ACL file loaded: {}", config.aclfile),
            Err(e) => {
                log_fatal!(
                    "Can't load the ACL file '{}': {e}. Fix the line named, or start \
                     without aclfile",
                    config.aclfile
                );
                std::process::exit(1);
            }
        }
    }

    let addr: SocketAddr = config.addr().parse()?;
    let mut listener_early: Option<TcpListener> = None;

    // Try native KDB format first, fall back to Redis RDB. Replicas load
    // too — see the note at the same point in `run_uring`.
    {
        let loaded = crate::aof::recover_local(
            config.aof,
            &store,
            &expiry_store,
            &index_store,
            &dict_store,
            &wait_table,
            &ts_index,
            &config,
            &compaction_tx,
            &repl_state,
        );
        // A node that was a replica when it was last saved, and has loaded
        // exactly that save, can ask its master to continue from there.
        if loaded.exact_dump
            && let Some(point) = crate::replication::load_saved_resync_point()
        {
            log_info!(
                "Replication: Loaded dataset is offset {} of {} — a partial resync will be asked for",
                point.offset,
                point.repl_id
            );
            repl_state
                .repl_offset
                .store(point.offset, std::sync::atomic::Ordering::Relaxed);
            repl_state.set_resync_point(Some(point));
        } else if loaded.exact_dump
            && config.replicaof_addr().is_none()
            && cluster_state.my_master_id().is_none()
            && let Some((repl_id, offset)) = crate::replication::load_saved_master_point()
        {
            // And a master stopped cleanly carries on its stream, so that
            // its replicas need not be sent everything again.
            log_info!(
                "Replication: Loaded dataset is offset {offset} of this node's stream {repl_id} \
                 — replicas stopped cleanly with it can be continued"
            );
            repl_state.resume_as_master(repl_id, offset, &hub_tx);
        } else if config.replicaof_addr().is_none()
            && cluster_state.my_master_id().is_none()
            && store
                .iter()
                .any(|db| db.iter().any(|shard| !crate::read_lock!(shard).is_empty()))
        {
            repl_state.fingerprint_at_start(&store, &expiry_store, &hub_tx);
        }
        if !loaded.index_names.is_empty() {
            vector::SNAPSHOT_INDEX_REBUILD.store(true, std::sync::atomic::Ordering::Release);
            listener_early = Some(bind_listener(addr).await?);
            if loaded.hnsw_loaded {
                log_info!(
                    "Listening on {addr} — HNSW graph(s) loaded from snapshot; running text/numeric index pass in background"
                );
            } else {
                log_info!(
                    "Listening on {addr} — rebuilding {} search index(es) in the background (most commands return LOADING until rebuild finishes; see INFO logs for progress)",
                    loaded.index_names.len()
                );
            }
            spawn_snapshot_index_backfill_thread(
                Arc::clone(&store),
                Arc::clone(&index_store),
                loaded.index_names,
            );
        }
    }
    // The AOF writer gets a dedicated single-thread runtime rather than a task on
    // the shared one. `rewrite_aof` quiesces by holding every shard lock and then
    // waits for the writer to perform the swap; if the writer were a task here,
    // enough connection tasks blocking synchronously on those same locks could
    // occupy every worker thread and the writer would never be polled -- the
    // server would hang holding all locks. A dedicated runtime makes that
    // impossible. (The io_uring path already does this for its own reasons.)
    if aof_enabled {
        aof::spawn_auto_rewrite_thread(
            Arc::clone(&store),
            Arc::clone(&expiry_store),
            Arc::clone(&index_store),
            config.aof_rewrite_percentage,
            config.aof_rewrite_min_size,
        );
    }
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(aof::start_aof_writer(aof_rx))
    });

    // Replica client — runs on its own thread and runtime (see
    // `ReplicaStarter::start`).
    let replica_starter = crate::replication::replica_client::ReplicaStarter {
        store: Arc::clone(&store),
        expiry_store: Arc::clone(&expiry_store),
        index_store: Arc::clone(&index_store),
        dict_store: Arc::clone(&dict_store),
        wait_table: Arc::clone(&wait_table),
        ts_index: Arc::clone(&ts_index),
        config: Arc::clone(&config),
        compaction_tx: compaction_tx.clone(),
        repl_state: Arc::clone(&repl_state),
        hub_tx: hub_tx.clone(),
        pubsub: pubsub.clone(),
        client_registry: Arc::clone(&client_registry),
        tracking_table: Arc::clone(&tracking_table),
        monitor_registry: Arc::clone(&monitor_registry),
    };
    // The cluster bus starts here, once everything it may need to act on —
    // the stores, the replication state, the pub/sub hub — exists.
    cluster_state.start(
        &config,
        cluster::ClusterHooks {
            replica_starter: replica_starter.clone(),
            pubsub: pubsub.clone(),
        },
    );
    start_configured_replication(&config, &cluster_state, &replica_starter);

    // ONE listener — tokio's multi-thread runtime handles distribution
    let listener = match listener_early {
        Some(l) => l,
        None => bind_listener(addr).await?,
    };
    // `CONFIG SET port` moves it.
    let mut listener = crate::listen::Listener::new(listener, addr.port());
    log_info!("Ready to accept connections on {addr}");

    #[cfg(feature = "tls")]
    if config.tls_enabled() {
        use crate::tls::acceptor;

        let tls_acceptor = match acceptor::build(
            &config.tls_cert_file,
            &config.tls_key_file,
            &config.tls_ca_cert_file,
            &config.tls_auth_clients,
        ) {
            Ok(a) => a,
            Err(e) => {
                log_fatal!("TLS: {e}");
                std::process::exit(1);
            }
        };

        let tls_addr: SocketAddr = format!("{}:{}", config.bind, config.tls_port)
            .parse()
            .expect("invalid TLS bind address");
        let tls_listener = TcpListener::bind(tls_addr).await?;

        log_info!("TLS listener   : {}:{}", config.bind, config.tls_port);

        let tls_config = Arc::clone(&config);
        let tls_store = Arc::clone(&store);
        let tls_expiry_store = expiry_store.clone();
        let tls_lastsave = Arc::clone(&lastsave);
        // server::handle_client expects a tokio Sender; bridge from the std SyncSender once here.
        let (tls_aof_tx, mut tls_aof_bridge_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        {
            let std_tx = aof_tx.clone();
            tokio::spawn(async move {
                while let Some(msg) = tls_aof_bridge_rx.recv().await {
                    // Same contract as AofSender: never drop silently.
                    if std_tx.send(msg).is_err() {
                        crate::aof::record_lost_aof_record();
                        break;
                    }
                }
            });
        }
        let tls_started_at = Arc::clone(&started_at);
        let tls_index_store = Arc::clone(&index_store);
        let tls_wait_table = Arc::clone(&wait_table);
        let tls_dict_store = dict_store.clone();
        let tls_ts_index = ts_index.clone();
        let tls_pubsub = pubsub.clone();
        let tls_hub_tx = hub_tx.clone();
        let tls_repl_state = Arc::clone(&repl_state);
        let tls_watch_registry = Arc::clone(&watch_registry);
        let tls_client_registry = Arc::clone(&client_registry);
        let tls_acl_store = Arc::clone(&acl_store);
        let tls_acl_log = Arc::clone(&acl_log);
        let tls_slowlog = Arc::clone(&slowlog);
        let tls_latency = Arc::clone(&latency);
        let tls_compaction_tx = compaction_tx.clone();
        let tls_cluster_state = Arc::clone(&cluster_state);
        let tls_monitor_registry = Arc::clone(&monitor_registry);
        let tls_tracking_table = Arc::clone(&tracking_table);

        let listen_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            loop {
                let (tcp_stream, peer_addr) = match tls_listener.accept().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        log_warn!("TLS: accept error: {e}");
                        continue;
                    }
                };

                let tls_stream = match listen_acceptor.accept(tcp_stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        log_verbose!("TLS: handshake failed from {peer_addr}: {e} ({e:?})");
                        continue;
                    }
                };

                crate::stats::TOTAL_CONNECTIONS_RECEIVED
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                let peer_addr_str = peer_addr.to_string();

                let store = Arc::clone(&tls_store);
                let expiry_store = tls_expiry_store.clone();
                let lastsave = Arc::clone(&tls_lastsave);
                let aof_tx = tls_aof_tx.clone();
                let config = Arc::clone(&tls_config);
                let started_at = Arc::clone(&tls_started_at);
                let index_store = Arc::clone(&tls_index_store);
                let wait_table = Arc::clone(&tls_wait_table);
                let dict_store = tls_dict_store.clone();
                let ts_index = tls_ts_index.clone();
                let pubsub = tls_pubsub.clone();
                let hub_tx = tls_hub_tx.clone();
                let repl_state = Arc::clone(&tls_repl_state);
                let watch_registry = Arc::clone(&tls_watch_registry);
                let client_registry = Arc::clone(&tls_client_registry);
                let acl_store = Arc::clone(&tls_acl_store);
                let acl_log = Arc::clone(&tls_acl_log);
                let slowlog = Arc::clone(&tls_slowlog);
                let latency = Arc::clone(&tls_latency);
                let compaction_tx = tls_compaction_tx.clone();
                let cluster_state = Arc::clone(&tls_cluster_state);
                let monitor_registry = Arc::clone(&tls_monitor_registry);
                let tracking_table = Arc::clone(&tls_tracking_table);

                tokio::spawn(async move {
                    if let Err(e) = server::handle_client(
                        tls_stream,
                        store,
                        expiry_store,
                        lastsave,
                        aof_tx,
                        aof_enabled,
                        config,
                        started_at,
                        index_store,
                        dict_store,
                        wait_table,
                        ts_index,
                        pubsub,
                        hub_tx,
                        repl_state,
                        watch_registry,
                        client_registry,
                        peer_addr_str,
                        acl_store,
                        acl_log,
                        slowlog,
                        latency,
                        compaction_tx,
                        cluster_state,
                        monitor_registry,
                        tracking_table,
                    )
                    .await
                    {
                        if e.to_string().contains("unexpected end of file")
                            || e.to_string().contains("Connection reset")
                        {
                            return;
                        }
                        log_verbose!("Connection error: {e}");
                    }
                });
            }
        });
    }

    // Set up signal handlers before entering the accept loop.
    // Both SIGTERM (systemd/kill) and SIGINT (Ctrl+C) trigger shutdown.
    #[cfg(unix)]
    let mut sigterm = {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::terminate())?
    };

    let snapshot_store = Arc::clone(&store);
    let snapshot_expiry_store = Arc::clone(&expiry_store);
    let snapshot_index_store = Arc::clone(&index_store);
    let snapshot_repl_state = Arc::clone(&repl_state);
    // Interval that fires every 50ms to check for command-issued SHUTDOWN.
    // Created OUTSIDE the loop so it accumulates ticks — unlike sleep()
    // inside select!, which resets every iteration and never fires if
    // listener.accept() always completes first.
    let mut shutdown_check = tokio::time::interval(tokio::time::Duration::from_millis(50));
    shutdown_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Consume the first tick which fires immediately.
    shutdown_check.tick().await;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        #[cfg(unix)]
        let shutdown = tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, peer_addr)) => {
                        crate::stats::TOTAL_CONNECTIONS_RECEIVED
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        {
                        let peer_addr_str = peer_addr.to_string();
                        let store = Arc::clone(&store);
                        let expiry_store = Arc::clone(&expiry_store);
                        let lastsave = Arc::clone(&lastsave);
                        let aof_tx = aof_tx.clone();
                        let config = Arc::clone(&config);
                        let started_at = Arc::clone(&started_at);
                        let index_store = Arc::clone(&index_store);
                        let wait_table = Arc::clone(&wait_table);
                        let dict_store = Arc::clone(&dict_store);
                        let ts_index = Arc::clone(&ts_index);
                        let pubsub = pubsub.clone();
                        let hub_tx = hub_tx.clone();
                        let repl_state = Arc::clone(&repl_state);
                        let watch_registry = Arc::clone(&watch_registry);
                        let client_registry = Arc::clone(&client_registry);
                        let acl_store = Arc::clone(&acl_store);
                        let acl_log = Arc::clone(&acl_log);
                        let slowlog = Arc::clone(&slowlog);
                        let latency = Arc::clone(&latency);
                        let compaction_tx = compaction_tx.clone();
                        let cluster_state = Arc::clone(&cluster_state);
                        let monitor_registry = Arc::clone(&monitor_registry);
                        let tracking_table = Arc::clone(&tracking_table);
                        tokio::spawn(async move {
                            if let Err(e) = server::handle_client(
                                socket,
                                store,
                                expiry_store,
                                lastsave,
                                aof_tx,
                                aof_enabled,
                                config,
                                started_at,
                                index_store,
                                dict_store,
                                wait_table,
                                ts_index,
                                pubsub,
                                hub_tx,
                                repl_state,
                                watch_registry,
                                client_registry,
                                peer_addr_str,
                                acl_store,
                                acl_log,
                                slowlog,
                                latency,
                                compaction_tx,
                                cluster_state,
                                monitor_registry,
                                tracking_table
                            )
                            .await
                            {
                                if e.to_string().contains("unexpected end of file")
                                    || e.to_string().contains("Connection reset")
                                {
                                    return;
                                }
                                log_verbose!("Connection error: {e}");
                            }
                        });
                        false
                        }
                    }
                    Err(e) => {
                        log_throttled!(10, crate::loglevel::WARN, "Accept error: {e} — if this is \"Too many open files\", raise the open-file limit (ulimit -n, or LimitNOFILE= under systemd)");
                        false
                    }
                }
            }
            _ = &mut ctrl_c => {
                log_info!("SIGINT received — shutting down gracefully");
                true
            }
            _ = sigterm.recv() => {
                log_info!("SIGTERM received — shutting down gracefully");
                true
            }
            // NEW: poll for command-issued SHUTDOWN every 50 ms.
            // This arm fires rarely (only after SHUTDOWN is sent by a client),
            // so the 50 ms timer overhead is negligible.
            _ = shutdown_check.tick() => {
                // One atomic load every 50ms. Free when SHUTDOWN is never called.
                SHUTDOWN_PENDING.load(Relaxed)
            }
        };

        #[cfg(not(unix))]
        let shutdown = tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, peer_addr)) => {
                        crate::stats::TOTAL_CONNECTIONS_RECEIVED
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        {
                        let peer_addr_str = peer_addr.to_string();
                        let store = Arc::clone(&store);
                        let expiry_store = Arc::clone(&expiry_store);
                        let lastsave = Arc::clone(&lastsave);
                        let aof_tx = aof_tx.clone();
                        let config = Arc::clone(&config);
                        let started_at = Arc::clone(&started_at);
                        let index_store = Arc::clone(&index_store);
                        let wait_table = Arc::clone(&wait_table);
                        let dict_store = Arc::clone(&dict_store);
                        let ts_index = Arc::clone(&ts_index);
                        let pubsub = pubsub.clone();
                        let hub_tx = hub_tx.clone();
                        let repl_state = Arc::clone(&repl_state);
                        let watch_registry = Arc::clone(&watch_registry);
                        let client_registry = Arc::clone(&client_registry);
                        let acl_store = Arc::clone(&acl_store);
                        let acl_log = Arc::clone(&acl_log);
                        let slowlog = Arc::clone(&slowlog);
                        let latency = Arc::clone(&latency);
                        let compaction_tx = compaction_tx.clone();
                        let cluster_state = Arc::clone(&cluster_state);
                        let monitor_registry = Arc::clone(&monitor_registry);
                        let tracking_table = Arc::clone(&tracking_table);
                        tokio::spawn(async move {
                            if let Err(e) = server::handle_client(
                                socket,
                                store,
                                expiry_store,
                                lastsave,
                                aof_tx,
                                aof_enabled,
                                config,
                                started_at,
                                index_store,
                                dict_store,
                                wait_table,
                                ts_index,
                                pubsub,
                                hub_tx,
                                repl_state,
                                watch_registry,
                                client_registry,
                                peer_addr_str,
                                acl_store,
                                acl_log,
                                slowlog,
                                latency,
                                compaction_tx,
                                cluster_state,
                                monitor_registry,
                                tracking_table
                            )
                            .await
                            {
                                if e.to_string().contains("unexpected end of file")
                                    || e.to_string().contains("Connection reset")
                                {
                                    return;
                                }
                                log_verbose!("Connection error: {e}");
                            }
                        });
                        false
                        }
                    }
                    Err(e) => {
                        log_throttled!(10, crate::loglevel::WARN, "Accept error: {e} — if this is \"Too many open files\", raise the open-file limit (ulimit -n, or LimitNOFILE= under systemd)");
                        false
                    }
                }
            }
            _ = &mut ctrl_c => {
                log_info!("Ctrl+C received — shutting down gracefully");
                true
            }
            _ = shutdown_check.tick() => {
                SHUTDOWN_PENDING.load(Relaxed)
            }
        };

        if shutdown {
            break;
        }
    }
    // After breaking out of the loop, read the mode to decide how to exit:
    let m = SHUTDOWN_MODE.load(Relaxed);
    drop(aof_tx);
    if m != mode::NOW {
        // Give the AOF writer a moment to flush pending entries.
        // SHUTDOWN NOW skips this wait.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    if m != mode::NOSAVE {
        log_info!("Stopping — writing final snapshot...");
        let store_c = Arc::clone(&snapshot_store);
        let expiry_c = Arc::clone(&snapshot_expiry_store);
        let index_c = Arc::clone(&snapshot_index_store);
        let repl_c = Arc::clone(&snapshot_repl_state);
        // With the replica's position in its master's stream beside it, so
        // that the restart can ask to continue.
        let result = tokio::task::spawn_blocking(move || {
            crate::replication::save_dataset(&store_c, &expiry_c, &index_c, &repl_c)
        })
        .await;
        match result {
            Ok(Ok(())) => log_info!("Goodbye."),
            // Not a clean exit: whatever restarts this node should know its
            // last writes are not on disk.
            Ok(Err(e)) => {
                log_error!(
                    "Final snapshot failed: {e}. Writes since the last successful save \
                             are not in dump.kdb (they are in the AOF, if appendonly is on). \
                             Check free disk space and write permission on the data directory"
                );
                std::process::exit(1);
            }
            Err(e) => {
                log_error!("Snapshot task panicked: {e}");
                std::process::exit(1);
            }
        }
    } else {
        log_info!("SHUTDOWN NOSAVE — skipping snapshot. Goodbye.");
    }

    unsafe { libc::_exit(0) };
    #[allow(unreachable_code)]
    Ok(())
}
