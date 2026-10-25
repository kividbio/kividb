// src/lib.rs
// First, so its logging macros are in scope in every module after it.
#[macro_use]
pub mod loglevel;
pub mod acl;
pub mod aof;
pub mod cluster;
pub mod cmdstats;
pub mod command_gate;
pub mod commands;
pub mod compaction;
pub mod config;
pub mod eviction;
pub mod field_expiry;
pub mod latency;
pub mod lazyfree;
pub mod listen;
#[cfg(feature = "lua")]
mod lua_vm_blocking;
pub mod member_set;
pub mod memory_use;
pub mod monitor;
pub mod node_tls;
pub mod pause;
pub mod protected_mode;
pub mod pubsub;
pub mod replication;
pub mod resp;
pub mod script_clock;
pub mod server;
pub mod shared_bytes;
pub mod shutdown;
pub mod slowlog;
pub mod snapshot;
pub mod stats;
pub mod store;
pub mod tag_locks;
pub mod tracking;
pub mod types;
pub mod update_check;
pub mod value_slab;
pub mod vector;
pub mod wait_table;
pub mod watch;
pub mod write_gate;
pub mod write_order;
use crate::replication::{ReplState, start_hub};
pub mod client_registry;
pub mod keyspace_events;
pub mod keyspec;

#[cfg(feature = "tls")]
mod tls;

#[cfg(target_os = "linux")]
pub mod server_uring;

/// Counts what it allocates: `maxmemory` is measured against that. See
/// `memory_use`.
#[global_allocator]
static ALLOCATOR: memory_use::Counting = memory_use::Counting;

pub static RUNTIME_MAXMEMORY: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
/// Entry point for integration tests — starts a tokio server on the given
/// config and blocks the calling thread. Meant to run inside a thread::spawn.
pub fn run_for_test(config: config::Config) {
    let cfg = std::sync::Arc::new(config);
    crate::RUNTIME_MAXMEMORY.store(cfg.maxmemory, std::sync::atomic::Ordering::Relaxed);
    // Only ever switched on here: the servers of one test process share it.
    if cfg!(feature = "lua") && cfg.lua_parallel_scripts {
        crate::tag_locks::set_enabled(true);
    }
    crate::eviction::configure(&cfg);
    crate::compaction::configure(&cfg);
    if let Err(e) = crate::node_tls::init(&cfg) {
        panic!("TLS between nodes: {e}");
    }
    crate::protected_mode::set(cfg.protected_mode);
    let started_at = std::time::Instant::now();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(crate::server_tokio(cfg, started_at))
        .unwrap();
}

async fn server_tokio(
    config: std::sync::Arc<config::Config>,
    started_at: std::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    use parking_lot::RwLock;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    let store = store::new_store(config.databases);
    let expiry_store = store::new_expiry_store(config.databases);
    let index_store = vector::new_index_store();
    let lastsave = Arc::new(RwLock::new(0i64));
    let aof_enabled = config.aof;
    let started_at = Arc::new(started_at);
    let wait_table = Arc::new(crate::wait_table::WaitTable::new());
    let dict_store = vector::new_dict_store();
    vector::register_dict_store(&index_store, &dict_store);
    let ts_index = store::new_ts_index();
    let pubsub = pubsub::PubSubHub::new();
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
    let latency = crate::latency::new_latency_tracker();
    crate::commands::helpers::spawn_active_expire_task(
        Arc::clone(&store),
        Arc::clone(&expiry_store),
        Arc::clone(&index_store),
        pubsub.clone(),
        Arc::clone(&latency),
    );
    crate::compaction::spawn(Arc::clone(&store));
    let watch_registry = crate::watch::new_watch_registry();
    crate::watch::register_for(&store, &watch_registry);
    let client_registry = crate::client_registry::new_client_registry();
    let acl_store = acl::new_acl_store(&config.requirepass);
    let acl_log = acl::new_acl_log();
    let slowlog = crate::slowlog::new_slowlog_from_config(&config);
    let cluster_state = std::sync::Arc::new(
        crate::cluster::ClusterState::from_config(&config).expect("cluster config file"),
    );
    let tracking_table = crate::tracking::new_tracking_table();
    crate::tracking::register_for(&store, &tracking_table, &client_registry, &acl_store);

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
                // Fatal — misconfigured ACL file should not silently proceed
                std::process::exit(1);
            }
        }
    }
    // The AOF writer gets a dedicated single-thread runtime rather than a task on
    // the shared one. `rewrite_aof` quiesces by holding every shard lock and then
    // waits for the writer to perform the swap; if the writer were a task here,
    // enough connection tasks blocking synchronously on those same locks could
    // occupy every worker thread and the writer would never be polled -- the
    // server would hang holding all locks. A dedicated runtime makes that
    // impossible. (The io_uring path already does this for its own reasons.)
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(aof::start_aof_writer(aof_rx))
    });

    // One monitor registry for the whole server. Each connection used to be
    // handed a registry of its own, so `MONITOR` on one connection could never
    // see commands run on another.
    let monitor_registry = crate::monitor::new_monitor_registry();

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
    cluster_state.start(
        &config,
        crate::cluster::ClusterHooks {
            replica_starter: replica_starter.clone(),
            pubsub: pubsub.clone(),
        },
    );
    if let Some((host, port)) = config.replicaof_addr() {
        replica_starter.start(host, port);
    } else if let Some(master_id) = cluster_state.my_master_id() {
        // A cluster replica restarting: resume following its master.
        let addr = cluster_state
            .nodes
            .read()
            .get(&master_id)
            .map(|m| (m.ip.clone(), m.client_port));
        if let Some((ip, port)) = addr {
            replica_starter.start(ip, port);
        }
    }

    let addr: SocketAddr = config.addr().parse()?;
    let listener = TcpListener::bind(addr).await?;

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
        let tls_aof_tx = aof_tx.clone();
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
                    let _ = server::handle_client(
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
                    .await;
                });
            }
        });
    }

    let mut listener = crate::listen::Listener::new(listener, config.port);
    loop {
        let (socket, peer_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        crate::stats::TOTAL_CONNECTIONS_RECEIVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let peer_addr_str = peer_addr.to_string();
        let store = Arc::clone(&store);
        let expiry_store = Arc::clone(&expiry_store);
        let index_store = Arc::clone(&index_store);
        let lastsave = Arc::clone(&lastsave);
        let aof_tx = aof_tx.clone();
        let config = Arc::clone(&config);
        let started_at = Arc::clone(&started_at);
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
            let _ = server::handle_client(
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
                tracking_table,
            )
            .await;
        });
    }
}
