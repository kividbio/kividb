// src/cluster/bus/mod.rs
//
// The cluster bus — a separate TCP listener (typically client_port + 10000)
// used for inter-node coordination: handshakes (MEET), heartbeats (PING/PONG),
// gossip (piggybacked node info on heartbeats), failure verdicts, replica
// elections, and forwarding of published messages.
//
// The protocol is RESP-based, not Redis's binary cluster bus format. This
// trades wire compatibility with real redis-server for implementation
// simplicity and easier debugging. KiviDB nodes can cluster with each other
// natively; interop with vanilla Redis is a future concern.
//
// The bus has no authentication of its own: anything that can reach the port
// can send it messages. It must be reachable only by cluster members.
//
// Each node runs, on a runtime of its own (see `start`):
//   - One listener task (accepts connections from peers)
//   - One heartbeat task (periodically PINGs all peers)
//   - One failure-detection / election task
//   - One long-lived outbound link per peer, started on demand
//
// All bus state is accessed through the shared Arc<ClusterState>.

pub mod auth;
pub mod dialer;
pub mod heartbeat;
pub mod listener;
pub mod message;

pub use dialer::send_meet;
pub use heartbeat::{run_failure_detection_loop, run_heartbeat_loop};
pub use listener::run_bus_listener;
pub use message::{BusMessage, GossipEntry, SlotClaim, decode, encode};

use crate::cluster::ClusterState;
use std::sync::Arc;

/// Convention: cluster bus port = client port + 10000.
/// This matches Redis's default and is what every cluster client expects.
pub const BUS_PORT_OFFSET: u16 = 10000;

/// Start the cluster bus: listener, heartbeats, failure detection.
///
/// The bus gets a thread and a runtime to itself on every startup path. It
/// used to share the server's runtime on the tokio path, where the workers
/// that would have run it can all be parked at once — every connection task
/// waiting on a write behind a checkpoint or a full-sync cut. A node in that
/// state stops answering PINGs although nothing is wrong with it, and a long
/// enough pause gets it declared failed and failed over. With its own runtime
/// the bus keeps heartbeating whatever the data path is doing.
///
/// Returns once the runtime is up and its handle is published on `state`, so
/// command handlers can spawn bus work as soon as the server accepts clients.
pub fn start(
    state: Arc<ClusterState>,
    repl_state: Arc<crate::replication::ReplState>,
    bind: String,
    bus_port: u16,
) {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<()>(1);
    let spawned = std::thread::Builder::new()
        .name("kividb-cluster-bus".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    log_error!("Cluster: could not start the bus runtime: {e}");
                    let _ = ready_tx.send(());
                    return;
                }
            };
            let _ = state.bus_runtime.set(runtime.handle().clone());
            let _ = ready_tx.send(());
            runtime.block_on(async move {
                tokio::spawn(run_heartbeat_loop(Arc::clone(&state)));
                tokio::spawn(run_failure_detection_loop(Arc::clone(&state), repl_state));
                if let Err(e) = run_bus_listener(bind, bus_port, state).await {
                    log_error!("Cluster: Bus listener exited: {e}");
                }
                // Without a listener this node cannot be reached by its
                // peers, but it can still reach them; keep the runtime alive
                // for the tasks above rather than tearing the bus down.
                std::future::pending::<()>().await;
            });
        });
    match spawned {
        Ok(_) => {
            let _ = ready_rx.recv();
            heartbeat::spawn_bus_watchdog();
        }
        Err(e) => log_error!("Cluster: could not start the bus thread: {e}"),
    }
}
