#!/usr/bin/env python3
"""A replica must keep its own AOF, and that AOF must describe *its* dataset.

The replication applier used to run with `aof_enabled: false`, so a replica
applied its master's entire write stream and logged none of it. Measured on a
real Kubernetes deployment: after 5,004 keys and a full cluster kill, the two
nodes that had been replicas held 0-byte AOFs while the ex-master held 178 KB.
A promoted replica was therefore durable in name only -- it served the right
data from RAM and had nothing on disk to come back from.

Turning the append on is not sufficient by itself, and phase C is why. A full
resync replaces the whole store; the AOF does not follow. Without rebasing it,
a replica's log is either stale-plus-stream (it was a master before) or
stream-only (it was empty), and in both cases replaying it produces a dataset
the node never had. That failure is invisible until a restart, which is the
worst possible time to discover it.

Phases:
  A  a replica with --aof writes a non-empty AOF while following a master
  B  hard-kill both, restart the replica standalone -- it recovers its dataset
     from its own disk, with no master and no snapshot involved
  C  a node that already had data as a master, then resynced from a different
     master, recovers ONLY the new dataset -- proving the post-sync rebase
  D  writes interleaved across databases replay into the right ones, so the
     implicit SELECT the replica writes into its own log is not lost
  E  `CONFIG SET appendonly yes` on a running server seeds the log with the
     data that already exists. Without that the log covers only what happens
     next, and the next restart -- which trusts a non-empty AOF over the
     snapshot -- silently drops everything written before the toggle.

Usage:  replica_aof_durability.py --binary ./target/release/kividb [-n 5000]
Exit:   0 pass | 1 durability failure | 2 harness problem
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time


# ── Minimal RESP client ──────────────────────────────────────────────────────


class Client:
    def __init__(self, port, timeout=10):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.file = self.sock.makefile("rb")

    def cmd(self, *args):
        buf = bytearray(b"*%d\r\n" % len(args))
        for a in args:
            if isinstance(a, str):
                a = a.encode()
            buf += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(buf)
        return self._read()

    def _read(self):
        line = self.file.readline()
        if not line:
            raise IOError("connection closed")
        kind, body = line[:1], line[1:-2]
        if kind in (b"+", b":"):
            return body
        if kind == b"-":
            raise IOError("server error: %s" % body.decode(errors="replace"))
        if kind == b"$":
            n = int(body)
            if n < 0:
                return None
            data = self.file.read(n + 2)
            return data[:-2]
        if kind == b"*":
            n = int(body)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise IOError("unexpected reply type %r" % kind)

    def info(self, section):
        raw = self.cmd("INFO", section) or b""
        out = {}
        for line in raw.decode(errors="replace").splitlines():
            if ":" in line and not line.startswith("#"):
                k, v = line.split(":", 1)
                out[k.strip()] = v.strip()
        return out

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


# ── Server lifecycle ─────────────────────────────────────────────────────────


class Server:
    #: Every server ever spawned, so a failing phase cannot leave one running.
    #: An orphan from an aborted run is not merely untidy: it holds the port,
    #: the next run's `wait_ready` connects to *it* instead of the process it
    #: just started, and the results describe the wrong server entirely.
    spawned = []

    #: `--io-model` for every server, so both legs of the Linux matrix drive
    #: this through their own request path. The replica applier and the AOF
    #: writer are wired differently on the two, and an io_uring regression
    #: already starved that writer once.
    io_model = None

    def __init__(self, binary, port, workdir, replicaof=None, label="", aof=True):
        self.port, self.workdir, self.label = port, workdir, label or str(port)
        require_port_free(port, self.label)
        os.makedirs(workdir, exist_ok=True)
        argv = [os.path.abspath(binary), "--port", str(port), "--bind", "127.0.0.1"]
        if aof:
            argv.append("--aof")
        if Server.io_model:
            argv += ["--io-model", Server.io_model]
        if replicaof:
            # Two separate values, not one "host port" string -- clap declares
            # --replicaof with num_args(2).
            argv += ["--replicaof", replicaof[0], str(replicaof[1])]
        # cwd is the whole story for persistence: kividb has no --dir, so
        # dump.kdb and appendonly.aof land relative to the working directory.
        self.proc = subprocess.Popen(
            argv,
            cwd=workdir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        Server.spawned.append(self)

    def wait_ready(self, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise IOError(
                    "%s exited with %s before accepting connections"
                    % (self.label, self.proc.returncode)
                )
            try:
                c = Client(self.port, timeout=1)
                c.cmd("PING")
                c.close()
                return
            except (OSError, IOError):
                time.sleep(0.1)
        raise IOError("%s never became ready" % self.label)

    @property
    def aof_path(self):
        return os.path.join(self.workdir, "appendonly.aof")

    def aof_size(self):
        return os.path.getsize(self.aof_path) if os.path.exists(self.aof_path) else 0

    def kill_hard(self):
        """SIGKILL, never SHUTDOWN.

        A clean shutdown writes a snapshot, and a snapshot would let the next
        start recover without touching the AOF at all -- which is exactly the
        thing under test. Only an unclean exit proves the AOF did the work.
        """
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=30)
        if self in Server.spawned:
            Server.spawned.remove(self)


def port_listening(port):
    try:
        s = socket.create_connection(("127.0.0.1", port), 0.5)
    except OSError:
        return False
    s.close()
    return True


def require_port_free(port, label, timeout=30.0):
    """Wait for the port to be free, then refuse to start if it still isn't.

    Two different problems share this check, and only one of them is an error:

    * A **stray server from an earlier run** holds the port. Starting anyway is
      the dangerous case -- the leftover answers the readiness probe and the
      whole phase silently measures a process this run never started, which is
      how a fresh replica once reported DBSIZE=6000 and a 220 KB baseline.
    * A server this run **just SIGKILLed** has been reaped, but the kernel has
      not finished tearing its listening socket down. Purely a race, and the
      one that failed CI: phase C started on the port phase B had just killed,
      and lost. Reaping the child says nothing about the socket.

    Polling separates them: the race resolves in milliseconds, a real orphan
    never does.
    """
    deadline = time.monotonic() + timeout
    while port_listening(port):
        if time.monotonic() >= deadline:
            raise IOError(
                "port %d is still in use after %.0fs; refusing to start %s "
                "(kill the stray kividb, or pass different "
                "--master-port/--replica-port)" % (port, timeout, label)
            )
        time.sleep(0.1)


def kill_all_servers():
    # kill_hard mutates Server.spawned, so iterate a copy.
    for srv in list(Server.spawned):
        try:
            srv.kill_hard()
        except (OSError, subprocess.SubprocessError):
            pass
    Server.spawned.clear()


def wait_for(predicate, timeout, what):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, IOError) as e:
            last = e
        time.sleep(0.2)
    raise IOError("timed out waiting for %s (last: %r)" % (what, last))


def write_keys(port, prefix, n, chunk=2000):
    """Pipelined SETs; returns the number acknowledged."""
    s = socket.create_connection(("127.0.0.1", port), 10)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    f = s.makefile("rb")
    acked = 0
    for start in range(0, n, chunk):
        end = min(start + chunk, n)
        buf = bytearray()
        for i in range(start, end):
            k = b"%s:%d" % (prefix.encode(), i)
            v = b"v%d" % i
            buf += b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (
                len(k), k, len(v), v
            )
        s.sendall(buf)
        for _ in range(start, end):
            line = f.readline()
            if not line:
                raise IOError("connection closed after %d acks" % acked)
            if line[:1] != b"+":
                raise IOError("unexpected reply %r" % line)
            acked += 1
    s.close()
    return acked


def drain_aof(server, timeout=120):
    """Poll until the AOF stops growing.

    A fixed sleep cannot distinguish "the record was discarded" from "the
    record is still queued" -- that mistake once reported a healthy build as
    losing 63% of its writes.
    """
    deadline = time.monotonic() + timeout
    last, stable = -1, 0
    while time.monotonic() < deadline:
        size = server.aof_size()
        if size == last:
            stable += 1
            if stable >= 4:
                return size
        else:
            stable = 0
        last = size
        time.sleep(0.25)
    return server.aof_size()


# ── Phases ───────────────────────────────────────────────────────────────────


def phase_a_and_b(binary, root, ports, n, failures):
    print("== Phase A/B: a replica's own AOF, and recovery from it ==")
    master_dir = os.path.join(root, "a-master")
    replica_dir = os.path.join(root, "a-replica")

    master = Server(binary, ports[0], master_dir, label="master")
    master.wait_ready()
    replica = Server(
        binary, ports[1], replica_dir,
        replicaof=("127.0.0.1", ports[0]), label="replica",
    )
    replica.wait_ready()

    rc = Client(ports[1])
    wait_for(
        lambda: rc.info("replication").get("master_link_status") == "up",
        30, "replica link up",
    )
    print("ok    replica linked to master")

    acked = write_keys(ports[0], "rk", n)
    if acked != n:
        print("FAIL  master acknowledged only %d of %d writes" % (acked, n))
        failures.append("phase A: writes not acknowledged")
        return
    wait_for(lambda: int(rc.cmd("DBSIZE")) >= n, 60, "replica to converge")
    print("ok    replica converged on %d keys" % n)

    size = drain_aof(replica)
    persist = rc.info("persistence")
    print("      replica AOF   : %d bytes on disk" % size)
    print("      INFO reports  : aof_current_size=%s aof_base_size=%s lost=%s"
          % (persist.get("aof_current_size"), persist.get("aof_base_size"),
             persist.get("aof_lost_records")))

    if size == 0:
        print("FAIL  the replica's AOF is empty -- it applied %d writes and "
              "logged none of them." % n)
        print("      Promoting this node yields a server with nothing on disk.")
        failures.append("phase A: replica AOF empty")
    else:
        print("PASS  the replica logged the stream it applied")

    if persist.get("aof_current_size") in (None, "0"):
        print("FAIL  INFO persistence does not report a non-zero aof_current_size")
        failures.append("phase A: aof_current_size missing/zero")
    if persist.get("aof_lost_records") not in (None, "0"):
        print("FAIL  aof_lost_records=%s -- the AOF has a hole"
              % persist.get("aof_lost_records"))
        failures.append("phase A: records lost")
    rc.close()

    # ── Phase B ──
    print()
    print("      hard-killing both nodes (SIGKILL, no snapshot)")
    replica.kill_hard()
    master.kill_hard()

    # No --replicaof: this is the promotion case. Nothing but the local AOF
    # can supply the data now.
    promoted = Server(binary, ports[1], replica_dir, label="promoted")
    promoted.wait_ready()
    pc = Client(ports[1])
    recovered = int(pc.cmd("DBSIZE"))
    spot = pc.cmd("GET", "rk:%d" % (n // 2))
    print("      after restart : DBSIZE=%d, rk:%d=%r" % (recovered, n // 2, spot))

    if recovered < n or spot != b"v%d" % (n // 2):
        print("FAIL  the promoted replica recovered %d of %d keys" % (recovered, n))
        failures.append("phase B: promoted replica lost data")
    else:
        print("PASS  the promoted replica rebuilt %d keys from its own disk" % n)
    pc.close()
    promoted.kill_hard()


def phase_c(binary, root, ports, n, failures):
    print()
    print("== Phase C: a resync rebases the AOF onto the new dataset ==")
    node_dir = os.path.join(root, "c-node")
    master_dir = os.path.join(root, "c-master")

    # 1. The node lives a first life as a standalone master with its own data.
    solo = Server(binary, ports[1], node_dir, label="node(solo)")
    solo.wait_ready()
    write_keys(ports[1], "old", n)
    drain_aof(solo)
    old_size = solo.aof_size()
    print("ok    node wrote %d 'old:*' keys as a standalone master (AOF %d bytes)"
          % (n, old_size))
    solo.kill_hard()

    # 2. A different master, with a disjoint dataset.
    master = Server(binary, ports[0], master_dir, label="master")
    master.wait_ready()
    write_keys(ports[0], "new", n)
    print("ok    master holds %d 'new:*' keys" % n)

    # 3. The node comes back as that master's replica, carrying its old AOF.
    node = Server(
        binary, ports[1], node_dir,
        replicaof=("127.0.0.1", ports[0]), label="node(replica)",
    )
    node.wait_ready()
    nc = Client(ports[1])
    wait_for(
        lambda: nc.info("replication").get("master_link_status") == "up",
        30, "node link up",
    )
    wait_for(lambda: int(nc.cmd("DBSIZE")) >= n, 60, "node to converge")
    # A write after the resync, so the node's log is image + stream rather than
    # image alone -- the shape a real replica actually has on disk. It has to
    # go through the master: the node is a read-only replica, and the point is
    # to exercise the replicated path anyway.
    mc = Client(ports[0])
    mc.cmd("SET", "new:sentinel", "post-resync")
    mc.close()
    wait_for(lambda: nc.cmd("GET", "new:sentinel") == b"post-resync",
             30, "sentinel to replicate")
    drain_aof(node)
    print("ok    node resynced: DBSIZE=%d, AOF now %d bytes"
          % (int(nc.cmd("DBSIZE")), node.aof_size()))
    nc.close()

    # 4. Kill and restart standalone -- what does its AOF actually replay to?
    node.kill_hard()
    master.kill_hard()
    revived = Server(binary, ports[1], node_dir, label="node(revived)")
    revived.wait_ready()
    vc = Client(ports[1])
    stale = int(vc.cmd("EXISTS", "old:0") or 0)
    fresh = int(vc.cmd("EXISTS", "new:0") or 0)
    sentinel = vc.cmd("GET", "new:sentinel")
    total = int(vc.cmd("DBSIZE"))
    print("      after restart : DBSIZE=%d, old:0 exists=%d, new:0 exists=%d, "
          "sentinel=%r" % (total, stale, fresh, sentinel))

    if stale:
        print("FAIL  'old:*' keys survived the resync in the AOF -- the log "
              "replays to a dataset this node never had.")
        failures.append("phase C: stale pre-resync data in AOF")
    if not fresh or sentinel != b"post-resync":
        print("FAIL  the synced dataset is not fully recoverable from the AOF")
        failures.append("phase C: post-resync data missing")
    if not stale and fresh and sentinel == b"post-resync":
        print("PASS  the AOF was rebased: only the synced dataset came back")
    vc.close()
    revived.kill_hard()


def phase_d(binary, root, ports, failures):
    print()
    print("== Phase D: the replica's AOF keeps writes in the right database ==")
    master_dir = os.path.join(root, "d-master")
    replica_dir = os.path.join(root, "d-replica")

    master = Server(binary, ports[0], master_dir, label="master")
    master.wait_ready()
    replica = Server(binary, ports[1], replica_dir,
                     replicaof=("127.0.0.1", ports[0]), label="replica")
    replica.wait_ready()
    rc = Client(ports[1])
    wait_for(lambda: rc.info("replication").get("master_link_status") == "up",
             30, "replica link up")

    # Interleave databases so the stream carries several SELECT switches. The
    # replica writes one AOF for all of them, and a SELECT that goes missing --
    # or lands in the wrong record -- silently files keys under the wrong db.
    mc = Client(ports[0])
    plan = [(0, "d0a"), (3, "d3a"), (0, "d0b"), (7, "d7a"), (3, "d3b"), (7, "d7b")]
    for db, key in plan:
        mc.cmd("SELECT", str(db))
        mc.cmd("SET", key, "in-db-%d" % db)
    mc.close()

    wait_for(lambda: (rc.cmd("SELECT", "7"), rc.cmd("GET", "d7b"))[1] == b"in-db-7",
             30, "last write to replicate")
    drain_aof(replica)
    rc.close()

    replica.kill_hard()
    master.kill_hard()
    revived = Server(binary, ports[1], replica_dir, label="replica(revived)")
    revived.wait_ready()
    vc = Client(ports[1])

    wrong = []
    for db, key in plan:
        vc.cmd("SELECT", str(db))
        got = vc.cmd("GET", key)
        if got != b"in-db-%d" % db:
            wrong.append((db, key, got))
    # And nothing leaked across: db 0 must not have picked up db 3's keys.
    vc.cmd("SELECT", "0")
    leaked = int(vc.cmd("EXISTS", "d3a") or 0) + int(vc.cmd("EXISTS", "d7a") or 0)
    print("      after restart : %d/%d keys in the right db, %d leaked into db 0"
          % (len(plan) - len(wrong), len(plan), leaked))

    if wrong or leaked:
        for db, key, got in wrong:
            print("FAIL  db %d key %s came back as %r" % (db, key, got))
        if leaked:
            print("FAIL  %d key(s) from other databases landed in db 0" % leaked)
        failures.append("phase D: AOF lost database routing")
    else:
        print("PASS  every key replayed into the database it was written to")
    vc.close()
    revived.kill_hard()


def phase_e(binary, root, ports, n, failures):
    print()
    print("== Phase E: CONFIG SET appendonly yes seeds the log with existing data ==")
    node_dir = os.path.join(root, "e-node")

    # AOF off at boot, so nothing is logged while the data is written.
    srv = Server(binary, ports[1], node_dir, aof=False, label="node(no-aof)")
    srv.wait_ready()
    write_keys(ports[1], "pre", n)
    c = Client(ports[1])
    print("ok    wrote %d 'pre:*' keys with AOF off (AOF %d bytes)"
          % (n, srv.aof_size()))

    # Turning it on must capture what already exists. If it only starts logging
    # from here, the next restart sees a non-empty AOF, treats it as
    # authoritative, and every pre-existing key is gone -- silently.
    print("      CONFIG SET appendonly yes ->", c.cmd("CONFIG", "SET", "appendonly", "yes"))
    c.cmd("SET", "post:toggle", "after")
    size = drain_aof(srv)
    persist = c.info("persistence")
    print("      AOF after toggle: %d bytes (base=%s)"
          % (size, persist.get("aof_base_size")))
    c.close()

    srv.kill_hard()
    revived = Server(binary, ports[1], node_dir, label="node(revived)")
    revived.wait_ready()
    vc = Client(ports[1])
    recovered = int(vc.cmd("DBSIZE"))
    spot = vc.cmd("GET", "pre:%d" % (n // 2))
    post = vc.cmd("GET", "post:toggle")
    print("      after restart : DBSIZE=%d, pre:%d=%r, post:toggle=%r"
          % (recovered, n // 2, spot, post))

    if recovered < n + 1 or spot != b"v%d" % (n // 2) or post != b"after":
        print("FAIL  enabling AOF at runtime did not capture the existing "
              "dataset -- %d of %d keys survived the restart" % (recovered, n + 1))
        failures.append("phase E: CONFIG SET appendonly lost pre-existing data")
    else:
        print("PASS  the runtime toggle seeded the log; nothing was lost")
    vc.close()
    revived.kill_hard()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True, help="path to the kividb binary")
    ap.add_argument("-n", type=int, default=5000, help="keys per phase")
    ap.add_argument("--master-port", type=int, default=7410)
    ap.add_argument("--replica-port", type=int, default=7411)
    ap.add_argument("--keep", action="store_true", help="keep the temp workdir")
    ap.add_argument("--io-model", default=None,
                    help="passed through to every server (tokio | uring)")
    args = ap.parse_args()
    Server.io_model = args.io_model

    if not os.path.exists(args.binary):
        print("FAIL  no binary at %s" % args.binary)
        return 2

    root = tempfile.mkdtemp(prefix="kividb-replica-aof-")
    ports = (args.master_port, args.replica_port)
    failures = []
    try:
        phase_a_and_b(args.binary, root, ports, args.n, failures)
        phase_c(args.binary, root, ports, args.n, failures)
        phase_d(args.binary, root, ports, failures)
        phase_e(args.binary, root, ports, args.n, failures)
    except (OSError, IOError, subprocess.SubprocessError) as e:
        print("\nHARNESS ERROR: %s" % e)
        return 2
    finally:
        # Before the temp dir goes: a phase that raised left its servers up.
        kill_all_servers()
        if args.keep:
            print("\n(workdir kept at %s)" % root)
        else:
            shutil.rmtree(root, ignore_errors=True)

    print()
    if failures:
        print("FAIL  %d check(s) failed:" % len(failures))
        for f in failures:
            print("      - %s" % f)
        return 1
    print("PASS  replica AOF durability holds across all phases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
