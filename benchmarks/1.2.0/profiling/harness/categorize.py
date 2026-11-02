"""categorize.py PROFILE_DIR OUT_CSV

Splits each profile's CPU samples (folded stacks from stackcollapse-perf.pl
--kernel, kernel frames end in _[k]) into categories, and multiplies by the
measured server CPU time per request (cpu.csv) to get microseconds per
request in each category.

Kernel samples are classified by the first matching frame anywhere in the
stack; user samples by walking from the leaf towards the root and taking
the first frame that matches a user category, so a memcpy called by the
parser counts as parsing, and malloc called by a command counts as
allocation.
"""
import csv
import gzip
import os
import re
import sys

KERNEL = [
    ("kernel: thread sleep/wake (futex, scheduler)", r"futex|schedule|__x64_sys_sched_yield|finish_task_switch|try_to_wake_up|pick_next_task|do_nanosleep|ep_poll(?!_callback)|io_cqring_wait|io_cqring_wait_schedule"),
    ("kernel: network I/O (syscalls, io_uring, TCP, softirq)", r"io_uring|io_submit|io_issue|tcp_|sock_|inet_|__sys_sendto|__sys_recvfrom|ksys_read|ksys_write|net_rx|napi|ip_|skb|__netif|epoll|do_syscall_64|entry_SYSCALL|syscall_exit|irq|softirq|__x64_sys_"),
]
USER = [
    ("memory allocation", r"\bmalloc\b|\bfree\b|realloc|calloc|^je_|jemalloc|^mi_|_int_malloc|_int_free|tcache|sdallocx|rallocx|mallocx|__rust_alloc|__rust_dealloc|__rust_realloc|alloc::alloc|zmalloc|zfree|zcalloc|zrealloc|ztrymalloc"),
    ("locks and atomics", r"parking_lot|RwLock|rwlock|Mutex|mutex|spin_loop|lock_shared|lock_exclusive|unlock|pthread_mutex|pthread_spin|atomic"),
    ("parsing requests", r"kividb::resp::parse|resp::parse|processMultibulkBuffer|processInlineBuffer|processInputBuffer|parseMultibulk|parse_command|::parse::|RespParser"),
    ("building replies", r"kividb::resp::write|resp::write|addReply|_addReplyToBuffer|_addReplyProtoToList|setDeferred|push_decimal|ReplyBuf|write_bulk|write_simple|write_int"),
    ("command execution (lookup, read/write the value)", r"kividb::commands|kividb::store|kividb::types|kividb::value_slab|kividb::shared_bytes|kividb::expiry|kividb::eviction|kividb::cmdstats|run_command|execute_command|^call$|processCommand|lookupKey|^dict|dictFind|kvstore|getCommand|setCommand|setGenericCommand|getGenericCommand|dbAdd|dbFind|dbSet|setKey|lookupCommand|siphash|hash_one|hashbrown|ahash|xxh|crc|keyHashSlot|updateLFU|updateCommandLatency|commandProcessed|afterCommand|propagate|notifyKeyspaceEvent|signalModifiedKey|incrRefCount|decrRefCount|createStringObject|sdsnew|sdscat|sdsMakeRoom|objectCommand|lookupKeyRead|lookupKeyWrite|getExpire|expireIfNeeded"),
    ("event loop and connection handling (user space)", r"kividb::server_uring|kividb::server|kividb::listen|kividb::client_registry|io_uring|liburing|tokio|mio::|aeProcessEvents|aeMain|aeApiPoll|readQueryFromClient|writeToClient|_writeToClient|handleClientsWithPendingWrites|connSocket|connRead|connWrite|IOThread|ioThread|beforeSleep|afterSleep|processIOThreads|sendReplyToClient|postponeClientRead|clientInstall|readToQueryBuf|handleClientsWithPendingReads|processClientsFromIOThread|enqueuePending|freeClientArgv|resetClient|commandProcessed|epoll_wait|__libc_read|__libc_write|__libc_recv|__libc_send|read$|write$|recv|send"),
]
KRX = [(n, re.compile(r)) for n, r in KERNEL]
URX = [(n, re.compile(r)) for n, r in USER]
ORDER = [n for n, _ in KERNEL] + [n for n, _ in USER] + ["other user space"]


def classify(frames):
    kframes = [f[:-4] for f in frames if f.endswith("_[k]")]
    if kframes:
        joined = ";".join(kframes)
        for name, rx in KRX:
            if rx.search(joined):
                return name
        return "kernel: network I/O (syscalls, io_uring, TCP, softirq)"
    for f in reversed(frames[1:]):  # leaf first, skip comm
        for name, rx in URX:
            if rx.search(f):
                return name
    return "other user space"


def main():
    root, out = sys.argv[1], sys.argv[2]
    rows = []
    trows = []
    for engine in sorted(os.listdir(root)):
        d = os.path.join(root, engine)
        if not os.path.isdir(d):
            continue
        cpu = {}
        cp = os.path.join(d, "cpu.csv")
        if os.path.exists(cp):
            for r in csv.DictReader(open(cp)):
                cpu[f"{r['workload']}-p{r['pipeline']}"] = r
        for f in sorted(os.listdir(d)):
            if not f.endswith(".folded.gz"):
                continue
            tag = f[: -len(".folded.gz")]
            totals = {}
            threads = {}
            n = nostack = 0
            for line in gzip.open(os.path.join(d, f), "rt", errors="replace"):
                stack, _, cnt = line.rstrip("\n").rpartition(" ")
                if not cnt.isdigit():
                    continue
                c = int(cnt)
                frames = stack.split(";")
                if len(frames) < 2:
                    # perf took the sample but captured no call chain at all (16-20% of
                    # samples on these VMs, for every engine): left out of the shares
                    nostack += c
                    continue
                cat = classify(frames)
                totals[cat] = totals.get(cat, 0) + c
                n += c
                th = re.sub(r"[-_]?\d+$", "", frames[0]) or frames[0]
                threads.setdefault(th, {}).setdefault(cat, 0)
                threads[th][cat] += c
            us = float(cpu[tag]["total_us_per_op"]) if tag in cpu else 0.0
            for th, cats in sorted(threads.items()):
                tn = sum(cats.values())
                trows.append({"engine": engine, "workload": tag, "thread": th,
                              "share_pct": round(tn * 100.0 / n, 1) if n else 0,
                              "us_per_op": round(tn / n * us, 3) if n else 0,
                              "top_category": max(cats, key=cats.get)})
            for cat in ORDER:
                share = totals.get(cat, 0) / n if n else 0.0
                rows.append({"engine": engine, "workload": tag, "category": cat, "samples": totals.get(cat, 0),
                             "share_pct": round(share * 100, 1), "us_per_op": round(share * us, 3),
                             "total_us_per_op": us,
                             "no_stack_pct": round(nostack * 100.0 / (n + nostack), 1) if n + nostack else 0})
    with open(out, "w", newline="\n") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    tout = out[:-4] + "-threads.csv"
    with open(tout, "w", newline="\n") as fh:
        w = csv.DictWriter(fh, fieldnames=list(trows[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(trows)
    print(f"{len(rows)} rows -> {out}, {len(trows)} -> {tout}")


if __name__ == "__main__":
    main()
