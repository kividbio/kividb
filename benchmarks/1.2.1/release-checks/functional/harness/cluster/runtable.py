#!/usr/bin/env python3
"""runtable.py <p6*.jsonl>...: markdown per-run table of the ASM + failover rounds (p6_fo_asm.sh output)."""
import json
import sys

print("| sweep | # | failover sent (ms after IMPORT) | vs import end (ms) | first attempt | promoted | import (ms, retries) | acked | lost | stale | wrong | list_lost |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|")
for f in sys.argv[1:]:
    tag = f.split("/")[-1].replace(".jsonl", "")
    for l in open(f):
        d = json.loads(l)
        fa = d.get("first_attempt_error") or ("" if d.get("import_retries") == 0 else "fenced, retried")
        fa = {"": "completed", "ERR no such import in progress": "fenced (dest vote), retried",
              "a replica of the source is being elected to take its slots over": "fenced (election), retried"}.get(fa, fa)
        if fa.startswith("a failover of this node began"):
            fa = "fenced by source (failover began), retried"
        if fa.startswith("the source was not asked to start"):
            fa = "fenced (dest vote), retried"
        sent = d.get("fo_after_import_created_ms")
        if sent is None:
            sent = "shell sleep %.2f s after starting mig.py" % d["delay_s"]
        end = d.get("fo_minus_import_end_ms") if "fo_after_import_created_ms" in d else None
        prom = ("yes, %.2f s" % d["promoted_s"]) if d["promoted"] else "no (refused: %d log line)" % d["mf_refused_logs"]
        print("| %s | %d | %s | %s | %s | %s | %s, %s | %d | %d | %d | %d | %d |" % (
            tag, d["round"], sent, end if end is not None else "-", fa, prom, d["import_ms"], d["import_retries"],
            d["acked"], d["lost"], d["stale"], d["wrong"], d["list_lost"]))
