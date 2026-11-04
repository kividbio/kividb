#!/usr/bin/env python3
"""strandfix.py <holder h:p> <owner h:p> <slot> <out.json>: close a slot that `redis-cli --cluster fix` left open
(rc8 soak, reshardfix rounds). The holder is a non-owner node that still has keys of the slot and the slot IMPORTING;
the owner is the node the cluster gives the slot to.

For each key on the holder:
  - the owner doesn't have it: MIGRATE it to the owner WITHOUT REPLACE (retried after TRYAGAIN);
  - the owner has it too: leave it, and record both copies (type, size, string value). Since fix made the owner the
    owner, clients have been writing such keys on the owner, so the owner's copy is the newer one.
Then CLUSTER SETSLOT <slot> STABLE on the holder. Prints and writes a JSON summary.
"""
import json
import sys
import time

import redis


def c(hp):
    h, p = hp.rsplit(":", 1)
    return redis.Redis(host=h, port=int(p), socket_timeout=30, decode_responses=True)


def desc(x, k):
    x.execute_command("ASKING")
    t = x.type(k)
    d = {"type": t}
    if t == "string":
        x.execute_command("ASKING")
        d["value"] = x.get(k)
    elif t in ("list", "zset", "hash", "set"):
        x.execute_command("ASKING")
        d["len"] = {"list": x.llen, "zset": x.zcard, "hash": x.hlen, "set": x.scard}[t](k)
    return d


holder, owner, slot, out = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
H, O = c(holder), c(owner)
oh, op = owner.rsplit(":", 1)
t0 = time.time()
res = {"slot": slot, "holder": holder, "owner": owner, "holder_keys_before": H.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot),
       "owner_keys_before": O.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot), "moved": [], "both": {}, "errors": []}
keys = H.execute_command("CLUSTER", "GETKEYSINSLOT", slot, 100000)
for k in keys:
    if O.exists(k):
        res["both"][k] = {"owner": desc(O, k), "holder": desc(H, k)}
        continue
    for _ in range(50):
        try:
            H.execute_command("MIGRATE", oh, int(op), k, 0, 10000)
            res["moved"].append(k)
            break
        except redis.exceptions.TryAgainError:
            time.sleep(0.05)
        except redis.ResponseError as e:
            if "BUSYKEY" in str(e):
                res["both"][k] = {"owner": desc(O, k), "holder": desc(H, k), "note": "BUSYKEY at MIGRATE"}
            else:
                res["errors"].append([k, str(e)])
            break
res["setslot_stable"] = str(H.execute_command("CLUSTER", "SETSLOT", slot, "STABLE"))
res["holder_keys_after"] = H.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)
res["owner_keys_after"] = O.execute_command("CLUSTER", "COUNTKEYSINSLOT", slot)
res["n_moved"], res["n_both"] = len(res["moved"]), len(res["both"])
res["moved_ack_keys"] = [k for k in res["moved"] if k.startswith("ack:")]
res["moved_ow_keys"] = [k for k in res["moved"] if k.startswith("ow:")]
res["seconds"] = round(time.time() - t0, 2)
json.dump(res, open(out, "w"), indent=1)
print(json.dumps({k: res[k] for k in ("slot", "holder_keys_before", "owner_keys_before", "n_moved", "n_both", "errors",
                                      "setslot_stable", "holder_keys_after", "owner_keys_after", "seconds")}))
print("moved ack keys:", len(res["moved_ack_keys"]), "moved ow keys:", len(res["moved_ow_keys"]))
print("both:", json.dumps(res["both"])[:1500])
