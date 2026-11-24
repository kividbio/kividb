#!/usr/bin/env python3
"""Commands for measure.sh's WORKLOADS=json (#1001), in RESP, for `redis-cli --pipe`.

  json_gen.py <dir> <n> <n_embedding>

Writes into <dir> (each -warm file starts by loading the keys):

  emb-set(-warm)  JSON.SET emb:<i> $ <document>, i below 200: the shape of
                  #1001, a 1,536-element float32 embedding written as doubles
                  (as Python's json.dumps writes float(np.float32(x))) and five
                  short fields, about 30 KB of text
  emb-get(-warm)  JSON.GET emb:<i>
  small-set(-warm), small-get(-warm)  the same for 10,000 documents of seven
                  short members
"""
import random
import struct
import sys

DIM = 1536
EMB_KEYS = 200
SMALL_KEYS = 10000


def resp(*args):
    out = ["*%d\r\n" % len(args)]
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out.append("$%d\r\n" % len(b))
        out.append(b.decode("latin-1"))
        out.append("\r\n")
    return "".join(out)


def f32(x):
    return struct.unpack("f", struct.pack("f", x))[0]


def embedding_doc(rng, i):
    emb = ",".join(repr(f32(rng.uniform(-1.0, 1.0))) for _ in range(DIM))
    return ('{"prefix":"memories","key":"emb:%d","namespace":"user:42","created_at":%d,'
            '"content":"note %d about topic %d","embedding":[%s]}' % (i, 1732000000 + i, i, i % 50, emb))


def small_doc(rng, i):
    return ('{"name":"user %d","age":%d,"email":"user%d@example.com","tags":["a","b","c"],'
            '"active":true,"score":%r,"address":{"city":"Pune","zip":"411001"}}'
            % (i, rng.randrange(18, 90), i, round(rng.uniform(0, 5), 2)))


def main():
    out, n, n_emb = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    rng = random.Random(1001)
    docs = [embedding_doc(rng, i) for i in range(EMB_KEYS)]

    def write(name, lines):
        with open("%s/%s" % (out, name), "w", newline="") as f:
            f.writelines(lines)

    # Each warm-up file starts with the load: the measured commands find
    # every key there.
    load = [resp("JSON.SET", "emb:%d" % i, "$", d) for i, d in enumerate(docs)]
    for name, count in (("emb-set-warm", 20), ("emb-set", n_emb)):
        picks = [rng.randrange(EMB_KEYS) for _ in range(count)]
        lines = [resp("JSON.SET", "emb:%d" % i, "$", docs[i]) for i in picks]
        write(name, (load if name.endswith("warm") else []) + lines)
    for name, count in (("emb-get-warm", 20), ("emb-get", n_emb)):
        lines = [resp("JSON.GET", "emb:%d" % rng.randrange(EMB_KEYS)) for _ in range(count)]
        write(name, (load if name.endswith("warm") else []) + lines)
    small = [small_doc(rng, i) for i in range(SMALL_KEYS)]
    load = [resp("JSON.SET", "small:%d" % i, "$", d) for i, d in enumerate(small)]
    for name, count in (("small-set-warm", 2000), ("small-set", n)):
        picks = [rng.randrange(SMALL_KEYS) for _ in range(count)]
        lines = [resp("JSON.SET", "small:%d" % i, "$", small[i]) for i in picks]
        write(name, (load if name.endswith("warm") else []) + lines)
    for name, count in (("small-get-warm", 2000), ("small-get", n)):
        lines = [resp("JSON.GET", "small:%d" % rng.randrange(SMALL_KEYS)) for _ in range(count)]
        write(name, (load if name.endswith("warm") else []) + lines)


if __name__ == "__main__":
    main()
