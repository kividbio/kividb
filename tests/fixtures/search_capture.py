# Captures raw replies of the search commands to a text file: one line per case,
# "name<TAB>hex of the reply's bytes". Usage: cap2.py <port> <2|3> <out file>
# The same cases are sent to valkey/valkey-bundle:9.2.0 and to KiviDB, over
# RESP3 (and RESP2 for KiviDB), to compare their bytes.
import re, socket, struct, sys, time


def enc(*parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        if isinstance(p, str):
            p = p.encode()
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out


def run(port, proto, outpath):
    s = socket.create_connection(("127.0.0.1", port))

    def call(*parts):
        s.sendall(enc(*parts))
        buf = b""
        first = True
        while True:
            try:
                s.settimeout(5 if first else 0.4)
                d = s.recv(65536)
            except socket.timeout:
                break
            if not d:
                break
            buf += d
            first = False
        return buf

    v = lambda *f: struct.pack("<%df" % len(f), *f)
    res = []

    def rec(name, *parts):
        r = call(*parts)
        res.append((name, r))
        print(name, repr(r))
        return r

    call("HELLO", proto)
    call("FLUSHALL")
    call("FT.DROPINDEX", "idx")
    rec("create", "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA", "t", "TAG", "n", "NUMERIC", "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2")
    call("HSET", "d:1", "t", "a", "n", "1", "v", v(0, 0), "extra", "x1")
    call("HSET", "d:2", "t", "a", "n", "2", "v", v(1, 0), "extra", "x2")
    call("HSET", "d:3", "t", "b", "n", "3", "v", v(0, 2), "extra", "x3")
    time.sleep(1.5)
    q = "*=>[KNN 3 @v $B AS dist]"
    B = v(0, 0)
    P = ["PARAMS", "2", "B", B, "DIALECT", "2"]
    rec("knn_all", "FT.SEARCH", "idx", q, *P)
    rec("knn_return", "FT.SEARCH", "idx", q, "RETURN", "2", "t", "dist", *P)
    rec("knn_return_vec", "FT.SEARCH", "idx", q, "RETURN", "2", "v", "dist", *P)
    rec("knn_return0", "FT.SEARCH", "idx", q, "RETURN", "0", *P)
    rec("knn_nocontent", "FT.SEARCH", "idx", q, "NOCONTENT", *P)
    rec("knn_limit00", "FT.SEARCH", "idx", q, "LIMIT", "0", "0", *P)
    rec("knn_limit11", "FT.SEARCH", "idx", q, "RETURN", "1", "dist", "LIMIT", "1", "1", *P)
    rec("knn_withscores", "FT.SEARCH", "idx", q, "WITHSCORES", "RETURN", "1", "dist", *P)
    rec("knn_filter", "FT.SEARCH", "idx", "(@t:{a})=>[KNN 3 @v $B AS dist]", "RETURN", "1", "dist", *P)
    rec("knn_empty", "FT.SEARCH", "idx", "(@t:{zzz})=>[KNN 3 @v $B AS dist]", *P)
    rec("knn_empty_nocontent", "FT.SEARCH", "idx", "(@t:{zzz})=>[KNN 3 @v $B AS dist]", "NOCONTENT", *P)
    rec("tag_one", "FT.SEARCH", "idx", "@t:{b}")
    rec("tag_one_return", "FT.SEARCH", "idx", "@t:{b}", "RETURN", "1", "n")
    rec("tag_empty", "FT.SEARCH", "idx", "@t:{zzz}")
    rec("num_one", "FT.SEARCH", "idx", "@n:[3 3]", "NOCONTENT")
    rec("num_sortby", "FT.SEARCH", "idx", "@n:[1 3]", "SORTBY", "n", "DESC", "NOCONTENT")
    rec("agg_basic", "FT.AGGREGATE", "idx", "@n:[1 3]", "LOAD", "1", "@n", "SORTBY", "2", "@n", "ASC")
    rec("agg_groupby", "FT.AGGREGATE", "idx", "*", "GROUPBY", "1", "@t", "REDUCE", "COUNT", "0", "AS", "c", "SORTBY", "2", "@t", "ASC")
    rec("agg_empty", "FT.AGGREGATE", "idx", "@t:{zzz}", "LOAD", "1", "@n")
    rec("agg_knn", "FT.AGGREGATE", "idx", q, "LOAD", "1", "@dist", *P)
    r = rec("agg_cursor", "FT.AGGREGATE", "idx", "@n:[1 3]", "LOAD", "1", "@n", "SORTBY", "2", "@n", "ASC", "WITHCURSOR", "COUNT", "2")
    cid = re.findall(rb":(\d+)\r\n", r)[-1].decode()
    rec("agg_cursor_read", "FT.CURSOR", "READ", "idx", cid)
    rec("info", "FT.INFO", "idx")
    rec("list", "FT._LIST")
    H = ["FT.HYBRID", "idx", "SEARCH", "@t:{a}", "VSIM", "@v", "$B"]
    rec("hybrid_key", *H, "PARAMS", "2", "B", B)
    rec("hybrid_load", *H, "COMBINE", "RRF", "2", "CONSTANT", "60", "LOAD", "1", "@n", "PARAMS", "2", "B", B)
    rec("hybrid_load_key", *H, "KNN", "2", "K", "2", "COMBINE", "RRF", "2", "CONSTANT", "60", "LOAD", "2", "@n", "@__key", "PARAMS", "2", "B", B)
    rec("hybrid_empty", "FT.HYBRID", "idx", "SEARCH", "@t:{zzz}", "VSIM", "@v", "$B", "KNN", "2", "K", "2", "FILTER", "@t:{zzz}", "PARAMS", "2", "B", B)
    with open(outpath, "w") as f:
        for name, r in res:
            f.write("%s\t%s\n" % (name, r.hex()))


run(int(sys.argv[1]), sys.argv[2], sys.argv[3])
