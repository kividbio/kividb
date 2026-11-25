#!/usr/bin/env python3
"""Open WebUI's Valkey vector store, run as shipped against a server.

usage: check.py PKG_DIR PORT OUT.xml

PKG_DIR holds the `open_webui` package that ci.sh built: Open WebUI's own valkey.py,
vector/main.py, vector/utils.py and json_codec.py at a pinned commit, and small stand-ins for the
three modules they import that belong to the rest of the application (config, env, misc). The
real `ValkeyClient` is driven through valkey-glide-sync; nothing is patched. Each step is one
check in the JUnit XML (benchmarks/README.md): the step passes when what the client returns is
what the store should hold, not merely when it raises nothing (`search`, `query` and `delete`
catch the client's errors and return None or nothing).
"""
import importlib.util
import os
import sys
import time

pkg, port, out = sys.argv[1], sys.argv[2], sys.argv[3]
sys.path.insert(0, pkg)
os.environ["VALKEY_URL"] = f"valkey://127.0.0.1:{port}"

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "compat_junit", os.path.join(HERE, "..", "ci", "compat_junit.py")
)
compat_junit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compat_junit)

SUITE = "open-webui-valkey"
results = []
state = {}


def step(group, name):
    """Run the decorated function as one check; a failed assert or an exception fails it."""

    def run(fn):
        started = time.time()
        try:
            fn()
            status, detail = "PASS", ""
        except BaseException as e:  # noqa: BLE001 - every failure is a result, not a crash
            status, detail = "FAIL", f"{type(e).__name__}: {e}"
        results.append(
            {
                "check": f"{group}: {name}",
                "status": status,
                "detail": detail,
                "ms": (time.time() - started) * 1000,
            }
        )
        print(f"{status}  {group}: {name}  {detail[:300]}", flush=True)
        return fn

    return run


def eventually(fn, what, seconds=20):
    """fn() until it returns something true: an index links what it is given a moment later."""
    deadline = time.time() + seconds
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(0.25)
    raise AssertionError(f"{what}: still {last!r} after {seconds}s")


DIM = 8


def unit(i):
    """A vector along axis i (a distinct direction for each of the first DIM)."""
    v = [0.0] * DIM
    v[i % DIM] = 1.0
    return v


def item(i, file_id, text=None):
    return {
        "id": f"doc-{i}",
        "text": text or f"text of document {i}",
        "vector": unit(i),
        "metadata": {"file_id": file_id, "source": f"src-{i}", "hash": f"h{i}", "name": f"n{i}"},
    }


COLLECTION = "file-abc"
ITEMS = [item(0, "f1"), item(1, "f1"), item(2, "f2"), item(3, "f2"), item(4, "f3")]


@step("init", "ValkeyClient() passes its version and module checks")
def _():
    from open_webui.retrieval.vector.dbs.valkey import ValkeyClient

    state["client"] = ValkeyClient()



def need_client():
    if state.get("client") is None:
        raise AssertionError("ValkeyClient() did not start")
    return state["client"]


@step("collection", "has_collection is False for an index that does not exist")
def _():
    assert need_client().has_collection(COLLECTION) is False


@step("collection", "insert creates the index and stores the items")
def _():
    c = need_client()
    c.insert(COLLECTION, ITEMS)
    assert c.has_collection(COLLECTION) is True


@step("collection", "has_collection is True once the index exists")
def _():
    assert need_client().has_collection(COLLECTION) is True


@step("search", "nearest neighbour of a stored vector is that document")
def _():
    c = need_client()

    def nearest():
        r = c.search(COLLECTION, [unit(2)], limit=3)
        return r if r and r.ids and r.ids[0] and r.ids[0][0] == "doc-2" else None

    r = eventually(nearest, "search for doc-2")
    assert len(r.ids[0]) == 3, r.ids
    assert r.documents[0][0] == "text of document 2", r.documents
    assert r.metadatas[0][0]["file_id"] == "f2", r.metadatas
    d = r.distances[0]
    assert d[0] >= d[1] >= d[2], d
    assert abs(d[0] - 1.0) < 1e-3, d


@step("search", "filtered search keeps to the documents of one file")
def _():
    c = need_client()

    def only_f1():
        r = c.search(COLLECTION, [unit(0)], filter={"file_id": "f1"}, limit=5)
        return r if r and r.ids and r.ids[0] else None

    r = eventually(only_f1, "filtered search")
    assert sorted(r.ids[0]) == ["doc-0", "doc-1"], r.ids
    assert r.ids[0][0] == "doc-0", r.ids


@step("search", "filtered search with $in and $ne")
def _():
    c = need_client()
    r = c.search(COLLECTION, [unit(4)], filter={"file_id": {"$in": ["f2", "f3"]}}, limit=5)
    assert r and sorted(r.ids[0]) == ["doc-2", "doc-3", "doc-4"], r
    r = c.search(COLLECTION, [unit(4)], filter={"file_id": {"$ne": "f3"}}, limit=5)
    assert r and sorted(r.ids[0]) == ["doc-0", "doc-1", "doc-2", "doc-3"], r


@step("search", "search of a collection that does not exist is None")
def _():
    assert need_client().search("no-such-collection", [unit(0)]) is None


@step("query", "query by metadata filter returns the documents and their metadata")
def _():
    c = need_client()
    r = c.query(COLLECTION, {"file_id": "f2"}, limit=10)
    assert r is not None, "query returned None"
    assert sorted(r.ids[0]) == ["doc-2", "doc-3"], r.ids
    assert sorted(r.documents[0]) == ["text of document 2", "text of document 3"], r.documents
    assert all(m["file_id"] == "f2" for m in r.metadatas[0]), r.metadatas


@step("get", "get returns every document of the collection")
def _():
    r = need_client().get(COLLECTION)
    assert r is not None, "get returned None"
    assert sorted(r.ids[0]) == [f"doc-{i}" for i in range(5)], r.ids
    assert len(r.documents[0]) == 5 and len(r.metadatas[0]) == 5


@step("upsert", "upsert replaces a document's text")
def _():
    c = need_client()
    c.upsert(COLLECTION, [item(1, "f1", text="new text of document 1")])
    r = c.query(COLLECTION, {"file_id": "f1"}, limit=10)
    assert r and dict(zip(r.ids[0], r.documents[0])).get("doc-1") == "new text of document 1", r


@step("delete", "delete by ids removes those documents")
def _():
    c = need_client()
    c.delete(COLLECTION, ids=["doc-4"])
    r = c.get(COLLECTION)
    assert r and "doc-4" not in r.ids[0] and len(r.ids[0]) == 4, r.ids


@step("delete", "delete by filter removes the matching documents")
def _():
    c = need_client()
    c.delete(COLLECTION, filter={"file_id": "f2"})
    r = c.get(COLLECTION)
    assert r and sorted(r.ids[0]) == ["doc-0", "doc-1"], r.ids
    q = c.query(COLLECTION, {"file_id": "f2"}, limit=10)
    assert q is not None and q.ids == [[]], q


@step("delete", "a deleted document is no longer found by search")
def _():
    c = need_client()

    def gone():
        r = c.search(COLLECTION, [unit(2)], limit=5)
        return r is not None and "doc-2" not in r.ids[0] and "doc-3" not in r.ids[0]

    eventually(gone, "search after delete")


@step("collection", "inserting vectors of another dimension is refused")
def _():
    c = need_client()
    try:
        c.insert(COLLECTION, [{"id": "x", "text": "t", "vector": [1.0, 0.0], "metadata": {}}])
    except ValueError:
        return
    raise AssertionError("no ValueError for a dimension of 2 in an index of 8")


@step("collection", "delete_collection drops the index and its documents")
def _():
    c = need_client()
    c.delete_collection(COLLECTION)
    assert c.has_collection(COLLECTION) is False
    assert c.get(COLLECTION) is None


@step("collection", "reset drops every collection of the prefix")
def _():
    c = need_client()
    c.insert("file-one", [item(0, "f1")])
    c.insert("file-two", [item(1, "f2")])
    assert c.has_collection("file-one") and c.has_collection("file-two")
    c.reset()
    assert c.has_collection("file-one") is False
    assert c.has_collection("file-two") is False


try:
    if state.get("client") is not None:
        state["client"].close()
except Exception:  # noqa: BLE001
    pass

compat_junit.write_junit(out, SUITE, results, sum(r["ms"] for r in results) / 1000)
failed = [r for r in results if r["status"] != "PASS"]
print(f"{len(results) - len(failed)} of {len(results)} checks pass")
sys.exit(0)
