"""
Vector search tests — FT.CREATE/FT.SEARCH (KNN)/FT.AGGREGATE/FT.INFO/FT.DROPINDEX
and Vector Set commands VADD/VSIM/VCARD/VREM/VDIM.

All FT.* commands are issued via execute_command against the rb (bytes) fixture.
Results come back as bytes; every helper decodes explicitly.

IMPORTANT: FT.SEARCH RETURN must only request text/numeric fields, never the
vector field itself — the server returns raw FLOAT32 bytes which are not valid
UTF-8 and will raise UnicodeDecodeError.
"""
import math
import os
import time
import pytest
import redis
from utils.helpers import vec_to_bytes

_IS_REDIS = any(name in os.path.basename(os.environ.get("KIVI_BINARY", ""))
                for name in ("redis-stack-server", "redis-server"))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _decode(v):
    """Decode bytes to str; pass through if already str."""
    return v.decode() if isinstance(v, bytes) else v


def _ft_search_knn(client, index, query_vec, k, field="embedding", extra_params=None):
    """
    Issue FT.SEARCH KNN and return (total: int, results: list[tuple[str, dict[str,str]]]).

    Only requests RETURN 1 score — never the vector field, because the server
    echoes raw FLOAT32 bytes back which are not valid UTF-8.
    """
    vec_bytes = vec_to_bytes(query_vec)
    cmd = [
        "FT.SEARCH", index,
        f"*=>[KNN {k} @{field} $vec AS score]",
        "PARAMS", "2", "vec", vec_bytes,
        "RETURN", "1", "score",   # ← only score, never the vector field
        "DIALECT", "2",
    ]
    if extra_params:
        cmd.extend(extra_params)
    raw = client.execute_command(*cmd)
    total = raw[0]
    results = []
    i = 1
    while i < len(raw):
        key = _decode(raw[i])
        fields_flat = raw[i + 1]
        i += 2
        fields = {}
        for j in range(0, len(fields_flat), 2):
            fields[_decode(fields_flat[j])] = _decode(fields_flat[j + 1])
        results.append((key, fields))
    return total, results


def _ft_search_hybrid(client, index, filter_expr, query_vec, k,
                      return_fields=None, field="embedding"):
    """
    Hybrid search with a pre-filter expression.
    return_fields: list of field names to RETURN (must be non-vector fields).
    Defaults to returning only score.
    """
    if return_fields is None:
        return_fields = ["score"]
    vec_bytes = vec_to_bytes(query_vec)
    return_clause = ["RETURN", str(len(return_fields))] + return_fields
    cmd = [
        "FT.SEARCH", index,
        f"({filter_expr})=>[KNN {k} @{field} $vec AS score]",
        "PARAMS", "2", "vec", vec_bytes,
    ] + return_clause + ["DIALECT", "2"]
    raw = client.execute_command(*cmd)
    total = raw[0]
    results = []
    i = 1
    while i < len(raw):
        key = _decode(raw[i])
        fields_flat = raw[i + 1]
        i += 2
        fields = {_decode(fields_flat[j]): _decode(fields_flat[j + 1])
                  for j in range(0, len(fields_flat), 2)}
        results.append((key, fields))
    return total, results


# ──────────────────────────────────────────────────────────────────────────────
# FLAT index
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def flat_index(rb):
    rb.execute_command(
        "FT.CREATE", "idx_flat",
        "ON", "HASH",
        "PREFIX", "1", "doc:",
        "SCHEMA", "embedding", "VECTOR", "FLAT", "6",
        "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE",
    )
    yield rb
    try:
        rb.execute_command("FT.DROPINDEX", "idx_flat")
    except Exception:
        pass


class TestFlatIndex:
    def test_create_and_info(self, flat_index):
        r = flat_index
        raw = r.execute_command("FT.INFO", "idx_flat")
        info = {_decode(raw[i]): raw[i + 1] for i in range(0, len(raw), 2)}
        assert _decode(info.get("index_name", b"")) == "idx_flat"

    def test_knn_returns_nearest(self, flat_index):
        r = flat_index
        r.hset(b"doc:a", mapping={b"embedding": vec_to_bytes([1.0, 0.0, 0.0, 0.0])})
        r.hset(b"doc:b", mapping={b"embedding": vec_to_bytes([0.0, 1.0, 0.0, 0.0])})
        r.hset(b"doc:c", mapping={b"embedding": vec_to_bytes([0.9, 0.1, 0.0, 0.0])})
        time.sleep(0.05)

        _, results = _ft_search_knn(r, "idx_flat", [1.0, 0.0, 0.0, 0.0], k=2)
        keys = [res[0] for res in results]
        assert "doc:a" in keys
        assert "doc:c" in keys

    def test_knn_score_ordering(self, flat_index):
        r = flat_index
        r.hset(b"doc:near", mapping={b"embedding": vec_to_bytes([1.0, 0.0, 0.0, 0.0])})
        r.hset(b"doc:far",  mapping={b"embedding": vec_to_bytes([0.0, 0.0, 0.0, 1.0])})
        time.sleep(0.05)

        _, results = _ft_search_knn(r, "idx_flat", [1.0, 0.0, 0.0, 0.0], k=2)
        scores = [float(res[1]["score"]) for res in results]
        assert scores == sorted(scores)  # ascending distance

    def test_k_larger_than_dataset(self, flat_index):
        r = flat_index
        r.hset(b"doc:only", mapping={b"embedding": vec_to_bytes([1.0, 0.0, 0.0, 0.0])})
        time.sleep(0.05)

        _, results = _ft_search_knn(r, "idx_flat", [1.0, 0.0, 0.0, 0.0], k=100)
        assert len(results) == 1

    def test_ft_list(self, flat_index):
        r = flat_index
        indexes = [_decode(x) for x in r.execute_command("FT._LIST")]
        assert "idx_flat" in indexes

    def test_ft_dropindex(self, rb):
        rb.execute_command(
            "FT.CREATE", "drop_me",
            "ON", "HASH", "PREFIX", "1", "x:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
        )
        rb.execute_command("FT.DROPINDEX", "drop_me")
        indexes = [_decode(x) for x in rb.execute_command("FT._LIST")]
        assert "drop_me" not in indexes


# ──────────────────────────────────────────────────────────────────────────────
# HNSW index
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def hnsw_index(rb):
    rb.execute_command(
        "FT.CREATE", "idx_hnsw",
        "ON", "HASH",
        "PREFIX", "1", "hnsw:",
        "SCHEMA", "vec", "VECTOR", "HNSW", "12",
        "TYPE", "FLOAT32", "DIM", "8",
        "DISTANCE_METRIC", "L2",
        "M", "16",
        "EF_CONSTRUCTION", "200",
        "EF_RUNTIME", "10"
    )
    yield rb
    try:
        rb.execute_command("FT.DROPINDEX", "idx_hnsw")
    except Exception:
        pass


class TestHnswIndex:
    def test_hnsw_approximate_knn(self, hnsw_index):
        r = hnsw_index
        import random
        random.seed(42)
        # Build a well-connected graph: insert 200 random vectors first
        # so the graph has density before inserting the target
        for i in range(200):
            vec = [random.gauss(0, 1) for _ in range(8)]
            r.hset(f"hnsw:item{i}".encode(), mapping={b"vec": vec_to_bytes(vec)})
        # Insert the target vector into an already-connected graph
        exact_vec = [1.0] * 8
        r.hset(b"hnsw:close", mapping={b"vec": vec_to_bytes(exact_vec)})
        time.sleep(0.15)

        _, results = _ft_search_knn(r, "idx_hnsw", exact_vec, k=5, field="vec")
        keys = [res[0] for res in results]
        assert "hnsw:close" in keys

    def test_hnsw_incremental_insert(self, hnsw_index):
        r = hnsw_index
        for i in range(10):
            r.hset(f"hnsw:v{i}".encode(), mapping={b"vec": vec_to_bytes([float(i)] * 8)})
        time.sleep(0.1)

        _, results = _ft_search_knn(r, "idx_hnsw", [0.0] * 8, k=3, field="vec")
        keys = [res[0] for res in results]
        assert "hnsw:v0" in keys

    def test_flushall_drops_hnsw_index(self, hnsw_index):
        r = hnsw_index
        for i in range(20):
            r.hset(f"hnsw:v{i}".encode(), mapping={b"vec": vec_to_bytes([float(i)] * 8)})
        time.sleep(0.05)
        assert r.dbsize() >= 20
        r.flushall()
        assert r.dbsize() == 0
        indexes = [_decode(x) for x in r.execute_command("FT._LIST")]
        assert "idx_hnsw" not in indexes
        with pytest.raises(redis.ResponseError):
            r.execute_command("FT.INFO", "idx_hnsw")


# ──────────────────────────────────────────────────────────────────────────────
# Hybrid search
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def hybrid_index(rb):
    rb.execute_command(
        "FT.CREATE", "idx_hybrid",
        "ON", "HASH",
        "PREFIX", "1", "prod:",
        "SCHEMA",
        "category", "TAG",
        "price", "NUMERIC",
        "embedding", "VECTOR", "FLAT", "6",
        "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE",
    )
    yield rb
    try:
        rb.execute_command("FT.DROPINDEX", "idx_hybrid")
    except Exception:
        pass


class TestHybridSearch:
    def test_tag_filter_plus_knn(self, hybrid_index):
        r = hybrid_index
        products = [
            (b"prod:1", "electronics", 99.9,  [1.0, 0.0, 0.0, 0.0]),
            (b"prod:2", "electronics", 149.9, [0.9, 0.1, 0.0, 0.0]),
            (b"prod:3", "clothing",    49.9,  [0.1, 0.9, 0.0, 0.0]),
            (b"prod:4", "electronics", 299.9, [0.8, 0.2, 0.0, 0.0]),
        ]
        for key, cat, price, vec in products:
            r.hset(key, mapping={
                b"category":  cat.encode(),
                b"price":     str(price).encode(),
                b"embedding": vec_to_bytes(vec),
            })
        time.sleep(0.05)

        # Return category (a text field) so we can verify the filter
        total, results = _ft_search_hybrid(
            r, "idx_hybrid",
            "@category:{electronics}",
            [1.0, 0.0, 0.0, 0.0], k=3,
            return_fields=["score", "category"],
        )
        assert total == 3
        for key, fields in results:
            assert fields.get("category") == "electronics"

    def test_numeric_range_filter_plus_knn(self, hybrid_index):
        r = hybrid_index
        items = [
            (b"prod:a", "electronics",  50.0, [1.0, 0.0, 0.0, 0.0]),
            (b"prod:b", "electronics", 150.0, [0.9, 0.1, 0.0, 0.0]),
            (b"prod:c", "electronics", 250.0, [0.95, 0.05, 0.0, 0.0]),
        ]
        for key, cat, price, vec in items:
            r.hset(key, mapping={
                b"category":  cat.encode(),
                b"price":     str(price).encode(),
                b"embedding": vec_to_bytes(vec),
            })
        time.sleep(0.05)

        total, _ = _ft_search_hybrid(
            r, "idx_hybrid",
            "@price:[0 100]",
            [1.0, 0.0, 0.0, 0.0], k=5,
        )
        assert total == 1  # only prod:a has price ≤ 100


# ──────────────────────────────────────────────────────────────────────────────
# FT.AGGREGATE
# ──────────────────────────────────────────────────────────────────────────────

class TestFtAggregate:
    def test_aggregate_count(self, hybrid_index):
        r = hybrid_index
        for i in range(5):
            r.hset(f"prod:ag{i}".encode(), mapping={
                b"category":  b"electronics",
                b"price":     str(i * 10).encode(),
                b"embedding": vec_to_bytes([1.0, 0.0, 0.0, 0.0]),
            })
        time.sleep(0.05)
        raw = r.execute_command(
            "FT.AGGREGATE", "idx_hybrid", "*",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "cnt",
        )
        assert raw[0] >= 1


# ──────────────────────────────────────────────────────────────────────────────
# FT.ALIAS
# ──────────────────────────────────────────────────────────────────────────────

class TestFtAlias:
    def test_alias_lifecycle(self, rb):
        rb.execute_command(
            "FT.CREATE", "alias_base_idx",
            "ON", "HASH", "PREFIX", "1", "ab:",
            "SCHEMA", "v", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
        )
        try:
            rb.execute_command("FT.ALIASADD", "myalias2", "alias_base_idx")
            indexes = [_decode(x) for x in rb.execute_command("FT._LIST")]
            assert "alias_base_idx" in indexes

            rb.execute_command("FT.ALIASUPDATE", "myalias2", "alias_base_idx")
            rb.execute_command("FT.ALIASDEL", "myalias2")
        finally:
            try:
                rb.execute_command("FT.ALIASDEL", "myalias2")
            except Exception:
                pass
            try:
                rb.execute_command("FT.DROPINDEX", "alias_base_idx")
            except Exception:
                pass


# ──────────────────────────────────────────────────────────────────────────────
# Vector Set commands (V*)
# ──────────────────────────────────────────────────────────────────────────────

class TestVectorSet:
    @pytest.fixture(autouse=True)
    def _require_vadd(self, r):
        try:
            r.execute_command("VADD", "__probe__", "VALUES", "1", "0.0", "__elem__")
        except redis.ResponseError as e:
            if "unknown command" in str(e).lower() or "unknown subcommand" in str(e).lower():
                pytest.skip("VADD not supported by this server")

    def test_vadd_vsim(self, rb):
        r = rb
        r.execute_command("VADD", "vs", "VALUES", "4", "1.0", "0.0", "0.0", "0.0", "elem_a")
        r.execute_command("VADD", "vs", "VALUES", "4", "0.9", "0.1", "0.0", "0.0", "elem_b")
        r.execute_command("VADD", "vs", "VALUES", "4", "0.0", "0.0", "0.0", "1.0", "elem_c")

        results = r.execute_command(
            "VSIM", "vs", "VALUES", "4", "1.0", "0.0", "0.0", "0.0", "COUNT", "2"
        )
        decoded = [_decode(x) for x in results]
        assert "elem_a" in decoded

    def test_vcard(self, rb):
        r = rb
        r.execute_command("VADD", "vs2", "VALUES", "2", "1.0", "0.0", "item1")
        r.execute_command("VADD", "vs2", "VALUES", "2", "0.0", "1.0", "item2")
        assert r.execute_command("VCARD", "vs2") == 2

    def test_vrem(self, rb):
        r = rb
        r.execute_command("VADD", "vs3", "VALUES", "2", "1.0", "0.0", "item1")
        r.execute_command("VADD", "vs3", "VALUES", "2", "0.0", "1.0", "item2")
        assert r.execute_command("VREM", "vs3", "item1") == 1
        assert r.execute_command("VCARD", "vs3") == 1

    def test_vdim(self, rb):
        r = rb
        r.execute_command("VADD", "vs5", "VALUES", "4", "1.0", "0.0", "0.0", "0.0", "v")
        assert r.execute_command("VDIM", "vs5") == 4

    def test_vsim_withscores(self, rb):
        r = rb
        r.execute_command("VADD", "vs6", "VALUES", "2", "1.0", "0.0", "a")
        r.execute_command("VADD", "vs6", "VALUES", "2", "0.0", "1.0", "b")
        results = r.execute_command(
            "VSIM", "vs6", "VALUES", "2", "1.0", "0.0",
            "COUNT", "2", "WITHSCORES"
        )
        assert len(results) >= 2

    def test_type_is_vectorset(self, rb):
        r = rb
        r.execute_command("VADD", "vs_type", "VALUES", "2", "1.0", "0.0", "v")
        t = _decode(r.execute_command("TYPE", "vs_type"))
        assert t == "vectorset"


# ──────────────────────────────────────────────────────────────────────────────
# Semantic similarity end-to-end scenario
# ──────────────────────────────────────────────────────────────────────────────

class TestSemanticSearch:
    def test_document_similarity(self, rb):
        """
        Axis-aligned clusters ensure the nearest-neighbour query reliably
        returns the correct cluster without depending on random seeds.
        """
        idx = "docs_sem_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH",
            "PREFIX", "1", "document:",
            "SCHEMA", "content_vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "8", "DISTANCE_METRIC", "COSINE",
        )
        try:
            def norm(v):
                mag = math.sqrt(sum(x * x for x in v))
                return [x / mag for x in v] if mag else v

            # Tech lives in dims 0-3, sports in dims 4-7 — orthogonal clusters
            docs = {
                b"document:tech1":   norm([1.0, 0.9, 0.8, 0.7, 0.0, 0.0, 0.0, 0.0]),
                b"document:tech2":   norm([0.9, 1.0, 0.8, 0.7, 0.0, 0.0, 0.0, 0.0]),
                b"document:sports1": norm([0.0, 0.0, 0.0, 0.0, 1.0, 0.9, 0.8, 0.7]),
                b"document:sports2": norm([0.0, 0.0, 0.0, 0.0, 0.9, 1.0, 0.8, 0.7]),
                b"document:cook1":   norm([0.0, 0.0, 0.5, 0.0, 0.0, 0.0, 0.5, 1.0]),
            }
            for key, vec in docs.items():
                rb.hset(key, mapping={b"content_vec": vec_to_bytes(vec)})
            time.sleep(0.05)

            query = norm([1.0, 0.9, 0.8, 0.7, 0.0, 0.0, 0.0, 0.0])
            _, results = _ft_search_knn(rb, idx, query, k=2, field="content_vec")
            keys = [res[0] for res in results]
            assert all("tech" in k for k in keys), f"Expected tech docs, got {keys}"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

# ──────────────────────────────────────────────────────────────────────────────
# FT.HYBRID — Redis 8.4 hybrid search command
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def ft_hybrid_index(rb):
    """Index with TEXT, TAG, NUMERIC, and a single VECTOR field for FT.HYBRID."""
    rb.execute_command(
        "FT.CREATE", "h_idx",
        "ON", "HASH",
        "PREFIX", "1", "h:",
        "SCHEMA",
        "title",    "TEXT",
        "category", "TAG",
        "price",    "NUMERIC",
        "v",        "VECTOR", "FLAT", "6",
        "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE",
    )
    yield rb
    try:
        rb.execute_command("FT.DROPINDEX", "h_idx")
    except Exception:
        pass


def _parse_hybrid(raw):
    """
    FT.HYBRID returns [count, key1, [fields1], key2, [fields2], ...]
    after fusion (no GROUPBY). Returns (count, [(key, fields_dict), ...]).
    """
    count = raw[0]
    rows = []
    i = 1
    while i + 1 < len(raw):
        key = _decode(raw[i])
        flat = raw[i + 1]
        fields = {}
        for j in range(0, len(flat), 2):
            fields[_decode(flat[j])] = _decode(flat[j + 1])
        rows.append((key, fields))
        i += 2
    return count, rows


def _parse_hybrid_grouped(raw):
    """
    FT.HYBRID with a GROUPBY tail emits [count, [fields1], [fields2], ...]
    — flat field arrays without per-row keys.
    """
    count = raw[0]
    rows = []
    for entry in raw[1:]:
        flat = entry
        fields = {}
        for j in range(0, len(flat), 2):
            fields[_decode(flat[j])] = _decode(flat[j + 1])
        rows.append(fields)
    return count, rows


class TestFtHybrid:
    """End-to-end coverage of FT.HYBRID — RRF, LINEAR, the full pipeline tail."""

    @pytest.fixture(autouse=True)
    def _require_ft_hybrid(self, r):
        try:
            r.execute_command("FT.HYBRID", "__probe__", "*", "KNN", "1", "@__v__", "$v")
        except redis.ResponseError as e:
            if "unknown command" in str(e).lower() or "unknown subcommand" in str(e).lower():
                pytest.skip("FT.HYBRID not supported by this server")

    def test_rrf_default(self, ft_hybrid_index):
        r = ft_hybrid_index
        v_match = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        v_far   = vec_to_bytes([0.0, 0.0, 1.0, 0.0])

        r.hset(b"h:1", mapping={
            b"title": b"rust programming", b"category": b"tech",
            b"price": b"30", b"v": v_match,
        })
        r.hset(b"h:2", mapping={
            b"title": b"python tutorial", b"category": b"tech",
            b"price": b"20", b"v": v_far,
        })
        r.hset(b"h:3", mapping={
            b"title": b"rust deep dive", b"category": b"tech",
            b"price": b"50", b"v": v_match,
        })
        time.sleep(0.05)

        q = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "PARAMS", "2", "q", q,
        )
        count, rows = _parse_hybrid(raw)
        assert count >= 2
        # h:1 and h:3 matched both text and vector — they must be top two
        top_keys = {rows[0][0], rows[1][0]}
        assert top_keys == {"h:1", "h:3"}

        # Default field names are present
        for _, fields in rows[:2]:
            assert "hybrid_score" in fields
            assert "text_score"   in fields
            assert "vsim_score"   in fields

    def test_linear_alpha_text_heavy(self, ft_hybrid_index):
        r = ft_hybrid_index
        v_close = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        v_med   = vec_to_bytes([0.7, 0.7, 0.0, 0.0])

        r.hset(b"h:text", mapping={
            b"title": b"rust programming", b"category": b"tech",
            b"price": b"10", b"v": v_med,
        })
        r.hset(b"h:vec",  mapping={
            b"title": b"unrelated content", b"category": b"tech",
            b"price": b"10", b"v": v_close,
        })
        time.sleep(0.05)

        q = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "COMBINE", "LINEAR", "4", "ALPHA", "0.9", "BETA", "0.1",
            "PARAMS", "2", "q", q,
        )
        count, rows = _parse_hybrid(raw)
        assert count >= 1
        # Text-heavy weighting: the text-matching doc must rank first
        assert rows[0][0] == "h:text"

    def test_linear_beta_vector_heavy(self, ft_hybrid_index):
        r = ft_hybrid_index
        v_close = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        v_med   = vec_to_bytes([0.7, 0.7, 0.0, 0.0])

        r.hset(b"h:text", mapping={
            b"title": b"rust programming", b"category": b"tech",
            b"price": b"10", b"v": v_med,
        })
        r.hset(b"h:vec",  mapping={
            b"title": b"unrelated content", b"category": b"tech",
            b"price": b"10", b"v": v_close,
        })
        time.sleep(0.05)

        q = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "COMBINE", "LINEAR", "4", "ALPHA", "0.1", "BETA", "0.9",
            "PARAMS", "2", "q", q,
        )
        count, rows = _parse_hybrid(raw)
        assert count >= 1
        # Vector-heavy weighting: the vector-closer doc must rank first
        assert rows[0][0] == "h:vec"

    def test_yield_score_as_renames_fields(self, ft_hybrid_index):
        r = ft_hybrid_index
        r.hset(b"h:1", mapping={
            b"title": b"rust", b"category": b"tech",
            b"price": b"10", b"v": vec_to_bytes([1.0, 0.0, 0.0, 0.0]),
        })
        time.sleep(0.05)

        q = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",   "YIELD_SCORE_AS", "my_text",
            "VSIM",   "@v", "$q",       "YIELD_SCORE_AS", "my_vec",
            "COMBINE", "RRF", "2",      "YIELD_SCORE_AS", "my_hybrid",
            "PARAMS", "2", "q", q,
        )
        count, rows = _parse_hybrid(raw)
        assert count == 1
        fields = rows[0][1]
        # Default names must NOT be present — only the user's chosen names
        assert "my_hybrid" in fields
        assert "my_text"   in fields
        assert "my_vec"    in fields
        assert "hybrid_score" not in fields
        assert "text_score"   not in fields
        assert "vsim_score"   not in fields

    def test_load_specific_fields(self, ft_hybrid_index):
        r = ft_hybrid_index
        r.hset(b"h:1", mapping={
            b"title": b"rust",
            b"category": b"tech",
            b"price": b"99",
            b"v": vec_to_bytes([1.0, 0.0, 0.0, 0.0]),
        })
        time.sleep(0.05)

        q = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "LOAD", "2", "@title", "@price",
            "PARAMS", "2", "q", q,
        )
        count, rows = _parse_hybrid(raw)
        assert count == 1
        fields = rows[0][1]
        assert fields["title"] == "rust"
        assert fields["price"] == "99"
        # category was not requested, must not appear
        assert "category" not in fields

    def test_load_star_returns_all_fields(self, ft_hybrid_index):
        r = ft_hybrid_index
        r.hset(b"h:1", mapping={
            b"title": b"rust",
            b"category": b"tech",
            b"price": b"99",
            b"v": vec_to_bytes([1.0, 0.0, 0.0, 0.0]),
        })
        time.sleep(0.05)

        q = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "LOAD", "*",
            "PARAMS", "2", "q", q,
        )
        count, rows = _parse_hybrid(raw)
        fields = rows[0][1]
        # All non-vector fields should be present.
        # The vector field 'v' itself returns raw bytes; LOAD * is allowed
        # to skip it for the same UTF-8 reason FT.SEARCH RETURN does, but
        # the non-vector fields must be present.
        assert fields.get("title") == "rust"
        assert fields.get("category") == "tech"
        assert fields.get("price") == "99"

    def test_sortby_text_score_desc(self, ft_hybrid_index):
        r = ft_hybrid_index
        v = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        r.hset(b"h:a", mapping={
            b"title": b"rust rust rust", b"category": b"tech",
            b"price": b"10", b"v": v,
        })
        r.hset(b"h:b", mapping={
            b"title": b"rust", b"category": b"tech",
            b"price": b"10", b"v": v,
        })
        time.sleep(0.05)

        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "SORTBY", "2", "text_score", "DESC",
            "PARAMS", "2", "q", v,
        )
        count, rows = _parse_hybrid(raw)
        assert count == 2
        assert rows[0][0] == "h:a"   # three "rust" tokens > one

    def test_limit_pagination(self, ft_hybrid_index):
        r = ft_hybrid_index
        v = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        for i in range(5):
            r.hset(f"h:{i}".encode(), mapping={
                b"title": b"rust", b"category": b"tech",
                b"price": str(i * 10).encode(), b"v": v,
            })
        time.sleep(0.05)

        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "LIMIT", "1", "2",
            "PARAMS", "2", "q", v,
        )
        count, rows = _parse_hybrid(raw)
        assert count == 2  # LIMIT bounds the returned rows

    def test_pipeline_groupby_reduce(self, ft_hybrid_index):
        r = ft_hybrid_index
        v = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        r.hset(b"h:1", mapping={
            b"title": b"rust", b"category": b"a",
            b"price": b"10",  b"v": v,
        })
        r.hset(b"h:2", mapping={
            b"title": b"rust", b"category": b"a",
            b"price": b"30",  b"v": v,
        })
        r.hset(b"h:3", mapping={
            b"title": b"rust", b"category": b"b",
            b"price": b"100", b"v": v,
        })
        time.sleep(0.05)

        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "LOAD", "2", "@category", "@price",
            "PARAMS", "2", "q", v,
            "GROUPBY", "1", "@category",
                "REDUCE", "COUNT", "0",      "AS", "cnt",
                "REDUCE", "AVG", "1", "@price", "AS", "avg_price",
        )
        count, rows = _parse_hybrid_grouped(raw)
        assert count == 2

        by_cat = {row["category"]: row for row in rows}
        assert int(by_cat["a"]["cnt"]) == 2
        assert abs(float(by_cat["a"]["avg_price"]) - 20.0) < 0.5
        assert int(by_cat["b"]["cnt"]) == 1
        assert abs(float(by_cat["b"]["avg_price"]) - 100.0) < 0.5

    def test_pipeline_apply_filter(self, ft_hybrid_index):
        r = ft_hybrid_index
        v = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
        for i, price in enumerate([50, 150, 250]):
            r.hset(f"h:{i}".encode(), mapping={
                b"title": b"rust", b"category": b"tech",
                b"price": str(price).encode(), b"v": v,
            })
        time.sleep(0.05)

        raw = r.execute_command(
            "FT.HYBRID", "h_idx",
            "SEARCH", "@title:rust",
            "VSIM", "@v", "$q",
            "LOAD", "1", "@price",
            "PARAMS", "2", "q", v,
            "APPLY", "@price * 1.1", "AS", "with_tax",
            "FILTER", "@price > 100",
        )
        count, rows = _parse_hybrid(raw)
        assert count == 2  # 150 and 250 survive
        for _, fields in rows:
            assert "with_tax" in fields
            price = float(fields["price"])
            assert price > 100
            assert abs(float(fields["with_tax"]) - price * 1.1) < 0.01


# ──────────────────────────────────────────────────────────────────────────────
# FT.ALTER — adding a second VECTOR field
# ──────────────────────────────────────────────────────────────────────────────

class TestFtAlterMultiVector:
    """FT.ALTER provisions a fresh live index for new VECTOR fields and backfills."""

    def test_alter_add_second_vector_field_with_backfill(self, rb):
        idx = "alter_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "av:",
            "SCHEMA",
            "v1", "VECTOR", "FLAT", "6",
                "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        try:
            v_match = vec_to_bytes([1.0, 0.0, 0.0, 0.0])
            v_other = vec_to_bytes([0.0, 1.0, 0.0, 0.0])

            # Pre-existing docs carry both v1 and v2 even though v2 is not in schema
            rb.hset(b"av:1", mapping={b"v1": v_match, b"v2": v_other})
            rb.hset(b"av:2", mapping={b"v1": v_other, b"v2": v_match})
            time.sleep(0.05)

            # Add v2 to schema — backfill should pick up existing values
            rb.execute_command(
                "FT.ALTER", idx, "SCHEMA", "ADD",
                "v2", "VECTOR", "FLAT", "6",
                    "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
            )
            time.sleep(0.05)

            # KNN on v2 with query [1,0,0,0] should rank av:2 first (its v2 matches)
            _, results = _ft_search_knn(rb, idx, [1.0, 0.0, 0.0, 0.0], k=2, field="v2")
            keys = [k for k, _ in results]
            assert keys[0] == "av:2", \
                f"v2 KNN should rank av:2 first, got {keys}"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

    def test_alter_three_vector_fields(self, rb):
        idx = "three_vec_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "tv:",
            "SCHEMA",
            "v1", "VECTOR", "FLAT", "6",
                "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        try:
            rb.execute_command(
                "FT.ALTER", idx, "SCHEMA", "ADD",
                "v2", "VECTOR", "HNSW", "10",
                    "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE",
                    "M", "16", "EF_CONSTRUCTION", "200",
            )
            rb.execute_command(
                "FT.ALTER", idx, "SCHEMA", "ADD",
                "v3", "VECTOR", "FLAT", "6",
                    "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
            )

            # Walk FT.INFO's flat key/value pairs to find the 'fields' nested array.
            # Each entry in that array is a string like "v1 (VECTOR FLAT(...) DIM=4 L2)"
            # — we just need to confirm v1, v2, v3 all appear.
            raw = rb.execute_command("FT.INFO", idx)
            info = {}
            i = 0
            while i + 1 < len(raw):
                k = _decode(raw[i]) if isinstance(raw[i], bytes) else raw[i]
                info[k] = raw[i + 1]
                i += 2

            # KiviDB uses "fields"; RediSearch uses "attributes"
            fields_arr = info.get("fields") or info.get("attributes") or []
            # attributes is a list of per-field flat arrays; flatten for string check
            flat = []
            for item in fields_arr:
                if isinstance(item, (list, tuple)):
                    flat.extend(_decode(x) for x in item if isinstance(x, (bytes, str)))
                else:
                    flat.append(_decode(item))
            joined = " ".join(flat)

            assert "v1" in joined, f"v1 missing in {joined!r}"
            assert "v2" in joined, f"v2 missing in {joined!r}"
            assert "v3" in joined, f"v3 missing in {joined!r}"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass


# ──────────────────────────────────────────────────────────────────────────────
# HNSW compaction — FT.DEBUG COMPACT and FT.INFO stats
# ──────────────────────────────────────────────────────────────────────────────

class TestHnswCompaction:
    """HNSW compaction lifecycle: tombstone tracking, manual trigger, stats."""

    @pytest.fixture(autouse=True)
    def _require_ft_debug_compact(self, rb):
        try:
            # Probe: FT.DEBUG COMPACT is a KiviDB-specific subcommand
            rb.execute_command("FT.DEBUG", "COMPACT", "__probe__", "__field__")
        except redis.ResponseError as e:
            if "unknown" in str(e).lower():
                pytest.skip("FT.DEBUG COMPACT not supported by this server")
            # Any other ResponseError (e.g. no such index) means the command exists
        except Exception:
            pass

    def _info_dict(self, raw):
        """Convert FT.INFO's flat key-value array into a dict (handles arrays)."""
        d = {}
        i = 0
        while i + 1 < len(raw):
            k = _decode(raw[i]) if isinstance(raw[i], bytes) else raw[i]
            d[k] = raw[i + 1]
            i += 2
        return d

    def test_tombstone_count_increments_on_delete(self, rb):
        idx = "comp_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "c:",
            "SCHEMA", "vec", "VECTOR", "HNSW", "10",
                "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
                "M", "16", "EF_CONSTRUCTION", "200",
        )
        try:
            for i in range(3):
                rb.hset(f"c:{i}".encode(), mapping={
                    b"vec": vec_to_bytes([float(i), 0.0, 0.0, 0.0]),
                })
            time.sleep(0.05)

            info = self._info_dict(rb.execute_command("FT.INFO", idx))
            assert _decode(info["hnsw_live_count"]) == "3"
            assert _decode(info["hnsw_tombstone_count"]) == "0"

            rb.delete(b"c:1")
            time.sleep(0.05)

            info2 = self._info_dict(rb.execute_command("FT.INFO", idx))
            assert _decode(info2["hnsw_live_count"]) == "2"
            assert _decode(info2["hnsw_tombstone_count"]) == "1"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

    def test_manual_compact_clears_tombstones(self, rb):
        idx = "comp_manual_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "cm:",
            "SCHEMA", "vec", "VECTOR", "HNSW", "10",
                "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
                "M", "16", "EF_CONSTRUCTION", "200",
        )
        try:
            for i in range(5):
                rb.hset(f"cm:{i}".encode(), mapping={
                    b"vec": vec_to_bytes([float(i), 0.0, 0.0, 0.0]),
                })
            time.sleep(0.05)
            rb.delete(b"cm:0")
            rb.delete(b"cm:1")
            rb.delete(b"cm:2")
            time.sleep(0.05)

            info = self._info_dict(rb.execute_command("FT.INFO", idx))
            assert int(_decode(info["hnsw_tombstone_count"])) >= 3

            # Force compaction — fields argument is required
            assert _decode(rb.execute_command(
                "FT.DEBUG", "COMPACT", idx, "vec"
            )) == "OK"
            time.sleep(0.3)

            info_after = self._info_dict(rb.execute_command("FT.INFO", idx))
            assert _decode(info_after["hnsw_tombstone_count"]) == "0"
            assert _decode(info_after["hnsw_live_count"]) == "2"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

    def test_ft_info_nested_hnsw_indexes_array(self, rb):
        """Each HNSW field gets its own entry in the canonical hnsw_indexes array."""
        idx = "comp_multi_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "cn:",
            "SCHEMA",
            "v1", "VECTOR", "HNSW", "10",
                "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
                "M", "16", "EF_CONSTRUCTION", "200",
            "v2", "VECTOR", "HNSW", "10",
                "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE",
                "M", "16", "EF_CONSTRUCTION", "200",
        )
        try:
            info = self._info_dict(rb.execute_command("FT.INFO", idx))
            assert "hnsw_indexes" in info
            entries = info["hnsw_indexes"]
            assert len(entries) == 2
            field_names = []
            for entry in entries:
                d = self._info_dict(entry)
                field_names.append(_decode(d["field"]))
            assert sorted(field_names) == ["v1", "v2"]
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

    def test_compact_unknown_field_errors(self, rb):
        idx = "comp_err_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "ce:",
            "SCHEMA", "vec", "VECTOR", "HNSW", "10",
                "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
                "M", "16", "EF_CONSTRUCTION", "200",
        )
        try:
            with pytest.raises(redis.ResponseError):
                rb.execute_command("FT.DEBUG", "COMPACT", idx, "nosuch_field")
            with pytest.raises(redis.ResponseError):
                rb.execute_command("FT.DEBUG", "COMPACT", "nosuch_idx", "vec")
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass


# ──────────────────────────────────────────────────────────────────────────────
# FT.SPELLCHECK — incremental vocabulary
# ──────────────────────────────────────────────────────────────────────────────

class TestFtSpellcheckIncremental:
    """The vocabulary is maintained on every indexed HSET — no shard-walk per query."""

    def test_new_term_appears_in_suggestions_immediately(self, rb):
        idx = "sp_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "sp:",
            "SCHEMA", "body", "TEXT",
        )
        try:
            rb.hset(b"sp:1", mapping={b"body": b"hello world"})
            time.sleep(0.02)

            raw = rb.execute_command(
                "FT.SPELLCHECK", idx, "helo", "DISTANCE", "1"
            )
            entry = raw[0]
            suggestions = entry[2]
            words = [_decode(s[1]) for s in suggestions]
            assert "hello" in words

            # Brand-new word added — should be queryable right away
            rb.hset(b"sp:2", mapping={b"body": b"zephyr"})
            time.sleep(0.02)

            raw2 = rb.execute_command(
                "FT.SPELLCHECK", idx, "zephir", "DISTANCE", "1"
            )
            entry2 = raw2[0]
            suggestions2 = entry2[2]
            words2 = [_decode(s[1]) for s in suggestions2]
            assert "zephyr" in words2
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

    def test_vocab_aggregates_across_text_fields(self, rb):
        idx = "sp_multi_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "spm:",
            "SCHEMA",
            "title", "TEXT",
            "body",  "TEXT",
            "tags",  "TEXT",
        )
        try:
            rb.hset(b"spm:1", mapping={
                b"title": b"alpha",
                b"body":  b"bravo charlie",
                b"tags":  b"delta",
            })
            time.sleep(0.02)

            raw = rb.execute_command(
                "FT.SPELLCHECK", idx, "delt", "DISTANCE", "1"
            )
            entry = raw[0]
            suggestions = entry[2]
            words = [_decode(s[1]) for s in suggestions]
            assert "delta" in words, \
                "vocab should aggregate across all TEXT fields"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass


# ──────────────────────────────────────────────────────────────────────────────
# FT.AGGREGATE APPLY — geodistance() and timefmt()
def _agg_first_row(raw) -> dict:
    """Parse the first result row from FT.AGGREGATE into a flat dict."""
    if len(raw) < 2:
        return {}
    row = raw[1]
    # RediSearch 2.10 with LOAD: raw[1] may be a list of rows [[k,v,...], ...]
    if isinstance(row, (list, tuple)) and row and isinstance(row[0], (list, tuple)):
        row = row[0]
    return {_decode(row[j]): _decode(row[j + 1]) for j in range(0, len(row), 2)}


# ──────────────────────────────────────────────────────────────────────────────

class TestAggregateExpressionFunctions:
    """The two new APPLY expression functions added in P1 #5."""

    def test_geodistance_sf_to_nyc(self, rb):
        if _IS_REDIS:
            pytest.skip("geodistance 4-arg form produces different results on RediSearch")
        idx = "geo_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "loc:",
            "SCHEMA",
            "name", "TEXT",
            "lat",  "NUMERIC",
            "lon",  "NUMERIC",
        )
        try:
            rb.hset(b"loc:sf", mapping={
                b"name": b"sf", b"lat": b"37.7749", b"lon": b"-122.4194",
            })
            time.sleep(0.02)

            # Distance from SF to NYC (40.7128, -74.0060) ≈ 4129 km
            raw = rb.execute_command(
                "FT.AGGREGATE", idx, "*",
                "LOAD", "2", "@lat", "@lon",
                "APPLY", "geodistance(@lat, @lon, 40.7128, -74.0060)", "AS", "dist",
            )
            d = _agg_first_row(raw)
            dist = float(d["dist"])
            assert 4_100_000 < dist < 4_160_000, \
                f"SF→NYC ~4129 km, got {dist:.0f} m"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

    def test_timefmt_default_iso(self, rb):
        idx = "time_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "ev:",
            "SCHEMA", "name", "TEXT", "ts", "NUMERIC",
        )
        try:
            rb.hset(b"ev:1", mapping={b"name": b"e", b"ts": b"1700000000"})
            time.sleep(0.02)

            raw = rb.execute_command(
                "FT.AGGREGATE", idx, "*",
                "LOAD", "1", "@ts",
                "APPLY", "timefmt(@ts)", "AS", "iso",
            )
            d = _agg_first_row(raw)
            assert d["iso"] == "2023-11-14T22:13:20Z"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

    def test_timefmt_custom_format(self, rb):
        idx = "time_fmt_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "tf:",
            "SCHEMA", "ts", "NUMERIC",
        )
        try:
            rb.hset(b"tf:1", mapping={b"ts": b"1700000000"})
            time.sleep(0.02)

            raw = rb.execute_command(
                "FT.AGGREGATE", idx, "*",
                "LOAD", "1", "@ts",
                "APPLY", 'timefmt(@ts, "%Y-%m-%d")', "AS", "date",
                "APPLY", 'timefmt(@ts, "%A")',       "AS", "weekday",
            )
            d = _agg_first_row(raw)
            assert d["date"] == "2023-11-14"
            assert d["weekday"] == "Tuesday"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass

    def test_geodistance_combined_with_filter_and_sortby(self, rb):
        """Realistic 'find nearby places' query — APPLY then FILTER then SORTBY."""
        if _IS_REDIS:
            pytest.skip("geodistance 4-arg form produces different results on RediSearch")
        idx = "near_idx"
        rb.execute_command(
            "FT.CREATE", idx,
            "ON", "HASH", "PREFIX", "1", "near:",
            "SCHEMA",
            "name", "TEXT",
            "lat",  "NUMERIC",
            "lon",  "NUMERIC",
        )
        try:
            # Three places at known distances from a reference point
            rb.hset(b"near:close",  mapping={
                b"name": b"close",  b"lat": b"40.7130", b"lon": b"-74.0058",
            })
            rb.hset(b"near:medium", mapping={
                b"name": b"medium", b"lat": b"40.7500", b"lon": b"-74.0000",
            })
            rb.hset(b"near:far",    mapping={
                b"name": b"far",    b"lat": b"41.5000", b"lon": b"-72.0000",
            })
            time.sleep(0.05)

            # All three within 200 km — only close + medium within 10 km
            raw = rb.execute_command(
                "FT.AGGREGATE", idx, "*",
                "LOAD", "3", "@name", "@lat", "@lon",
                "APPLY", "geodistance(@lat, @lon, 40.7128, -74.0060)", "AS", "dist_m",
                "FILTER", "@dist_m < 10000",
                "SORTBY", "2", "@dist_m", "ASC",
            )
            count = raw[0]
            assert count == 2

            # First row is the closest
            d = _agg_first_row(raw)
            assert d["name"] == "close"
        finally:
            try:
                rb.execute_command("FT.DROPINDEX", idx)
            except Exception:
                pass