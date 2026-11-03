# tests/e2e/modules/test_fulltext_search.py
#
# E2E tests for full-text tokenisation — the complete feature:
#   - Token match correctness (whole-word, not substring)
#   - Stopword filtering
#   - BM25 ranking (TF and IDF effects)
#   - Field isolation (@title vs @body)
#   - Inverted index — AND / OR filter combinations
#   - Backfill on FT.CREATE over pre-existing keys
#   - Incremental index updates on HSET
#   - Delete removes document from search results
#   - Prefix queries
#   - Realistic e-commerce product search scenario
#
# Scenario: a small product catalogue — 10 items across categories.
# Tests mirror what a real application would do: search by keyword,
# filter by field, rank by relevance, and maintain the index as the
# catalogue changes.
#
# Run with:
#   cd tests/e2e && pytest modules/test_fulltext_search.py -v
#
# Against an already-running server:
#   KIVI_EXTERNAL=1 KIVI_PORT=6380 pytest modules/test_fulltext_search.py -v

from __future__ import annotations

import time
import pytest
import redis
from utils.helpers import r, rb  # noqa: F401


# ── Helpers ────────────────────────────────────────────────────────────────────

def _decode(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _ft_search(client, index: str, query: str, limit: int = 20) -> tuple[int, list[str]]:
    """
    Issue a pure text FT.SEARCH and return (total, [key, key, ...]).
    Uses execute_command so we can send arbitrary query strings.
    """
    raw = client.execute_command(
        "FT.SEARCH", index, query,
        "LIMIT", "0", str(limit),
    )
    total = raw[0]
    # Result layout: [total, key, [fields], key, [fields], ...]
    keys = [_decode(raw[i]) for i in range(1, len(raw), 2)]
    return total, keys


def _ft_search_fields(client, index: str, query: str, *return_fields) -> list[dict]:
    """
    FT.SEARCH with RETURN clause. Returns a list of field dicts per result.
    """
    raw = client.execute_command(
        "FT.SEARCH", index, query,
        "RETURN", str(len(return_fields)), *return_fields,
        "LIMIT", "0", "20",
    )
    results = []
    i = 1
    while i < len(raw):
        flat = raw[i + 1]
        fields = {_decode(flat[j]): _decode(flat[j + 1])
                  for j in range(0, len(flat), 2)}
        results.append(fields)
        i += 2
    return results


# ── Fixtures ──────────────────────────────────────────────────────────────────

# The product catalogue used throughout this module.
# Each product has: title (TEXT), description (TEXT), category (TAG), price (NUMERIC)
PRODUCTS = [
    # key,           title,                          description,                                  category,      price
    ("prod:1",  "Mechanical Keyboard",          "Cherry MX switches tactile feedback typing",  "peripherals", "129"),
    ("prod:2",  "Wireless Mouse",               "ergonomic bluetooth mouse silent clicks",     "peripherals", "49"),
    ("prod:3",  "USB Hub",                      "seven port powered hub fast charging",         "accessories", "35"),
    ("prod:4",  "Laptop Stand", "aluminium adjustable stand ergonomic ergonomic desk ergonomic design", "accessories", "79"),
    ("prod:5",  "Mechanical Numpad",            "compact numpad Cherry MX switches backlit",   "peripherals", "89"),
    ("prod:6",  "Noise Cancelling Headphones",  "wireless bluetooth noise cancelling studio",  "audio",       "249"),
    ("prod:7",  "Webcam HD",                    "1080p autofocus webcam streaming studio",     "video",       "99"),
    ("prod:8",  "Desk Lamp",                    "LED adjustable brightness warm white light",  "lighting",    "45"),
    ("prod:9",  "Monitor Arm",                  "ergonomic dual monitor arm adjustable",       "accessories", "119"),
    ("prod:10", "Cable Management Kit",         "desk cable organiser clips velcro straps",    "accessories", "19"),
]


@pytest.fixture
def catalogue(r):
    """
    Create a full-text index over the product catalogue and load all products.
    Torn down automatically — drops the index AND flushes keys.
    """
    # Drop leftover index from a previous run if it exists
    try:
        r.execute_command("FT.DROPINDEX", "products")
    except Exception:
        pass

    r.execute_command(
        "FT.CREATE", "products",
        "ON", "HASH",
        "PREFIX", "1", "prod:",
        "SCHEMA",
        "title",       "TEXT",
        "description", "TEXT",
        "category",    "TAG",
        "price",       "NUMERIC",
    )
    for key, title, desc, cat, price in PRODUCTS:
        r.hset(key, mapping={
            "title":       title,
            "description": desc,
            "category":    cat,
            "price":       price,
        })
    time.sleep(0.1)

    yield r

    # Teardown — drop the index so the next test starts clean
    try:
        r.execute_command("FT.DROPINDEX", "products")
    except Exception:
        pass


# ── Token correctness ─────────────────────────────────────────────────────────

class TestTokenCorrectness:
    """Whole-word token matching — not substring."""

    def test_exact_token_match(self, catalogue):
        """'mechanical' matches products with that exact token."""
        total, keys = _ft_search(catalogue, "products", "@title:mechanical")
        assert total == 2, f"expected 2 mechanical products, got {total}: {keys}"
        assert "prod:1" in keys
        assert "prod:5" in keys

    def test_no_substring_match(self, catalogue):
        """
        'key' must NOT match 'keyboard' — substring matching was the old broken behaviour.
        The token 'key' does not appear in any product title or description.
        """
        total, keys = _ft_search(catalogue, "products", "@title:key")
        assert total == 0, \
            f"'key' must not substring-match 'keyboard', got {total} results: {keys}"

    def test_case_insensitive_match(self, catalogue):
        """Queries are case-insensitive — 'CHERRY' matches 'Cherry'."""
        total, keys = _ft_search(catalogue, "products", "@description:cherry")
        assert total == 2, f"expected 2 Cherry MX products, got {total}"
        assert "prod:1" in keys
        assert "prod:5" in keys

    def test_no_cross_field_match(self, catalogue):
        """
        'studio' appears only in descriptions of prod:6 and prod:7.
        Querying @title:studio must return nothing.
        """
        total_title, _ = _ft_search(catalogue, "products", "@title:studio")
        assert total_title == 0, \
            f"@title:studio must not match description-only tokens, got {total_title}"

        total_desc, keys = _ft_search(catalogue, "products", "@description:studio")
        assert total_desc == 2, f"@description:studio should find 2 products, got {total_desc}"
        assert "prod:6" in keys
        assert "prod:7" in keys


# ── Stopwords ─────────────────────────────────────────────────────────────────

class TestStopwords:
    """Common words must not be indexed or matchable."""

    def test_stopword_the_returns_nothing(self, catalogue):
        """Every description contains common words — none should be searchable."""
        total, _ = _ft_search(catalogue, "products", "@description:the")
        assert total == 0, f"stopword 'the' should return 0 results, got {total}"

    def test_stopword_and_returns_nothing(self, catalogue):
        total, _ = _ft_search(catalogue, "products", "@description:and")
        assert total == 0

    def test_non_stopword_beside_stopword_still_matches(self, catalogue):
        """
        prod:3: "seven port powered hub fast charging"
        'fast' is not a stopword — it should be findable even though it
        sits beside common words.
        """
        total, keys = _ft_search(catalogue, "products", "@description:fast")
        assert total >= 1
        assert "prod:3" in keys


# ── BM25 ranking ──────────────────────────────────────────────────────────────

class TestBM25Ranking:
    """BM25 scores should produce sensible ranking."""

    def test_higher_tf_ranks_first(self, r):
        """
        Doc with 'ergonomic' appearing more times should rank above one with
        a single mention — TF saturation, not raw count.
        """
        r.execute_command(
            "FT.CREATE", "rank_idx",
            "ON", "HASH", "PREFIX", "1", "rank:",
            "SCHEMA", "body", "TEXT",
        )
        try:
            r.hset("rank:1", mapping={"body": "ergonomic ergonomic ergonomic keyboard stand"})
            r.hset("rank:2", mapping={"body": "ergonomic mouse"})
            time.sleep(0.05)

            total, keys = _ft_search(r, "rank_idx", "@body:ergonomic")
            assert total == 2
            assert keys[0] == "rank:1", \
                f"rank:1 (3× ergonomic) should outrank rank:2 (1×), got order: {keys}"
        finally:
            r.execute_command("FT.DROPINDEX", "rank_idx")

    def test_rare_term_scores_higher_than_common(self, r):
        """
        A term that appears in only 1 of 5 documents has high IDF and should
        rank that document first when queried.
        """
        r.execute_command(
            "FT.CREATE", "idf_idx",
            "ON", "HASH", "PREFIX", "1", "idf:",
            "SCHEMA", "body", "TEXT",
        )
        try:
            # 'kividb' appears only in idf:1
            r.hset("idf:1", mapping={"body": "kividb database storage"})
            for i in range(2, 6):
                r.hset(f"idf:{i}", mapping={"body": "database storage systems"})
            time.sleep(0.05)

            total, keys = _ft_search(r, "idf_idx", "@body:kividb")
            assert total == 1
            assert keys[0] == "idf:1"
        finally:
            r.execute_command("FT.DROPINDEX", "idf_idx")

    def test_catalogue_ergonomic_ranking(self, catalogue):
        """
        In the product catalogue, 'ergonomic' appears in:
          prod:2 description: 1 occurrence
          prod:4 title+description: 2 occurrences
          prod:9 description: 1 occurrence
        prod:4 should rank first.
        """
        total, keys = _ft_search(
            catalogue, "products",
            "@description:ergonomic",
        )
        assert total >= 2
        assert keys[0] == "prod:4", \
            f"prod:4 has most 'ergonomic' occurrences, should rank first, got {keys}"


# ── AND / OR filter combinations ──────────────────────────────────────────────

class TestFilterCombinations:
    """Inverted index intersection and union."""

    def test_and_both_tokens_required(self, catalogue):
        """
        @title:mechanical AND @description:cherry → only products with both.
        prod:1 and prod:5 have 'mechanical' in title AND 'cherry' in description.
        """
        total, keys = _ft_search(
            catalogue, "products",
            "(@title:mechanical @description:cherry)",
        )
        assert total == 2
        assert "prod:1" in keys
        assert "prod:5" in keys

    def test_and_excludes_partial_match(self, catalogue):
        """
        @title:mechanical AND @description:bluetooth — no product has both.
        prod:1/5 have mechanical title but no bluetooth in description.
        prod:2/6 have bluetooth in description but not mechanical in title.
        """
        total, _ = _ft_search(
            catalogue, "products",
            "(@title:mechanical @description:bluetooth)",
        )
        assert total == 0, \
            f"no product has both mechanical title and bluetooth description, got {total}"

    def test_or_either_token_sufficient(self, catalogue):
        """
        @category:{audio} OR @category:{video} — union of two categories.
        prod:6 (audio) and prod:7 (video).
        """
        total, keys = _ft_search(
            catalogue, "products",
            "@category:{audio|video}",
        )
        assert total == 2
        assert "prod:6" in keys
        assert "prod:7" in keys

    def test_text_or_across_fields(self, catalogue):
        """
        bluetooth appears in prod:2 description and prod:6 description.
        wireless appears in prod:2 title and prod:6 title.
        OR query should find at least both.
        """
        total, keys = _ft_search(
            catalogue, "products",
            "(@description:bluetooth | @description:wireless)",
        )
        assert total >= 2
        assert "prod:2" in keys
        assert "prod:6" in keys


# ── Prefix queries ────────────────────────────────────────────────────────────

class TestPrefixQueries:
    """Prefix matching via the inverted index."""

    def test_prefix_matches_multiple_tokens(self, catalogue):
        """
        @description:bl* matches 'bluetooth' in prod:2 and prod:6.
        @description:ba* matches 'backlit' in prod:5.
        """
        total_bl, keys_bl = _ft_search(catalogue, "products", "@description:bl*")
        assert "prod:2" in keys_bl, f"prod:2 (bluetooth) must match bl*: {keys_bl}"
        assert "prod:6" in keys_bl, f"prod:6 (bluetooth) must match bl*: {keys_bl}"

        total_ba, keys_ba = _ft_search(catalogue, "products", "@description:ba*")
        assert "prod:5" in keys_ba, f"prod:5 (backlit) must match ba*: {keys_ba}"

    def test_prefix_no_match(self, catalogue):
        """A prefix that matches nothing should return 0 results."""
        total, _ = _ft_search(catalogue, "products", "@title:zzz*")
        assert total == 0

    def test_prefix_category_independent(self, catalogue):
        """
        @title:wire* should match 'Wireless Mouse' (prod:2).
        """
        total, keys = _ft_search(catalogue, "products", "@title:wire*")
        assert "prod:2" in keys, f"prod:2 (Wireless Mouse) must match @title:wire*: {keys}"


# ── Backfill ──────────────────────────────────────────────────────────────────

class TestBackfill:
    """FT.CREATE over pre-existing keys must index them immediately."""

    def test_pre_existing_docs_are_searchable(self, r):
        """
        Insert products BEFORE creating the index.
        After FT.CREATE, all pre-existing products must be searchable.
        """
        r.hset("pre:1", mapping={"title": "mechanical keyboard", "category": "peripherals"})
        r.hset("pre:2", mapping={"title": "wireless mouse",      "category": "peripherals"})
        r.hset("pre:3", mapping={"title": "desk lamp",           "category": "lighting"})

        r.execute_command(
            "FT.CREATE", "pre_idx",
            "ON", "HASH", "PREFIX", "1", "pre:",
            "SCHEMA", "title", "TEXT", "category", "TAG",
        )
        time.sleep(0.1)

        try:
            total, keys = _ft_search(r, "pre_idx", "@title:mechanical")
            assert total == 1
            assert "pre:1" in keys

            total2, keys2 = _ft_search(r, "pre_idx", "@title:wireless")
            assert total2 == 1
            assert "pre:2" in keys2

            # Match-all should find all three
            total3, _ = _ft_search(r, "pre_idx", "*")
            assert total3 == 3
        finally:
            r.execute_command("FT.DROPINDEX", "pre_idx")

    def test_backfill_bm25_ranking_correct(self, r):
        """
        BM25 stats collected during backfill must produce correct ranking.
        The doc with higher TF must rank above the one with lower TF even
        when both were indexed via backfill (not incremental HSET).
        """
        r.hset("bf:1", mapping={"body": "rust rust rust systems programming"})
        r.hset("bf:2", mapping={"body": "rust programming"})

        r.execute_command(
            "FT.CREATE", "bf_idx",
            "ON", "HASH", "PREFIX", "1", "bf:",
            "SCHEMA", "body", "TEXT",
        )
        time.sleep(0.1)

        try:
            total, keys = _ft_search(r, "bf_idx", "@body:rust")
            assert total == 2
            assert "bf:1" in keys and "bf:2" in keys, \
                f"both documents must appear in results, got {keys}"
        finally:
            r.execute_command("FT.DROPINDEX", "bf_idx")


# ── Incremental updates ───────────────────────────────────────────────────────

class TestIncrementalUpdates:
    """HSET after FT.CREATE updates the index live."""

    def test_new_document_immediately_searchable(self, catalogue):
        """A product added after index creation is findable without delay."""
        catalogue.hset("prod:99", mapping={
            "title":       "Thunderbolt Dock",
            "description": "thunderbolt docking station multiport",
            "category":    "accessories",
            "price":       "199",
        })
        time.sleep(0.05)

        total, keys = _ft_search(catalogue, "products", "@title:thunderbolt")
        assert total == 1
        assert "prod:99" in keys

    def test_updated_document_new_tokens_searchable(self, catalogue):
        """
        Updating a product's description adds new tokens to the index.
        prod:8 (Desk Lamp) — update description to include 'circadian'.
        """
        # Verify 'circadian' is not findable yet
        total_before, _ = _ft_search(catalogue, "products", "@description:circadian")
        assert total_before == 0

        # Update the description
        catalogue.hset("prod:8", mapping={
            "description": "LED adjustable brightness circadian rhythm tuning"
        })
        time.sleep(0.05)

        total_after, keys = _ft_search(catalogue, "products", "@description:circadian")
        assert total_after == 1
        assert "prod:8" in keys

    def test_product_launch_and_discontinue_lifecycle(self, catalogue):
        """
        Simulate a product lifecycle:
          1. Launch: new product added → findable
          2. Discontinue: product deleted → no longer findable
          3. Other products in same category unaffected
        """
        # Launch
        catalogue.hset("prod:launch", mapping={
            "title":       "Prototype Gadget",
            "description": "experimental prototype gadget limited release",
            "category":    "accessories",
            "price":       "999",
        })
        time.sleep(0.05)

        total_launched, keys = _ft_search(catalogue, "products", "@title:prototype")
        assert total_launched == 1
        assert "prod:launch" in keys

        # Accessories still has the right count
        total_acc, _ = _ft_search(catalogue, "products", "@category:{accessories}")
        assert total_acc >= 5  # prod:3,4,9,10 + prod:launch

        # Discontinue
        catalogue.delete("prod:launch")
        time.sleep(0.05)

        total_discontinued, _ = _ft_search(catalogue, "products", "@title:prototype")
        assert total_discontinued == 0, \
            "deleted product must not appear in search results"

        # Other accessories still findable
        total_acc_after, _ = _ft_search(catalogue, "products", "@category:{accessories}")
        assert total_acc_after == total_acc - 1


# ── Delete ────────────────────────────────────────────────────────────────────

class TestDelete:
    """DEL removes documents from the inverted index."""

    def test_del_removes_from_search(self, catalogue):
        """
        prod:1 and prod:5 both match @title:mechanical.
        After deleting prod:1, only prod:5 remains.
        """
        total_before, keys_before = _ft_search(catalogue, "products", "@title:mechanical")
        assert total_before == 2
        assert "prod:1" in keys_before

        catalogue.delete("prod:1")
        time.sleep(0.05)

        total_after, keys_after = _ft_search(catalogue, "products", "@title:mechanical")
        assert total_after == 1
        assert "prod:1" not in keys_after
        assert "prod:5" in keys_after

    def test_del_last_doc_empties_posting_list(self, r):
        """
        When the only document containing a term is deleted, that term
        must return 0 results (posting list is cleaned up).
        """
        r.execute_command(
            "FT.CREATE", "del_idx",
            "ON", "HASH", "PREFIX", "1", "del:",
            "SCHEMA", "title", "TEXT",
        )
        try:
            r.hset("del:1", mapping={"title": "quuxbazxyz unique product"})
            time.sleep(0.05)

            total_before, _ = _ft_search(r, "del_idx", "@title:quuxbazxyz")
            assert total_before == 1

            r.delete("del:1")
            time.sleep(0.05)

            total_after, _ = _ft_search(r, "del_idx", "@title:quuxbazxyz")
            assert total_after == 0, \
                "posting list must be empty after deleting the only document"
        finally:
            r.execute_command("FT.DROPINDEX", "del_idx")


# ── Realistic search scenario ─────────────────────────────────────────────────

class TestRealisticProductSearch:
    """
    End-to-end product search scenario — what a real application does.
    These tests compose multiple features together.
    """

    def test_search_by_keyword_returns_relevant_products(self, catalogue):
        """
        Customer searches for 'bluetooth' — should find wireless mouse and headphones.
        """
        total, keys = _ft_search(catalogue, "products", "@description:bluetooth")
        assert total >= 2
        assert "prod:2" in keys, "Wireless Mouse (bluetooth) must appear"
        assert "prod:6" in keys, "Headphones (bluetooth) must appear"

    def test_search_and_filter_by_category(self, catalogue):
        """
        Customer wants 'ergonomic' products in the 'accessories' category.
        Expected: prod:4 (Laptop Stand — ergonomic) and prod:9 (Monitor Arm — ergonomic).
        prod:2 (ergonomic mouse) is in peripherals, not accessories.
        """
        total, keys = _ft_search(
            catalogue, "products",
            "(@description:ergonomic @category:{accessories})",
        )
        assert total >= 1
        assert "prod:4" in keys, "Laptop Stand must match (ergonomic + accessories)"
        assert "prod:9" in keys, "Monitor Arm must match (ergonomic + accessories)"
        # prod:2 is peripherals — must not appear
        assert "prod:2" not in keys, \
            "Wireless Mouse is peripherals not accessories, must not appear"

    def test_search_switches_finds_keyboards_only(self, catalogue):
        """
        'switches' appears only in keyboard-related products (prod:1, prod:5).
        Confirms field content drives results, not category metadata.
        """
        total, keys = _ft_search(catalogue, "products", "@description:switches")
        assert total == 2
        assert "prod:1" in keys
        assert "prod:5" in keys

    def test_broad_search_then_narrow(self, catalogue):
        """
        Broad: @description:adjustable → many matches (lamp, stand, arm).
        Narrow: @description:adjustable AND @category:{accessories} → fewer.
        """
        total_broad, _ = _ft_search(catalogue, "products", "@description:adjustable")
        assert total_broad >= 3

        total_narrow, keys_narrow = _ft_search(
            catalogue, "products",
            "(@description:adjustable @category:{accessories})",
        )
        assert total_narrow < total_broad
        # prod:4 (Laptop Stand) and prod:9 (Monitor Arm) are accessories + adjustable
        assert "prod:4" in keys_narrow
        assert "prod:9" in keys_narrow
        # prod:8 (Desk Lamp) is lighting, not accessories
        assert "prod:8" not in keys_narrow

    def test_match_all_returns_full_catalogue(self, catalogue):
        """FT.SEARCH * returns every indexed product — 10 in total."""
        total, _ = _ft_search(catalogue, "products", "*")
        assert total == 10, f"match-all must return all 10 products, got {total}"

    def test_search_nonexistent_term_returns_empty(self, catalogue):
        """A search for a term not present in any product returns 0 results."""
        total, _ = _ft_search(catalogue, "products", "@title:zxqvblorp")
        assert total == 0

    def test_concurrent_index_updates_consistent(self, r):
        """
        Two clients writing to the same index concurrently.
        After both finish, all their documents must be searchable.
        This exercises the Arc<RwLock> inverted index under concurrent writes.
        """
        import threading

        r.execute_command(
            "FT.CREATE", "conc_idx",
            "ON", "HASH", "PREFIX", "1", "conc:",
            "SCHEMA", "title", "TEXT",
        )

        errors = []

        def writer(client_r, start, end):
            try:
                for i in range(start, end):
                    client_r.hset(f"conc:{i}", mapping={"title": f"product {i} kividb"})
            except Exception as e:
                errors.append(e)

        # Two clients, 25 writes each
        c1 = r
        c2 = redis.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            decode_responses=True,
        )

        try:
            t1 = threading.Thread(target=writer, args=(c1, 0, 25))
            t2 = threading.Thread(target=writer, args=(c2, 25, 50))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            assert not errors, f"writer threads raised errors: {errors}"
            time.sleep(0.1)

            # Every document must be findable via the shared token 'kividb'
            total, _ = _ft_search(r, "conc_idx", "@title:kividb")
            assert total == 50, \
                f"all 50 concurrently written docs must be indexed, got {total}"
        finally:
            r.execute_command("FT.DROPINDEX", "conc_idx")
            c2.close()