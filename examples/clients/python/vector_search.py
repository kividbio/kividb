"""KiviDB's built-in vector and full-text search, from redis-py.

The FT.* commands are Redis Stack's: an index over hashes with a TEXT, a TAG
and a VECTOR field, then a KNN query filtered by tag. No module to load.

    pip install -r requirements.txt
    python vector_search.py
"""

import os
import struct

import redis
from redis.commands.search.field import TagField, TextField, VectorField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

r = redis.Redis.from_url(os.environ.get("KIVIDB_URL", "redis://localhost:6380/0"))


def vec(*xs):
    """A vector as KiviDB (and Redis) store it: little-endian FLOAT32."""
    return struct.pack(f"<{len(xs)}f", *xs)


try:
    r.ft("docs").dropindex(delete_documents=True)
except redis.ResponseError:
    pass  # no index yet

r.ft("docs").create_index(
    [
        TextField("title"),
        TagField("kind"),
        VectorField(
            "embedding",
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": 3, "DISTANCE_METRIC": "COSINE"},
        ),
    ],
    definition=IndexDefinition(prefix=["doc:"], index_type=IndexType.HASH),
)

docs = {
    "doc:1": ("Red apples", "fruit", vec(1.0, 0.0, 0.0)),
    "doc:2": ("Green apples", "fruit", vec(0.9, 0.1, 0.0)),
    "doc:3": ("Ripe bananas", "fruit", vec(0.0, 1.0, 0.0)),
    "doc:4": ("Red car", "vehicle", vec(1.0, 0.0, 0.1)),
}
for key, (title, kind, embedding) in docs.items():
    r.hset(key, mapping={"title": title, "kind": kind, "embedding": embedding})

# The two fruit nearest to (1, 0, 0).
query = (
    Query("(@kind:{fruit})=>[KNN 2 @embedding $q AS score]")
    .sort_by("score")
    .return_fields("title", "score")
    .dialect(2)
)
result = r.ft("docs").search(query, query_params={"q": vec(1.0, 0.0, 0.0)})
titles = [doc.title for doc in result.docs]
assert titles == ["Red apples", "Green apples"], titles

# Full-text on the same index.
result = r.ft("docs").search(Query("@title:apples").return_fields("title"))
assert result.total == 2, result

print("python vector search: ok")
