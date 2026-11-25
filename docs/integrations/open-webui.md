# Open WebUI

Open WebUI's Valkey vector store (`VECTOR_DB=valkey`, `backend/open_webui/retrieval/vector/dbs/valkey.py`, on
`valkey-glide-sync`) runs on KiviDB as shipped, with nothing patched. Point `VALKEY_URL` at KiviDB
(`valkey://host:6380`). Its HNSW index with TAG filters on `file_id` is the filtered nearest-neighbour
query KiviDB is built for. The default image is enough; this path runs no Lua.

What the store reads from the server, and what KiviDB answers:

- **The version gate.** The store reads `valkey_version` from `INFO` (`redis_version` if there is none) and
  wants 9.0.1 or later. KiviDB's `INFO` has `valkey_version:9.0.1` besides `redis_version:8.10.0`. `MODULE
  LIST` names `search` at version 8.10.1, above the 1.2.0 it asks for.
- **A missing index.** `has_collection` calls `FT.INFO` and takes an index to be absent when the error says
  `not found in database`. valkey-glide connections (`CLIENT SETINFO LIB-NAME` begins with `Glide`) get
  valkey-search's text, `Index with name '<name>' not found in database 0`; other clients get Redis 8.10's,
  `SEARCH_INDEX_NOT_FOUND Index not found: <name>`.
- **The reply shapes.** valkey-glide converts `FT.SEARCH`, `FT.INFO` and `FT._LIST` replies itself from
  valkey-search's layout, which a GLIDE connection is answered in (`search-resp3-reply`, in
  [CONFIGURATION.md](../CONFIGURATION.md#search-reply-shape-over-resp3)). `FT.INFO` keeps a vector field's
  dimensions in an `index` list, where the store reads them to refuse vectors of another size.

**Checked:** 17 of 17 checks pass on KiviDB and on `valkey/valkey-bundle:9.2.0`: Open WebUI's own
`ValkeyClient` (valkey.py, vector/main.py, vector/utils.py and json_codec.py at commit `0666e59d26a4`, with
small stand-ins for its `config`, `env` and `misc` modules) through insert, search, filtered search with
`$in` and `$ne`, query, get, upsert, delete by ids and by filter, `delete_collection` and `reset`. Redis 8.10
fails every check: it is not Valkey, and has no `valkey_version`. The harness, and the script that runs it
against valkey-bundle, are in [`benchmarks/open-webui-valkey-compat/`](../../benchmarks/open-webui-valkey-compat/);
the compat workflow runs both.
