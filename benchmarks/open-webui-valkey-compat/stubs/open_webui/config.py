"""Stand-in for open_webui.config: the settings valkey.py reads, at Open WebUI's defaults.

Open WebUI's own config.py is the application's settings database; the Valkey store takes
seven values from it. VALKEY_URL comes from the environment, as in Open WebUI.
"""
import os

VALKEY_URL = os.environ.get("VALKEY_URL", "valkey://localhost:6379")
VALKEY_COLLECTION_PREFIX = "open_webui"
VALKEY_DISTANCE_METRIC = "COSINE"
VALKEY_INDEX_TYPE = "HNSW"
VALKEY_HNSW_M = 16
VALKEY_HNSW_EF_CONSTRUCTION = 200
VALKEY_HNSW_EF_RUNTIME = 10
