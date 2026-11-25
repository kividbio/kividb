#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              Open WebUI's Valkey vector store at a pinned commit, with requirements.txt
#   ci.sh run DIR PORT OUT.xml   its ValkeyClient through insert, search, filtered search, query, get,
#                                upsert, delete and reset (check.py), against 127.0.0.1:PORT
#
# The files are Open WebUI's own, fetched from the commit below and checked against their SHA-256:
# backend/open_webui/retrieval/vector/dbs/valkey.py, retrieval/vector/main.py, retrieval/vector/utils.py
# and utils/json_codec.py. open_webui.config, .env and .utils.misc, which belong to the rest of the
# application, are the small stand-ins in stubs/. Nothing in the client is patched.
#
# Redis cannot take part: the client reads `valkey_version` from INFO (Redis has none, so its
# `redis_version`, 8.x, is below the 9.0.1 it asks for) and talks valkey-search's FT.* replies. Every
# check fails on Redis, so the job compares KiviDB against kividb-passes.txt, and control.sh runs the
# same checks against valkey/valkey-bundle, which they must pass.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cmd=$1 dir=$2
commit=0666e59d26a4d056e0bacee41742486badd71cf8
raw=https://raw.githubusercontent.com/open-webui/open-webui/$commit/backend/open_webui
case $cmd in
  setup)
    mkdir -p "$dir/pkg"
    cp -r "$here/stubs/open_webui" "$dir/pkg/"
    mkdir -p "$dir/pkg/open_webui/retrieval/vector/dbs"
    : > "$dir/pkg/open_webui/retrieval/__init__.py"
    : > "$dir/pkg/open_webui/retrieval/vector/__init__.py"
    : > "$dir/pkg/open_webui/retrieval/vector/dbs/__init__.py"
    fetch() { # PATH-IN-REPO DESTINATION SHA256
      curl -fsSL --retry 3 "$raw/$1" -o "$dir/pkg/open_webui/$2"
      echo "$3  $dir/pkg/open_webui/$2" | sha256sum -c --quiet -
    }
    fetch retrieval/vector/dbs/valkey.py retrieval/vector/dbs/valkey.py 84ebc3593e819ff6b9f405daecb851e5372584d01d3f0f8a8e70d79364a509b7
    fetch retrieval/vector/main.py retrieval/vector/main.py 04ba29739dba9812c8a211bb4f79e4852122c83eb7a40beaf4c0bc58f2d4def2
    fetch retrieval/vector/utils.py retrieval/vector/utils.py 7ab03f0ad55bcf2e508007278ca8f07b545cc7336c95745bdff045a8e4ba990d
    fetch utils/json_codec.py utils/json_codec.py 73c85e62aa32700f9f28a01eafa2fd007fb05874dcd6aa36cf70e6f7af9e679e
    uv venv -q --clear --python 3.12 "$dir/venv"
    VIRTUAL_ENV=$dir/venv uv pip install -q -r "$here/requirements.txt"
    ;;
  run)
    port=$3 out=$4
    timeout 600 "$dir/venv/bin/python" "$here/check.py" "$dir/pkg" "$port" "$out" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
