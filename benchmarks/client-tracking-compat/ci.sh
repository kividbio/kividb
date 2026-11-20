#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              nothing to install: the harness is standard-library Python 3
#   ci.sh run DIR PORT OUT.xml   client_tracking_compat.py against 127.0.0.1:PORT, as JUnit XML
# Client-side caching as Frappe uses it: `CLIENT TRACKING ON REDIRECT <id> NOLOOP` with the
# invalidations read from `__redis__:invalidate` on a second connection. Any port works.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cmd=${1:-} dir=${2:-}
case $cmd in
  setup) mkdir -p "$dir" ;;
  run)
    port=$3 out=$4
    # The harness exits 0 whatever the results: they are in OUT.xml.
    timeout 300 python3 "$here/client_tracking_compat.py" --port "$port" --junitxml "$out" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
