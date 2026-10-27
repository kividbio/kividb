#!/usr/bin/env bash
# Run every client example against a KiviDB server.
#
#   examples/clients/run-all.sh                    # KiviDB on localhost:6380
#   KIVIDB_URL=redis://host:6380/0 examples/clients/run-all.sh
#   SKIP=java,bullmq examples/clients/run-all.sh   # leave some out
#
# The BullMQ example needs a build with Lua (the `lua` or `full` variant).
# Needs python3, node and npm, go, and a JDK with maven, for the ones run.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
export KIVIDB_URL=${KIVIDB_URL:-redis://localhost:6380/0}
skip=",${SKIP:-},"

# run NAME COMMAND...: run it unless NAME is in SKIP; stop at the first failure.
run() {
  if [[ $skip == *",$1,"* ]]; then
    echo "$1: skipped"
    return 0
  fi
  "${@:2}" || { echo "$1: FAILED" >&2; exit 1; }
}

python_examples() {
  cd "$here/python" &&
    python3 -m venv .venv &&
    .venv/bin/pip install -q -r requirements.txt &&
    .venv/bin/python quickstart.py &&
    .venv/bin/python vector_search.py
}
node_install() { cd "$here/node" && npm install --silent --no-audit --no-fund; }
go_example() { cd "$here/go" && go run .; }
java_example() { cd "$here/java" && mvn -q compile exec:java; }

run python python_examples
run node node_install
run node node "$here/node/quickstart.mjs"
run bullmq node "$here/node/bullmq.mjs"
run go go_example
run java java_example
echo "all examples: ok"
