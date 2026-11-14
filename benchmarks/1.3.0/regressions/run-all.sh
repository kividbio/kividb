#!/usr/bin/env bash
# The 1.3.0 regression suite's single runner. See README.md.
#
#   run-all.sh --candidate <path-to-kividb | tag> [--label NAME] [--only 01,04]
#              [--engines v1_2_2,candidate,redis] [--record] [--candidate-commit SHA]
#   FULL=1 run-all.sh ...      also the long configurations (17.5M keys with EX)
#
# Each suite runs against v1.2.2 (the release tarball, SHA-256 checked), the
# candidate and Redis 8.10.x, one server at a time on this machine, each
# configuration on a fresh server. Results go to results/<label>/: summary.md,
# summary.json and raw.tar.gz. The exit status is compare.py's: 1 when a gate
# fails. --record writes the measured values into baseline.json instead.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
export REGRESS_LIB=$here/lib
# shellcheck source=lib/engines.sh
source "$here/lib/engines.sh"

CANDIDATE= LABEL= ONLY= ENGINES=v1_2_2,candidate,redis RECORD= CAND_COMMIT=
REFERENCE_TAG=${REFERENCE_TAG:-v1.2.2}
SUITE_TIMEOUT=${SUITE_TIMEOUT:-1800}
while [ $# -gt 0 ]; do
  case $1 in
    --candidate) CANDIDATE=$2; shift 2 ;;
    --label) LABEL=$2; shift 2 ;;
    --only) ONLY=$2; shift 2 ;;
    --engines) ENGINES=$2; shift 2 ;;
    --record) RECORD=--record; shift ;;
    --candidate-commit) CAND_COMMIT=$2; shift 2 ;;
    -h | --help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
[ -n "$CANDIDATE" ] || { echo "--candidate <path|tag> is required" >&2; exit 2; }
[ "$(uname -s)" = Linux ] || { echo "Linux only (VmRSS, io_uring)" >&2; exit 2; }

mkdir -p "$REGRESS_CACHE"
# Python: a venv with the pinned client.
VENV=$REGRESS_CACHE/venv
if [ ! -x "$VENV/bin/python" ] || ! cmp -s "$here/requirements.txt" "$VENV/.requirements"; then
  rm -rf "$VENV"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q --disable-pip-version-check -r "$here/requirements.txt"
  cp "$here/requirements.txt" "$VENV/.requirements"
fi
export PYTHON=$VENV/bin/python

# Engines.
declare -A BIN KIND
BIN[v1_2_2]=$(engines_fetch_release "$REFERENCE_TAG" default "$REGRESS_CACHE/releases")
engines_fetch_release "$REFERENCE_TAG" full "$REGRESS_CACHE/releases" >/dev/null  # for suites that need Lua
BIN[candidate]=$(engines_resolve_candidate "$CANDIDATE" "$REGRESS_CACHE/releases")
BIN[redis]=$(engines_resolve_redis "$REGRESS_CACHE/redis")
KIND[v1_2_2]=kividb KIND[candidate]=kividb KIND[redis]=redis
if [ -z "$CAND_COMMIT" ]; then
  CAND_COMMIT=$("${BIN[candidate]}" --version 2>/dev/null | head -1 || true)
fi
[ -n "$LABEL" ] || LABEL=run-$(date -u +%Y%m%d-%H%M%S)
OUT=$here/results/$LABEL
mkdir -p "$OUT"
rm -f "$OUT/results.jsonl"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/kividb-regress-run.XXXXXX")
mkdir -p "$WORK/raw/logs"
CHILD=
trap '[ -n "$CHILD" ] && kill "$CHILD" 2>/dev/null; engine_stop; rm -rf "$WORK"' EXIT
trap 'exit 130' INT TERM

vm_size() {
  curl -fs -m 2 -H Metadata:true "http://169.254.169.254/metadata/instance/compute/vmSize?api-version=2021-02-01&format=text" 2>/dev/null ||
    curl -fs -m 2 http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo "${VM_SIZE:-unknown}"
}

# Which suites.
mapfile -t SUITES < <(cd "$here/suites" && ls [0-9][0-9]-*.py | sort)
want_suite() {
  [ -z "$ONLY" ] && return 0
  local s=$1 w
  for w in ${ONLY//,/ }; do case $s in "$w" | "$w"-* | "$w".py) return 0 ;; esac; done
  return 1
}
want_engine() { case ",$ENGINES," in *",$1,"*) return 0 ;; esac; return 1; }

declare -A SECS
status=0
for f in "${SUITES[@]}"; do
  suite=${f%.py}
  want_suite "$suite" || continue
  echo "=== $suite"
  s0=$(date +%s)
  while IFS= read -r cfg; do
    name=$("$PYTHON" -c 'import json,sys;print(json.loads(sys.argv[1])["name"])' "$cfg")
    io=$("$PYTHON" -c 'import json,sys;print(json.loads(sys.argv[1])["io"])' "$cfg")
    threads=$("$PYTHON" -c 'import json,sys;print(json.loads(sys.argv[1])["threads"])' "$cfg")
    engs=$("$PYTHON" -c 'import json,sys;print(" ".join(json.loads(sys.argv[1])["engines"]))' "$cfg")
    for e in $engs; do
      want_engine "$e" || continue
      echo "--- $suite $name $e"
      engine_start "${KIND[$e]}" "${BIN[$e]}" "$io" "$threads" || { status=1; continue; }
      # In the background and waited for, so that a signal stops the run at once.
      timeout "$SUITE_TIMEOUT" "$PYTHON" "$here/suites/$f" --config "$name" --engine "$e" \
        --kind "${KIND[$e]}" --port "$ENGINE_PORT" --pid "$ENGINE_PID" --out "$WORK" </dev/null &
      CHILD=$!
      rc=0
      wait "$CHILD" || rc=$?
      CHILD=
      if [ $rc -ne 0 ]; then
        echo "!!! $suite $name $e failed ($rc)" >&2
        status=1
      fi
      tail -c 65536 "$ENGINE_LOG" >"$WORK/raw/logs/$suite-$name-$e.log" 2>/dev/null || true
      engine_stop
    done
  done < <("$PYTHON" "$here/suites/$f" --configs)
  SECS[$suite]=$(($(date +%s) - s0))
  echo "=== $suite took ${SECS[$suite]} s"
done
[ -f "$WORK/results.jsonl" ] && cp "$WORK/results.jsonl" "$OUT/results.jsonl"

# Where and what: env.json.
{
  secs=$(for k in "${!SECS[@]}"; do printf '"%s": %s,' "$k" "${SECS[$k]}"; done)
  "$PYTHON" - "$OUT/env.json" "$LABEL" "$CAND_COMMIT" "{${secs%,}}" "$(vm_size)" \
    "${BIN[v1_2_2]}" "${BIN[candidate]}" "${BIN[redis]}" <<'EOF'
import json, sys, os, platform, subprocess, hashlib, datetime
out, label, commit, secs, vm, b122, bcand, bredis = sys.argv[1:9]
def sha(p):
    h = hashlib.sha256()
    with open(os.path.realpath(p), "rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            h.update(blk)
    return h.hexdigest()
def ver(p):
    try:
        return subprocess.run([p, "--version"], capture_output=True, text=True, timeout=10).stdout.strip().splitlines()[0]
    except Exception as e:
        return "?"
cpu = "?"
try:
    cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
except Exception:
    pass
import redis
env = {
    "label": label, "date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "vm": vm, "cpu": cpu, "nproc": os.cpu_count(), "kernel": platform.release(),
    "glibc": platform.libc_ver()[1], "python": platform.python_version(), "redis_py": redis.__version__,
    "candidate_commit": commit, "suite_seconds": json.loads(secs),
    "engines": {k: {"path": p, "sha256": sha(p), "version": ver(p)}
                for k, p in (("v1_2_2", b122), ("candidate", bcand), ("redis", bredis))},
}
json.dump(env, open(out, "w"), indent=1)
EOF
}

# One small tarball of the raw files; the summaries next to it.
cp "$OUT/env.json" "$WORK/raw/"
[ -f "$OUT/results.jsonl" ] && cp "$OUT/results.jsonl" "$WORK/raw/"
tar -czf "$OUT/raw.tar.gz" -C "$WORK" raw
if [ -f "$OUT/results.jsonl" ]; then
  set +e
  "$PYTHON" "$here/lib/compare.py" "$OUT" --baseline "$here/baseline.json" --label "$LABEL" $RECORD
  cmp=$?
  set -e
  [ $cmp -ne 0 ] && status=$cmp
fi
rm -f "$OUT/results.jsonl"  # it is inside raw.tar.gz
echo "results: $OUT"
exit $status
