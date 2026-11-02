#!/usr/bin/env bash
# e2e_published.sh <tag> <repo-checkout> <out-dir>
# The command-surface job of .github/workflows/smoke-test.yml, run by hand: the repo's tests/e2e suite
# (checked out at the tag) against the PUBLISHED default image, once per protocol (RESP2, RESP3).
set -u
TAG=$1 SRC=$2 OUT=$3
IMG=ghcr.io/kividbio/kividb:$TAG
mkdir -p "$OUT"
python3 -m venv "$HOME/e2e-venv"
"$HOME/e2e-venv/bin/pip" install -q -r "$SRC/tests/e2e/requirements.txt"
export KIVI_BINARY=$HOME/kividb-published
cid=$(docker create "$IMG"); docker cp "$cid:/usr/local/bin/kividb" "$KIVI_BINARY"; docker rm "$cid" >/dev/null
chmod +x "$KIVI_BINARY"; "$KIVI_BINARY" --version | tee "$OUT/e2e-version.txt"
for resp in 2 3; do
  docker rm -f e2e-smoke >/dev/null 2>&1
  docker run -d --name e2e-smoke -p 6380:6380 "$IMG" --port 6380 --loglevel error --io-model tokio --protected-mode no >/dev/null
  for _ in $(seq 1 30); do nc -z 127.0.0.1 6380 && break; sleep 0.5; done
  (cd "$SRC/tests/e2e" && KIVI_EXTERNAL=1 KIVI_PORT=6380 KIVI_RESP=$resp timeout 3600 "$HOME/e2e-venv/bin/pytest" modules/ -q \
     -p no:cacheprovider --timeout 120 \
     --deselect modules/test_acid.py::TestFullACIDScenario::test_balance_transfer_acid \
     --deselect modules/test_acid.py::TestAtomicity::test_multi_key_transfer_is_atomic) > "$OUT/e2e-resp$resp.log" 2>&1
  echo "RESP$resp exit=$? $(tail -1 "$OUT/e2e-resp$resp.log")" | tee -a "$OUT/e2e-summary.txt"
  docker rm -f e2e-smoke >/dev/null 2>&1
done
