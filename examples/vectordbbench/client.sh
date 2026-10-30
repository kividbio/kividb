#!/usr/bin/env bash
# The client machine: installs VectorDBBench with its KiviDB client, then runs
# the Cohere 1M search performance case against the database machine.
#
#   KIVIDB_HOST=10.20.0.5 KIVIDB_PASSWORD=... ./client.sh
#
# The KiviDB client was merged in zilliztech/VectorDBBench#884, after the
# 2.0.0 release on PyPI: until a release has it, VectorDBBench is installed
# from its main branch.
set -eu
: "${KIVIDB_HOST:?set KIVIDB_HOST}"
: "${KIVIDB_PASSWORD:?set KIVIDB_PASSWORD}"
REPO=${VDBBENCH_REPO:-https://github.com/zilliztech/VectorDBBench}
BRANCH=${VDBBENCH_BRANCH:-main}

cd "$HOME"
sudo apt-get update -qq >/dev/null
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git python3-venv python3-dev build-essential >/dev/null
[ -d vdbbench ] || git clone -q --branch "$BRANCH" "$REPO" vdbbench
python3 -m venv venv
. venv/bin/activate
pip install -q --upgrade pip
pip install -q -e "./vdbbench[kividb]"

ulimit -n 65535
# HNSW: M 16, EF_CONSTRUCTION 200, EF_RUNTIME 128. The dataset (about 3 GB)
# is downloaded on the first run, into /tmp/vectordb_bench/dataset.
vectordbbench kividb \
  --host "$KIVIDB_HOST" --port 6380 --password "$KIVIDB_PASSWORD" \
  --case-type Performance768D1M \
  --db-label "kividb-m16-efc200-ef${EF_RUNTIME:-128}" \
  --m 16 --ef-construction 200 --ef-runtime "${EF_RUNTIME:-128}" \
  --num-concurrency 1,5,10,20,30,40,60,80,100,120 \
  2>&1 | tee "$HOME/vdbbench.log"

# To try another EF_RUNTIME on the index already built, add
# `--skip-drop-old --skip-load` and set EF_RUNTIME.
