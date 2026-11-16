#!/usr/bin/env bash
# followup.sh <tag>: reruns after the first pass.
#   - the repo's docker/verify/verify-published.sh (the first pass lacked the docker compose plugin)
#   - the rpm on ubi9-minimal with `rpm -i` (microdnf cannot install a local file, a harness fault)
#   - the rpm's declared requirements and pre-install script
set -u
TAG=${1:?tag}
ARCH=$(dpkg --print-architecture); case $ARCH in amd64) UN=x86_64 ;; arm64) UN=aarch64 ;; esac
R=$HOME/smoke-results/$ARCH; B=$HOME/rc-bin/$TAG
rpm=$(readlink -f "$B/kividb.$UN.rpm")
(cd "$HOME/src/docker/verify" && sh ./verify-published.sh "$TAG" ghcr.io/kividbio/kividb) > "$R/repo-verify-published.log" 2>&1
e=$?
echo "repo verify-published.sh (rerun with docker compose) exit=$e $(grep -c '^PASS' "$R/repo-verify-published.log") PASS / $(grep -c '^FAIL' "$R/repo-verify-published.log") FAIL" | tee -a "$R/repo-checks.txt"
{
docker rm -f xr-min >/dev/null 2>&1
docker run -d --name xr-min -v "$rpm":/tmp/kividb.rpm:ro registry.access.redhat.com/ubi9/ubi-minimal sleep infinity >/dev/null
echo "useradd present: $(docker exec xr-min sh -c 'command -v useradd || echo no')"
docker exec xr-min rpm -i /tmp/kividb.rpm 2>&1 | tail -3
docker exec xr-min rpm -q kividb >/dev/null 2>&1 && echo "PASS: rpm -i on clean ubi9-minimal" || echo "FAIL: rpm -i on clean ubi9-minimal (no shadow-utils)"
echo "--- rpm -qpR (requires):"; docker exec xr-min rpm -qpR /tmp/kividb.rpm
echo "--- rpm -qp --scripts (preinstall head):"; docker exec xr-min rpm -qp --scripts /tmp/kividb.rpm | sed -n 1,12p
docker exec xr-min microdnf install -y shadow-utils >/dev/null 2>&1
docker exec xr-min rpm -i /tmp/kividb.rpm 2>&1 | tail -2
docker exec xr-min rpm -q kividb >/dev/null 2>&1 && echo "PASS: rpm -i on ubi9-minimal after microdnf install shadow-utils" || echo "FAIL: rpm -i on ubi9-minimal even with shadow-utils"
docker rm -f xr-min >/dev/null 2>&1
} 2>&1 | tee "$R/extra/rpm-ubi-minimal.log"
echo FOLLOWUP-DONE
