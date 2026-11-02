#!/usr/bin/env bash
# Smoke-test every published artifact of a release tag that runs on THIS machine's architecture.
#   smoke_all.sh v1.2.0-rc4 [repo-checkout]
# Run once on an amd64 host and once on an arm64 host. Needs: docker (+ buildx, compose), redis-tools, openssl, curl.
# Writes results/<arch>/ : one log per artifact, summary.tsv, manifests.txt, sha256 check of the binaries.
set -u
TAG=${1:?tag}
SRC=${2:-$HOME/src}
H=$(cd "$(dirname "$0")" && pwd)
ARCH=$(dpkg --print-architecture)            # amd64 | arm64
case $ARCH in amd64) UN=x86_64 ;; arm64) UN=aarch64 ;; esac
export WANT_VERSION=${TAG#v}
OUT=${OUT:-$HOME/smoke-results/$ARCH}
mkdir -p "$OUT"; : > "$OUT/summary.tsv"
export CERTS=$HOME/certs
bash "$H/gen_certs.sh" "$CERTS" >/dev/null

run() { # run <label> <kind> <ref> <variant>
  local label=$1-$ARCH
  bash "$H/smoke_artifact.sh" "$2" "$3" "$4" "$label" > "$OUT/$label.log" 2>&1
  local line; line=$(grep '^RESULT' "$OUT/$label.log" | tail -1)
  local p f; p=$(echo "$line" | awk '{print $3}'); f=$(echo "$line" | awk '{print $4}')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$2" "$3" "$4" "${p:-0}" "${f:-?}" \
    "$(echo "$line" | cut -d' ' -f5-)" >> "$OUT/summary.tsv"
  echo "$label pass=${p:-0} fail=${f:-?}"
}

# ---- 1. image manifests: both registries, every variant, both architectures, same digests
{
  for v in "" -lua -tls -full; do
    for reg in ghcr.io/kividbio/kividb docker.io/kividb/kividb; do
      ref=$reg:$TAG$v
      d=$(docker buildx imagetools inspect "$ref" --format '{{json .Manifest.Digest}}' 2>&1 | tr -d '"')
      plats=$(docker buildx imagetools inspect "$ref" --format '{{range .Manifest.Manifests}}{{.Platform.OS}}/{{.Platform.Architecture}} {{end}}' 2>&1)
      echo -e "$ref\t$d\t$plats"
    done
  done
} > "$OUT/manifests.txt"
cat "$OUT/manifests.txt"

# ---- 2. every image variant from both registries, pulled for this architecture
for reg in ghcr.io/kividbio/kividb docker.io/kividb/kividb; do
  short=$( [ "${reg%%/*}" = ghcr.io ] && echo ghcr || echo hub)
  for v in default lua tls full; do
    suffix=$( [ $v = default ] && echo "" || echo "-$v")
    docker pull -q "$reg:$TAG$suffix" >/dev/null 2>&1
    run "image-$short-$v" image "$reg:$TAG$suffix" $v
  done
done
# the per-architecture source tags on GHCR (what the manifests point at)
for v in default lua tls full; do
  infix=$( [ $v = default ] && echo "" || echo "$v-")
  docker pull -q "ghcr.io/kividbio/kividb:$TAG-$infix$ARCH" >/dev/null 2>&1
  run "image-ghcr-arch-$v" image "ghcr.io/kividbio/kividb:$TAG-$infix$ARCH" $v
done

# ---- 3. binaries and packages (a release candidate publishes them to GHCR with ORAS, not as a GitHub Release)
B=$HOME/rc-bin/$TAG
mkdir -p "$B"
if [ ! -f "$B/SHA256SUMS.txt" ]; then
  (cd "$B" && oras pull "ghcr.io/kividbio/kividb-binaries:$TAG" >/dev/null 2>&1)
fi
if [ ! -f "$B/SHA256SUMS.txt" ]; then
  # Stable releases: GitHub Release assets
  (cd "$B" && gh release download "$TAG" -R kividbio/kividb 2>/dev/null)
fi
(cd "$B" && ls -l && sha256sum -c SHA256SUMS.txt) > "$OUT/binaries-sha256.txt" 2>&1
cat "$OUT/binaries-sha256.txt"
for v in default lua tls full; do
  pre=$( [ $v = default ] && echo kividb || echo "kividb-$v")
  tgz=$B/$pre-linux-$UN.tar.gz
  d=$HOME/rc-bin/x-$v; rm -rf "$d"; mkdir -p "$d"
  if tar xzf "$tgz" -C "$d" 2>/dev/null; then
    run "tarball-$v" bin "$d/kividb/kividb" $v
  else
    printf 'tarball-%s-%s\tbin\t%s\t%s\t0\t1\tmissing\n' $v "$ARCH" "$tgz" $v >> "$OUT/summary.tsv"
  fi
done
deb=$B/kividb_$ARCH.deb
[ -f "$deb" ] && run deb deb "$deb" default || printf 'deb-%s\tdeb\t%s\tdefault\t0\t1\tmissing\n' "$ARCH" "$deb" >> "$OUT/summary.tsv"
rpm=$B/kividb.$UN.rpm
[ -f "$rpm" ] && run rpm rpm "$rpm" default || printf 'rpm-%s\trpm\t%s\tdefault\t0\t1\tmissing\n' "$ARCH" "$rpm" >> "$OUT/summary.tsv"

# ---- 4. the repo's own published-image checks (smoke-test.yml's container-verify and replication-blackbox jobs)
if [ -d "$SRC/docker/verify" ]; then
  (cd "$SRC/docker/verify" && sh ./verify-published.sh "$TAG" ghcr.io/kividbio/kividb) > "$OUT/repo-verify-published.log" 2>&1
  echo "repo verify-published.sh exit=$? $(grep -c '^PASS' "$OUT/repo-verify-published.log") PASS / $(grep -c '^FAIL' "$OUT/repo-verify-published.log") FAIL" | tee -a "$OUT/repo-checks.txt"
  bash "$SRC/smoke/replication_blackbox.sh" "ghcr.io/kividbio/kividb:$TAG" > "$OUT/repo-replication-blackbox.log" 2>&1
  echo "repo replication_blackbox.sh exit=$? $(grep -c '^PASS' "$OUT/repo-replication-blackbox.log") PASS / $(grep -c '^FAIL' "$OUT/repo-replication-blackbox.log") FAIL" | tee -a "$OUT/repo-checks.txt"
fi
echo SMOKE-ALL-DONE
