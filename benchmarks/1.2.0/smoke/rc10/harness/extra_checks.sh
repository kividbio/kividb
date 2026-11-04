#!/usr/bin/env bash
# extra_checks.sh <tag> : the rc7 checks (rerun unchanged on rc10, except rpm -i on ubi9-minimal) the rc4 harness (smoke_all.sh) does not make, on THIS host's arch.
#   1. image architecture: every multi-arch tag (GHCR, Docker Hub) resolves to this host's arch; every
#      per-arch tag (-amd64 / -arm64, both of them) is the arch it says, by image config and by `file`.
#   2. redis-benchmark (short) against every tarball binary and every image variant from both registries,
#      plus over TLS for the tls/full builds.
#   3. variant features, done precisely: the exact EVAL refusal and that the connection still works after
#      it; TLS with a plain self-signed certificate (no CA, no client certificate).
#   4. the .deb: systemd runs it as the kividb service user; it serves and benchmarks.
#   5. the .rpm: install on clean rpm-based containers WITHOUT pre-installing shadow-utils (the rc4 note),
#      and under systemd (ubi9-init) as its service user.
# Needs: docker, redis-tools, openssl, file, the ORAS files in ~/rc-bin/<tag> (smoke_all.sh pulls them).
# Output: ~/smoke-results/<arch>/extra/*.  Lines: PASS:/FAIL:/INFO:, final "RESULT extra-<arch> <p> <f>".
set -u
TAG=${1:?tag}
WANT=${TAG#v}
ARCH=$(dpkg --print-architecture); case $ARCH in amd64) UN=x86_64; FILEARCH=x86-64 ;; arm64) UN=aarch64; FILEARCH=aarch64 ;; esac
OUT=${OUT:-$HOME/smoke-results/$ARCH}/extra; mkdir -p "$OUT"
B=$HOME/rc-bin/$TAG
P=6381 TP=6391
PASS=0 FAIL=0; FAILED=()
ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1 -- ${2:-}"; FAIL=$((FAIL+1)); FAILED+=("$1"); }
rc()  { redis-cli -h 127.0.0.1 -p "$P" "$@" 2>&1; }
up()  { for _ in $(seq 1 120); do [ "$(redis-cli -h 127.0.0.1 -p "${1:-$P}" PING 2>/dev/null)" = PONG ] && return 0; sleep 0.25; done; return 1; }
BENCH="$OUT/bench.tsv"; [ -f "$BENCH" ] || printf 'artifact\tvariant\ttransport\tPING_MBULK\tSET\tGET\tINCR\tLPUSH\tLPOP\n' > "$BENCH"
bench() { # bench <label> <variant> <transport> [extra redis-benchmark args]
  local label=$1 v=$2 t=$3; shift 3
  local csv; csv=$(timeout 120 redis-benchmark -h 127.0.0.1 "$@" -t ping_mbulk,set,get,incr,lpush,lpop -n 100000 -c 50 -r 10000 -d 64 --csv 2>&1)
  local row; row=$(echo "$csv" | awk -F'","' 'NR>1{gsub(/"/,"",$2); printf "\t%d", $2}')
  local n; n=$(echo "$row" | awk -F'\t' '{print NF-1}')
  printf '%s\t%s\t%s%s\n' "$label" "$v" "$t" "$row" >> "$BENCH"
  if [ "$n" = 6 ] && ! echo "$row" | grep -qP '\t0(\t|$)'; then ok "$label $t redis-benchmark: 6 tests ($row )"; else bad "$label $t redis-benchmark" "$csv"; fi
}
version_info() { rc INFO server | tr -d '\r' | awk -F: '$1=="kividb_version"{print $2}'; }
features() { # features <label> <variant>: EVAL accepted or refused cleanly, connection still usable
  local label=$1 v=$2
  local out; out=$(printf 'EVAL "return 7" 0\nPING\n' | rc)
  case $v in
    lua|full) [ "$out" = "7
PONG" ] && ok "$label: EVAL works" || bad "$label: EVAL" "$out" ;;
    *) local first; first=$(echo "$out" | head -1)
       if echo "$first" | grep -q '^ERR' && [ "$(echo "$out" | tail -1)" = PONG ]; then ok "$label: EVAL refused cleanly [$first], connection still serves"; else bad "$label: EVAL refusal" "$out"; fi ;;
  esac
}
SS=$HOME/selfsigned
if [ ! -f "$SS/self.crt" ]; then
  mkdir -p "$SS"; openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" -keyout "$SS/self.key" -out "$SS/self.crt" 2>/dev/null
  chmod 644 "$SS"/*
fi
tls_selfsigned() { # tls_selfsigned <label>: server started with the self-signed cert on $TP, no client certs asked
  local label=$1
  local T=(redis-cli -h 127.0.0.1 -p "$TP" --tls --cacert "$SS/self.crt")
  local o; o=$("${T[@]}" PING 2>&1)
  [ "$o" = PONG ] && ok "$label: TLS PING with a self-signed cert, no client cert" || { bad "$label: TLS self-signed PING" "$o"; return; }
  [ "$("${T[@]}" SET tlsk tlsv 2>&1)" = OK ] && [ "$("${T[@]}" GET tlsk 2>&1)" = tlsv ] && ok "$label: TLS SET/GET" || bad "$label: TLS SET/GET"
  o=$(redis-cli -h 127.0.0.1 -p "$TP" PING 2>&1); [ "$o" != PONG ] && ok "$label: plaintext on the TLS port refused" || bad "$label: plaintext on TLS port answered"
  o=$(redis-cli -h 127.0.0.1 -p "$TP" --tls PING 2>&1); [ "$o" != PONG ] && ok "$label: TLS client that does not trust the cert fails verification" || bad "$label: untrusted cert accepted by client?"
  bench "$label" "${label##*-}" tls -p "$TP" --tls --cacert "$SS/self.crt"
}

echo "=== extra checks $TAG on $ARCH ==="

# ---------------------------------------------------------------- 1. image architecture
{
for reg in ghcr.io/kividbio/kividb docker.io/kividb/kividb; do
  for v in "" -lua -tls -full; do
    ref=$reg:$TAG$v
    docker pull -q "$ref" >/dev/null 2>&1 || { bad "pull $ref"; continue; }
    a=$(docker image inspect "$ref" --format '{{.Os}}/{{.Architecture}}')
    [ "$a" = "linux/$ARCH" ] && ok "$ref resolves to linux/$ARCH on this host" || bad "$ref arch" "$a"
  done
done
for want in amd64 arm64; do
  for v in "" lua- tls- full-; do
    ref=ghcr.io/kividbio/kividb:$TAG-$v$want
    docker pull -q --platform linux/$want "$ref" >/dev/null 2>&1 || { bad "pull $ref"; continue; }
    a=$(docker image inspect "$ref" --format '{{.Os}}/{{.Architecture}}')
    cid=$(docker create --platform linux/$want "$ref"); docker cp -q "$cid:/usr/local/bin/kividb" /tmp/kv-arch >/dev/null 2>&1; docker rm "$cid" >/dev/null
    f=$(file -b /tmp/kv-arch | cut -d, -f1-2); rm -f /tmp/kv-arch
    case $want in amd64) fa=x86-64 ;; arm64) fa=aarch64 ;; esac
    raw=$(docker buildx imagetools inspect --raw "$ref" 2>/dev/null)
    other=$( [ $want = amd64 ] && echo arm64 || echo amd64)
    if [ "$a" = "linux/$want" ] && echo "$f" | grep -q "$fa" && ! echo "$raw" | grep -q "\"architecture\": *\"$other\""; then
      ok "$ref is linux/$want (config; binary: $f; no $other manifest)"
    else bad "$ref arch" "config=$a file=$f"; fi
  done
done
} 2>&1 | tee "$OUT/arch.log"

# ---------------------------------------------------------------- 2+3. tarball binaries
for v in default lua tls full; do
  pre=$( [ $v = default ] && echo kividb || echo "kividb-$v")
  d=$(mktemp -d); tar xzf "$B/$pre-linux-$UN.tar.gz" -C "$d"
  bin=$d/kividb/kividb; label=tarball-$v
  f=$(file -b "$bin"); echo "$f" | grep -q "$FILEARCH" && ok "$label: binary is $FILEARCH ($(echo "$f" | cut -d, -f1-2))" || bad "$label: arch" "$f"
  "$bin" --version | grep -q -- "$WANT" && ok "$label: --version [$("$bin" --version)]" || bad "$label: --version" "$("$bin" --version 2>&1)"
  args=(--protected-mode no --port $P --dir "$d")
  case $v in tls|full) args+=(--tls-port $TP --tls-cert-file "$SS/self.crt" --tls-key-file "$SS/self.key" --tls-auth-clients no) ;; esac
  (cd "$d" && exec "$bin" "${args[@]}" > "$d/log" 2>&1) & pid=$!
  if up; then
    [ "$(version_info)" = "$WANT" ] && ok "$label: INFO server kividb_version $WANT" || bad "$label: INFO version" "$(version_info)"
    [ "$(rc SET k v)" = OK ] && [ "$(rc GET k)" = v ] && ok "$label: PING/SET/GET" || bad "$label: SET/GET"
    rc INFO server | tr -d '\r' > "$OUT/info-server-$label.txt"
    features "$label" $v
    bench "$label" $v plain -p $P
    case $v in tls|full) tls_selfsigned "$label" ;; esac
  else bad "$label: start" "$(tail -5 "$d/log")"; fi
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; rm -rf "$d"
done 2>&1 | tee "$OUT/tarballs.log"

# ---------------------------------------------------------------- 2+3. images, both registries
for reg in ghcr.io/kividbio/kividb docker.io/kividb/kividb; do
  short=$( [ "${reg%%/*}" = ghcr.io ] && echo ghcr || echo hub)
  for v in default lua tls full; do
    ref=$reg:$TAG$( [ $v = default ] || echo "-$v"); label=image-$short-$v; cn=xc-$label
    docker rm -f "$cn" >/dev/null 2>&1
    args=(--protected-mode no --port 6380)
    case $v in tls|full) args+=(--tls-port 6390 --tls-cert-file /ss/self.crt --tls-key-file /ss/self.key --tls-auth-clients no) ;; esac
    docker run -d --name "$cn" -p 127.0.0.1:$P:6380 -p 127.0.0.1:$TP:6390 -v "$SS":/ss:ro "$ref" "${args[@]}" >/dev/null
    if up; then
      u=$(docker exec "$cn" id -u 2>/dev/null || docker inspect "$cn" --format '{{.Config.User}}')
      ok "$label: runs (user $u)"
      [ "$(version_info)" = "$WANT" ] && ok "$label: INFO server kividb_version $WANT" || bad "$label: INFO version" "$(version_info)"
      dv=$(docker run --rm "$ref" --version 2>&1); echo "$dv" | grep -q -- "$WANT" && ok "$label: --version [$dv]" || bad "$label: --version" "$dv"
      [ "$(rc SET k v)" = OK ] && [ "$(rc GET k)" = v ] && ok "$label: PING/SET/GET" || bad "$label: SET/GET"
      features "$label" $v
      bench "$label" $v plain -p $P
      case $v in tls|full) tls_selfsigned "$label" ;; esac
    else bad "$label: start" "$(docker logs "$cn" 2>&1 | tail -5)"; fi
    docker rm -f "$cn" >/dev/null 2>&1
  done
done 2>&1 | tee "$OUT/images.log"

# ---------------------------------------------------------------- 4. the .deb under systemd
{
deb=$B/kividb_$ARCH.deb
sudo dpkg --purge kividb >/dev/null 2>&1
if sudo dpkg -i "$deb" > "$OUT/deb-install.log" 2>&1; then ok "deb: dpkg -i"; else bad "deb: dpkg -i" "$(tail -3 "$OUT/deb-install.log")"; fi
up 6380 && ok "deb: server up on the packaged port 6380" || bad "deb: up" "$(systemctl status kividb --no-pager | tail -5)"
systemctl is-enabled -q kividb && ok "deb: unit enabled" || bad "deb: unit not enabled"
mp=$(systemctl show -p MainPID --value kividb); pu=$(ps -o user= -p "$mp" | tr -d ' ')
[ "$pu" = kividb ] && ok "deb: MainPID $mp runs as user kividb" || bad "deb: service user" "[$pu]"
getent passwd kividb | grep -q nologin && ok "deb: kividb is a system user with nologin ($(getent passwd kividb))" || bad "deb: user" "$(getent passwd kividb)"
[ "$(stat -c %U /var/lib/kividb)" = kividb ] && ok "deb: /var/lib/kividb owned by kividb" || bad "deb: data dir owner" "$(stat -c %U /var/lib/kividb)"
P=6380
[ "$(version_info)" = "$WANT" ] && ok "deb: INFO server kividb_version $WANT" || bad "deb: version" "$(version_info)"
[ "$(rc SET k v)" = OK ] && [ "$(rc GET k)" = v ] && ok "deb: PING/SET/GET" || bad "deb: SET/GET"
features deb default
bench deb-$ARCH default plain -p 6380
rc SAVE >/dev/null; ls -l /var/lib/kividb 2>/dev/null | sudo tee /dev/null >/dev/null
sudo ls /var/lib/kividb | grep -q . && ok "deb: SAVE writes into /var/lib/kividb ($(sudo ls /var/lib/kividb | tr '\n' ' '))" || bad "deb: SAVE file"
sudo dpkg --purge kividb > /dev/null 2>&1; sleep 1
redis-cli -p 6380 PING >/dev/null 2>&1 && bad "deb: still running after purge" || ok "deb: purge stops the service"
P=6381
} 2>&1 | tee "$OUT/deb.log"

# ---------------------------------------------------------------- 5. the .rpm on clean images, then under systemd
{
rpm=$(readlink -f "$B/kividb.$UN.rpm")
for img in amazonlinux:2023 rockylinux/rockylinux:9 almalinux:9 fedora:41 registry.access.redhat.com/ubi9/ubi registry.access.redhat.com/ubi9/ubi-minimal; do
  cn=xr-$(echo "$img" | tr -c 'a-z0-9\n' '-'); docker rm -f "$cn" >/dev/null 2>&1
  docker pull -q "$img" >/dev/null 2>&1 || { echo "INFO: $img not pullable"; continue; }
  docker run -d --name "$cn" -v "$rpm":/tmp/kividb.rpm:ro "$img" sleep infinity >/dev/null
  pm=$(docker exec "$cn" sh -c 'command -v dnf || command -v microdnf || command -v yum')
  had=$(docker exec "$cn" sh -c 'command -v useradd >/dev/null && echo yes || echo no')
  # microdnf cannot install a local file (rc7 harness fault): use rpm -i there
  case $pm in *microdnf) inst=(rpm -i /tmp/kividb.rpm) ;; *) inst=("$pm" install -y /tmp/kividb.rpm) ;; esac
  if docker exec "$cn" "${inst[@]}" > "$OUT/rpm-clean-$cn.log" 2>&1 && docker exec "$cn" rpm -q kividb >/dev/null 2>&1; then
    ok "rpm: installs on clean $img ($pm; useradd present before: $had)"
  else
    if [ "$had" = no ]; then
      bad "rpm: install on clean $img (no shadow-utils; package does not require it)" "$(grep -m1 -i 'not found\|error' "$OUT/rpm-clean-$cn.log")"
    else
      bad "rpm: install on clean $img" "$(tail -3 "$OUT/rpm-clean-$cn.log")"
    fi
  fi
  docker exec "$cn" rpm -qpR /tmp/kividb.rpm > "$OUT/rpm-requires.txt" 2>/dev/null
  docker rm -f "$cn" >/dev/null 2>&1
done
echo "INFO: rpm -qpR (declared requirements): $(tr '\n' ' ' < "$OUT/rpm-requires.txt")"
# systemd as PID 1 in a UBI 9 init container: the package's unit must start the server as kividb
cn=xr-systemd; docker rm -f "$cn" >/dev/null 2>&1
docker run -d --name "$cn" --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw --tmpfs /run --tmpfs /tmp \
  --network host -v "$rpm":/opt/kividb.rpm:ro registry.access.redhat.com/ubi9/ubi-init >/dev/null
for _ in $(seq 1 40); do docker exec "$cn" systemctl is-system-running 2>/dev/null | grep -qE 'running|degraded' && break; sleep 0.5; done
echo "INFO: systemd in container: $(docker exec "$cn" systemctl is-system-running 2>&1)"
docker exec "$cn" dnf install -y /opt/kividb.rpm > "$OUT/rpm-systemd-install.log" 2>&1 && ok "rpm: dnf install under systemd (ubi9-init)" || bad "rpm: install ubi9-init" "$(tail -3 "$OUT/rpm-systemd-install.log")"
up 6380 && ok "rpm: postinstall enabled+started the unit, PING on 6380" || bad "rpm: unit up" "$(docker exec "$cn" systemctl status kividb --no-pager 2>&1 | tail -6)"
docker exec "$cn" systemctl is-enabled -q kividb && ok "rpm: unit enabled" || bad "rpm: unit not enabled"
mp=$(docker exec "$cn" systemctl show -p MainPID --value kividb); pu=$(docker exec "$cn" ps -o user= -p "$mp" 2>/dev/null | tr -d ' ')
[ -z "$pu" ] && pu=$(docker exec "$cn" stat -c %U /proc/$mp 2>/dev/null)
[ "$pu" = kividb ] && ok "rpm: MainPID runs as user kividb" || bad "rpm: service user" "[$pu]"
P=6380
[ "$(version_info)" = "$WANT" ] && ok "rpm: INFO server kividb_version $WANT" || bad "rpm: version" "$(version_info)"
[ "$(rc SET k v)" = OK ] && [ "$(rc GET k)" = v ] && ok "rpm: PING/SET/GET" || bad "rpm: SET/GET"
features rpm default
bench rpm-$ARCH default plain -p 6380
docker exec "$cn" dnf remove -y kividb > /dev/null 2>&1; sleep 1
redis-cli -p 6380 PING >/dev/null 2>&1 && bad "rpm: still running after dnf remove" || ok "rpm: dnf remove stops the service"
docker rm -f "$cn" >/dev/null 2>&1
P=6381
} 2>&1 | tee "$OUT/rpm.log"

p=$(cat "$OUT"/{arch,tarballs,images,deb,rpm}.log | grep -c '^PASS'); f=$(cat "$OUT"/{arch,tarballs,images,deb,rpm}.log | grep -c '^FAIL')
echo "RESULT extra-$ARCH $p $f" | tee "$OUT/result.txt"
cat "$OUT"/{arch,tarballs,images,deb,rpm}.log | grep '^FAIL' | tee -a "$OUT/result.txt"
echo EXTRA-DONE
