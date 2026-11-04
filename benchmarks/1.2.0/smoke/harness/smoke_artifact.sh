#!/usr/bin/env bash
# Smoke test of ONE published KiviDB artifact.
#
#   smoke_artifact.sh image <image-ref> <variant> <label>     container from a registry
#   smoke_artifact.sh bin   <path/to/kividb> <variant> <label> binary from a release tarball
#   smoke_artifact.sh deb   <file.deb> default <label>          installed with dpkg, run by systemd
#   smoke_artifact.sh rpm   <file.rpm> default <label>          installed with dnf in amazonlinux:2023
#
# variant: default | lua | tls | full (decides which of the Lua / TLS checks must pass or must be refused)
# Env: WANT_VERSION (default 1.2.0-rc4), PORT (6380), TPORT (6390), CERTS (dir made by gen_certs.sh)
#
# Prints one PASS/FAIL line per check and a final "RESULT <label> <pass> <fail>" line.
set -u
KIND=$1 REF=$2 VARIANT=$3 LABEL=$4
WANT_VERSION=${WANT_VERSION:-1.2.0-rc4}
PORT=${PORT:-6380}
TPORT=${TPORT:-6390}
CERTS=${CERTS:-$HOME/certs}
WORK=$(mktemp -d /tmp/smk.XXXX)
PASS=0 FAIL=0
FAILED=()
case $VARIANT in lua|full) HAS_LUA=1 ;; *) HAS_LUA=0 ;; esac
case $VARIANT in tls|full) HAS_TLS=1 ;; *) HAS_TLS=0 ;; esac

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1 -- ${2:-}"; FAIL=$((FAIL+1)); FAILED+=("$1"); }
expect() { # expect <desc> <want> <got>
  if [ "$3" = "$2" ]; then ok "$1"; else bad "$1" "want [$2] got [$3]"; fi
}
rc() { redis-cli -h 127.0.0.1 -p "$PORT" "$@" 2>&1; }
wait_up() {
  for _ in $(seq 1 120); do
    [ "$(rc PING 2>/dev/null)" = "PONG" ] && return 0
    sleep 0.25
  done
  return 1
}

TLS_ARGS=()
if [ $HAS_TLS = 1 ]; then
  # Mutual TLS: the server asks for a client certificate signed by the CA.
  TLS_ARGS=(--tls-port "$TPORT" --tls-cert-file C/server.crt --tls-key-file C/server.key --tls-ca-cert-file C/ca.crt)
fi
args_for() { # args_for <certdir>: server args with C/ replaced by the cert dir as seen by the server
  local out=() a
  for a in "${TLS_ARGS[@]}"; do out+=("${a/C\//$1/}"); done
  echo "${out[@]}"
}

# ---------------------------------------------------------------- lifecycle per kind
case $KIND in
image)
  CN=smk-$LABEL
  docker rm -f "$CN" >/dev/null 2>&1; docker volume rm "$CN" >/dev/null 2>&1
  start() {
    docker run -d --name "$CN" -p 127.0.0.1:$PORT:6380 -p 127.0.0.1:$TPORT:6390 \
      -v "$CN":/home/kividb -v "$CERTS":/certs:ro "$REF" \
      --protected-mode no --port 6380 --aof $(args_for /certs) >/dev/null
  }
  first_start() { start; }
  kill9()   { docker kill -s KILL "$CN" >/dev/null; }
  restart() { docker start "$CN" >/dev/null; }
  graceful(){ docker stop -t 30 "$CN" >/dev/null; docker start "$CN" >/dev/null; }
  version_out() { docker run --rm "$REF" --version 2>&1; }
  logs()    { docker logs "$CN" 2>&1 | tail -20; }
  cleanup() { docker rm -f "$CN" >/dev/null 2>&1; docker volume rm "$CN" >/dev/null 2>&1; }
  refuse_tls() { # a build without TLS must refuse to start when asked for a TLS port
    timeout 20 docker run --rm -v "$CERTS":/certs:ro "$REF" --protected-mode no --port 6399 --tls-port 6398 \
      --tls-cert-file /certs/server.crt --tls-key-file /certs/server.key >/dev/null 2>&1
  }
  ;;
bin)
  DATA=$WORK/data; mkdir -p "$DATA"
  start() {
    (cd "$DATA" && exec "$REF" --protected-mode no --port "$PORT" --dir "$DATA" --aof $(args_for "$CERTS") \
      >>"$WORK/server.log" 2>&1) &
    echo $! > "$WORK/pid"
  }
  first_start() { start; }
  kill9()   { kill -9 "$(cat "$WORK/pid")"; sleep 0.5; }
  restart() { start; }
  graceful(){ rc SHUTDOWN >/dev/null; sleep 1; start; }
  version_out() { "$REF" --version 2>&1; }
  logs()    { tail -20 "$WORK/server.log"; }
  cleanup() { kill -9 "$(cat "$WORK/pid")" 2>/dev/null; rm -rf "$WORK"; }
  refuse_tls() {
    timeout 20 "$REF" --port 6399 --tls-port 6398 --tls-cert-file "$CERTS/server.crt" \
      --tls-key-file "$CERTS/server.key" >/dev/null 2>&1
  }
  ;;
deb)
  [ "$PORT" = 6380 ] || { echo "deb test uses the packaged port 6380"; exit 2; }
  first_start() {
    sudo dpkg -i "$REF" >"$WORK/install.log" 2>&1 || { cat "$WORK/install.log"; return 1; }
    for _ in $(seq 1 40); do systemctl is-active -q kividb && break; sleep 0.25; done
    systemctl is-active -q kividb && ok "deb: installed, unit enabled and active" || bad "deb: unit active" "$(systemctl status kividb 2>&1 | tail -5)"
    wait_up
    # The packaged config has no AOF: turn it on the way an operator would.
    echo "appendonly yes" | sudo tee -a /etc/kividb/kividb.conf >/dev/null
    sudo systemctl restart kividb
  }
  kill9()   { sudo systemctl kill -s KILL kividb; sleep 0.3; }
  restart() { :; } # Restart=on-failure brings it back by itself after 2 s: that is part of the test
  graceful(){ sudo systemctl restart kividb; }
  version_out() { /usr/bin/kividb --version 2>&1; }
  logs()    { sudo journalctl -u kividb --no-pager | tail -20; }
  cleanup() { sudo dpkg --purge kividb >/dev/null 2>&1; sudo rm -rf /var/lib/kividb /etc/kividb; rm -rf "$WORK"; }
  refuse_tls() { false; }
  ;;
rpm)
  CN=smk-$LABEL
  docker rm -f "$CN" >/dev/null 2>&1
  first_start() {
    docker run -d --name "$CN" --network host -v "$(readlink -f "$REF")":/tmp/kividb.rpm:ro amazonlinux:2023 sleep infinity >/dev/null
    docker exec "$CN" dnf install -y -q procps-ng >/dev/null 2>&1
    # The container image is minimal: no shadow-utils (groupadd/useradd), which every full AL2023 / RHEL host
    # has. Recorded as a finding, not as a failure of the package on a real host.
    if docker exec "$CN" dnf install -y -q /tmp/kividb.rpm >"$WORK/install-minimal.log" 2>&1; then
      echo "NOTE: rpm installs on the minimal amazonlinux:2023 image"
    else
      echo "NOTE: rpm does not install on the minimal amazonlinux:2023 image (no shadow-utils; the package does not require it): $(grep -m1 -i 'not found\|error' "$WORK/install-minimal.log")"
      docker exec "$CN" dnf install -y -q shadow-utils >/dev/null 2>&1
    fi
    docker exec "$CN" rpm -q kividb >/dev/null 2>&1 || docker exec "$CN" dnf install -y -q /tmp/kividb.rpm >"$WORK/install.log" 2>&1
    docker exec "$CN" rpm -q kividb >/dev/null 2>&1 \
      && ok "rpm: dnf install on Amazon Linux 2023 (with shadow-utils, as on a full host)" || { bad "rpm: dnf install" "$(tail -5 "$WORK/install.log")"; return 1; }
    docker exec "$CN" test -x /usr/bin/kividb && docker exec "$CN" id kividb >/dev/null 2>&1 \
      && ok "rpm: /usr/bin/kividb and the kividb user exist" || bad "rpm: files/user"
    start
  }
  start() {
    docker exec -d -u kividb "$CN" sh -c "cd /var/lib/kividb && exec /usr/bin/kividb --port $PORT --aof >>/tmp/server.log 2>&1"
  }
  kill9()   { docker exec "$CN" pkill -9 -x kividb; sleep 0.5; }
  restart() { start; }
  graceful(){ rc SHUTDOWN >/dev/null; sleep 1; start; }
  version_out() { docker exec "$CN" /usr/bin/kividb --version 2>&1; }
  logs()    { docker exec "$CN" tail -20 /tmp/server.log; }
  cleanup() { docker rm -f "$CN" >/dev/null 2>&1; rm -rf "$WORK"; }
  refuse_tls() { false; }
  ;;
*) echo "unknown kind $KIND"; exit 2 ;;
esac

echo "=== $LABEL ($KIND $REF, variant $VARIANT) ==="
first_start
if wait_up; then ok "starts and answers PING with PONG"; else bad "start" "$(logs)"; echo "RESULT $LABEL $PASS $FAIL"; cleanup; exit 1; fi

# ---------------------------------------------------------------- version
v=$(version_out)
echo "$v" | grep -q -- "$WANT_VERSION" && ok "--version says $WANT_VERSION ($v)" || bad "--version" "$v"
expect "INFO kividb_version" "$WANT_VERSION" "$(rc INFO server | tr -d '\r' | awk -F: '$1=="kividb_version"{print $2}')"

# ---------------------------------------------------------------- INFO
info=$(rc INFO all | tr -d '\r')
for s in Server Clients Memory Persistence Stats Replication CPU Keyspace; do
  echo "$info" | grep -q "^# $s" && ok "INFO has # $s" || bad "INFO section $s"
done
expect "INFO aof_enabled" "1" "$(echo "$info" | awk -F: '$1=="aof_enabled"{print $2}')"
expect "INFO role" "master" "$(echo "$info" | awk -F: '$1=="role"{print $2}')"

# ---------------------------------------------------------------- commands of each type
rc FLUSHALL >/dev/null
expect "string SET"      OK      "$(rc SET s1 hello)"
expect "string GET"      hello   "$(rc GET s1)"
expect "string APPEND"   11      "$(rc APPEND s1 ' world')"
expect "string INCRBY"   42      "$(rc INCRBY c 42)"
expect "string MSET/MGET" "a
b"  "$(rc MSET m1 a m2 b >/dev/null; rc MGET m1 m2)"
big=$(head -c 1048576 /dev/zero | tr '\0' x)
expect "string 1 MB value" 1048576 "$(printf %s "$big" | rc -x SET big >/dev/null; rc STRLEN big)"
expect "expiry SET EX" OK "$(rc SET e1 v EX 100)"
t=$(rc TTL e1); [ "$t" -gt 90 ] 2>/dev/null && [ "$t" -le 100 ] && ok "expiry TTL ($t)" || bad "expiry TTL" "$t"
rc SET e2 v PX 200 >/dev/null; sleep 0.5
expect "expiry key gone after PX 200" 0 "$(rc EXISTS e2)"
expect "hash HSET"    2   "$(rc HSET h f1 v1 f2 v2)"
expect "hash HGET"    v1  "$(rc HGET h f1)"
expect "hash HINCRBY" 5   "$(rc HINCRBY h n 5)"
expect "hash HLEN"    3   "$(rc HLEN h)"
expect "list RPUSH"   3   "$(rc RPUSH l a b c)"
expect "list LRANGE"  "a
b
c" "$(rc LRANGE l 0 -1)"
expect "list LPOP"    a   "$(rc LPOP l)"
expect "set SADD"     3   "$(rc SADD st a b c)"
expect "set SISMEMBER" 1  "$(rc SISMEMBER st b)"
expect "set SCARD"    3   "$(rc SCARD st)"
expect "zset ZADD"    3   "$(rc ZADD z 1 a 2 b 3 c)"
expect "zset ZRANGE WITHSCORES" "a
1
b
2
c
3" "$(rc ZRANGE z 0 -1 WITHSCORES)"
expect "zset ZSCORE"  2   "$(rc ZSCORE z b)"
x=$(rc XADD x '*' f v); echo "$x" | grep -qE '^[0-9]+-[0-9]+$' && ok "stream XADD ($x)" || bad "stream XADD" "$x"
expect "stream XLEN"  1   "$(rc XLEN x)"
expect "hll PFADD"    1   "$(rc PFADD hl a b c)"
expect "hll PFCOUNT"  3   "$(rc PFCOUNT hl)"
rc SETBIT bm 7 1 >/dev/null
expect "bitmap BITCOUNT" 1 "$(rc BITCOUNT bm)"
rc GEOADD g 13.361389 38.115556 Palermo 15.087269 37.502669 Catania >/dev/null
expect "geo GEODIST km" 166.2742 "$(rc GEODIST g Palermo Catania km)"
expect "json JSON.SET" OK "$(rc JSON.SET j '$' '{"a":1,"b":[1,2]}')"
expect "json JSON.GET" '[1]' "$(rc JSON.GET j '$.a')"
expect "transaction MULTI/EXEC" "1
2" "$(printf 'MULTI\nINCR t\nINCR t\nEXEC\n' | rc | tail -2)"
expect "vector set VADD" 1 "$(rc VADD vs VALUES 3 1 0 0 a)"
rc VADD vs VALUES 3 0 1 0 b >/dev/null
expect "vector set VSIM" a "$(rc VSIM vs VALUES 3 0.9 0.1 0 COUNT 1)"

# ---------------------------------------------------------------- search
expect "FT.CREATE" OK "$(rc FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT n NUMERIC SORTABLE tag TAG)"
rc HSET doc:1 title "hello world" n 5 tag red >/dev/null
rc HSET doc:2 title "goodbye moon" n 50 tag blue >/dev/null
sleep 0.3
expect "FT.SEARCH text"    1 "$(rc FT.SEARCH idx hello NOCONTENT | head -1)"
expect "FT.SEARCH numeric" 1 "$(rc FT.SEARCH idx '@n:[1 10]' NOCONTENT | head -1)"
expect "FT.SEARCH tag"     doc:2 "$(rc FT.SEARCH idx '@tag:{blue}' NOCONTENT | sed -n 2p)"

# ---------------------------------------------------------------- Lua
if [ $HAS_LUA = 1 ]; then
  expect "Lua EVAL redis.call" OK "$(rc EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 lk lv)"
  expect "Lua EVAL result" lv "$(rc GET lk)"
  sha=$(rc SCRIPT LOAD "return ARGV[1]")
  expect "Lua EVALSHA" hi "$(rc EVALSHA "$sha" 0 hi)"
  expect "Lua FUNCTION LOAD" mylib "$(rc FUNCTION LOAD "#!lua name=mylib
redis.register_function('myf', function(keys, args) return args[1] end)")"
  expect "Lua FCALL" yo "$(rc FCALL myf 0 yo)"
else
  rc EVAL "return 1" 0 | grep -qi "ERR\|unknown" && ok "no Lua in this variant: EVAL refused" || bad "EVAL should be refused" "$(rc EVAL 'return 1' 0)"
fi

# ---------------------------------------------------------------- TLS
if [ $HAS_TLS = 1 ]; then
  T=(redis-cli -h 127.0.0.1 -p "$TPORT" --tls --cacert "$CERTS/ca.crt" --cert "$CERTS/client.crt" --key "$CERTS/client.key")
  expect "TLS PING (mutual TLS)" PONG "$("${T[@]}" PING 2>&1)"
  expect "TLS SET/GET" tv "$("${T[@]}" SET tk tv >/dev/null 2>&1; "${T[@]}" GET tk 2>&1)"
  out=$(redis-cli -h 127.0.0.1 -p "$TPORT" --tls --cacert "$CERTS/ca.crt" PING 2>&1)
  [ "$out" != "PONG" ] && ok "TLS without a client certificate refused" || bad "TLS no-client-cert should be refused" "$out"
  expect "TLS INFO tls_port" "$( [ $KIND = image ] && echo 6390 || echo "$TPORT")" "$(rc CONFIG GET tls-port | sed -n 2p)"
elif [ $KIND = image ] || [ $KIND = bin ]; then
  if refuse_tls; then bad "a build without TLS must refuse --tls-port" "it started"; else ok "no TLS in this variant: --tls-port refused at start"; fi
fi

# ---------------------------------------------------------------- persistence: AOF on, kill -9, restart
expect "CONFIG GET appendonly" yes "$(rc CONFIG GET appendonly | sed -n 2p)"
seq 1 20000 | awk '{k="key:"$1; v="val-"$1; printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length(v), v}' \
  | rc --pipe >/dev/null
state() { # a fingerprint of the data set
  {
    rc DBSIZE; rc GET s1; rc GET c; rc STRLEN big; rc HGETALL h | paste - - | sort; rc LRANGE l 0 -1
    rc SMEMBERS st | sort; rc ZRANGE z 0 -1 WITHSCORES; rc XLEN x; rc PFCOUNT hl; rc BITCOUNT bm
    rc JSON.GET j; rc GET key:1; rc GET key:12345; rc GET key:20000; rc TTL e1 | sed 's/^[0-9]\{1,2\}$/ttl-ok/'
    rc VCARD vs; rc FT.SEARCH idx hello NOCONTENT
  } | md5sum | cut -c1-12
}
before=$(state); dbs=$(rc DBSIZE)
kill9; restart
if wait_up; then ok "restarts after kill -9"; else bad "restart after kill -9" "$(logs)"; fi
for _ in $(seq 1 40); do [ "$(rc DBSIZE)" = "$dbs" ] && break; sleep 0.25; done
expect "kill -9 + AOF replay: DBSIZE back ($dbs)" "$dbs" "$(rc DBSIZE)"
expect "kill -9 + AOF replay: every checked value back" "$before" "$(state)"
graceful
if wait_up; then ok "restarts after a clean shutdown"; else bad "restart after shutdown" "$(logs)"; fi
for _ in $(seq 1 40); do [ "$(rc DBSIZE)" = "$dbs" ] && break; sleep 0.25; done
expect "clean shutdown + snapshot load: every checked value back" "$before" "$(state)"
expect "INFO still says version after restarts" "$WANT_VERSION" "$(rc INFO server | tr -d '\r' | awk -F: '$1=="kividb_version"{print $2}')"

echo "RESULT $LABEL $PASS $FAIL ${FAILED[*]:-}"
cleanup
[ "$FAIL" = 0 ]
