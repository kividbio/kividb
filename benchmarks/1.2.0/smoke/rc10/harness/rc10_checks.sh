#!/usr/bin/env bash
# rc10_checks.sh <tag> : the checks for what changed between rc7 and rc10, on THIS host's arch.
#   1. CONFIG GET repl-backlog-size (64 MB = 67108864) and shutdown-timeout (10) on every artifact:
#      4 tarballs, 4 GHCR images, 4 Docker Hub images, the .deb (systemd) and the .rpm (systemd).
#   2. kividb.lock in the data directory:
#      a. tarball: a second server in the same directory waits, logs that the directory is in use and
#         does not serve; it takes over with the data once the first stops (SHUTDOWN), and also after a
#         kill -9 of the first (the kernel drops the flock).
#      (systemctl starts are spaced 3 s apart: systemd refuses a sixth start within 10 s, start-limit-hit)
#      b. image: two containers on one volume, the same; `docker stop` exits 0 well inside the 10 s grace.
#      c. .deb under systemd and .rpm under systemd (ubi9-init): start, `systemctl restart` x5 (each timed,
#         none may hang), `systemctl stop`, start, SIGKILL + Restart=on-failure, the lock file in
#         /var/lib/kividb; a second server started by hand as user kividb waits and logs "in use", then
#         takes over when the unit stops.
#   3. The .rpm on clean amazonlinux:2023 (no shadow-utils) with dnf install, and that it now declares
#      shadow-utils. (extra_checks.sh repeats the clean-image matrix.)
# Needs: docker, redis-tools, the ORAS files in ~/rc-bin/<tag> (smoke_all.sh pulls them).
# Output: ~/smoke-results/<arch>/rc10/*.log, result.txt. Lines PASS:/FAIL:/INFO:.
set -u
TAG=${1:?tag}
WANT=${TAG#v}
ARCH=$(dpkg --print-architecture); case $ARCH in amd64) UN=x86_64 ;; arm64) UN=aarch64 ;; esac
OUT=${OUT:-$HOME/smoke-results/$ARCH}/rc10; mkdir -p "$OUT"
B=$HOME/rc-bin/$TAG
if [ ! -f "$B/SHA256SUMS.txt" ]; then mkdir -p "$B"; (cd "$B" && oras pull "ghcr.io/kividbio/kividb-binaries:$TAG" >/dev/null 2>&1); fi
WANT_BACKLOG=67108864 WANT_ST=10
ok()  { echo "PASS: $1"; }
bad() { echo "FAIL: $1 -- ${2:-}"; }
inf() { echo "INFO: $1"; }
pong() { [ "$(redis-cli -h 127.0.0.1 -p "$1" PING 2>/dev/null)" = PONG ]; }
up()  { local i; for i in $(seq 1 "${2:-120}"); do pong "$1" && return 0; sleep 0.25; done; return 1; }
down(){ local i; for i in $(seq 1 "${2:-120}"); do pong "$1" || return 0; sleep 0.25; done; return 1; }
cfg() { # cfg <label> <port>: CONFIG GET of the two new defaults
  local label=$1 p=$2 b s
  b=$(redis-cli -h 127.0.0.1 -p "$p" CONFIG GET repl-backlog-size 2>&1 | tr -d '\r' | sed -n 2p)
  s=$(redis-cli -h 127.0.0.1 -p "$p" CONFIG GET shutdown-timeout 2>&1 | tr -d '\r' | sed -n 2p)
  [ "$b" = $WANT_BACKLOG ] && ok "$label: CONFIG GET repl-backlog-size = $b (64 MB)" || bad "$label: repl-backlog-size" "[$b]"
  [ "$s" = $WANT_ST ] && ok "$label: CONFIG GET shutdown-timeout = $s" || bad "$label: shutdown-timeout" "[$s]"
}
load() { redis-benchmark -h 127.0.0.1 -p "$1" -t set -n 20000 -r 1000000000 -d 64 -q >/dev/null 2>&1; }
dbsize() { redis-cli -h 127.0.0.1 -p "$1" DBSIZE 2>/dev/null | tr -d '\r'; }
INUSE='is in use by another process'
el()  { awk -v a="$1" -v b="$(date +%s.%N)" 'BEGIN{printf "%.2f", b-a}'; }   # seconds since $1
lt15(){ awk -v t="$1" 'BEGIN{exit !(t<15)}'; }

echo "=== rc10 checks $TAG on $ARCH ==="

# ---------------------------------------------------------------- 1+2a. tarballs
{
for v in default lua tls full; do
  pre=$( [ $v = default ] && echo kividb || echo "kividb-$v")
  d=$(mktemp -d); tar xzf "$B/$pre-linux-$UN.tar.gz" -C "$d"; bin=$d/kividb/kividb; data=$d/data; mkdir -p "$data"
  label=tarball-$v
  (cd "$data" && exec "$bin" --protected-mode no --port 6381 --dir "$data" --aof --update-check no > "$d/a.log" 2>&1) & pa=$!
  if up 6381; then
    "$bin" --version | grep -q -- "$WANT" && ok "$label: --version has $WANT" || bad "$label: --version" "$("$bin" --version)"
    cfg "$label" 6381
    if [ $v = default ]; then
      # a second server in the same directory, on another port
      load 6381; n1=$(dbsize 6381)
      (cd "$data" && exec "$bin" --protected-mode no --port 6382 --dir "$data" --aof --update-check no > "$d/b.log" 2>&1) & pb=$!
      sleep 3
      grep -q "$INUSE" "$d/b.log" && ok "$label: a second server in the same dir logs [$(grep -m1 -o 'The data directory.*waiting for it to be free' "$d/b.log" | sed "s#$d#<dir>#")]" || bad "$label: second server did not log in-use" "$(tail -3 "$d/b.log")"
      pong 6382 && bad "$label: second server serves while the first holds the dir" || ok "$label: the second server does not serve while it waits"
      kill -0 $pb 2>/dev/null && ok "$label: the second server keeps waiting (still running after 3 s)" || bad "$label: second server exited" "$(tail -3 "$d/b.log")"
      t0=$(date +%s.%N); redis-cli -p 6381 SHUTDOWN >/dev/null 2>&1; wait $pa 2>/dev/null
      if up 6382 80; then
        t=$(el $t0)
        ok "$label: after SHUTDOWN of the first, the second starts serving in ${t}s"
        grep -q 'data directory is free after' "$d/b.log" && ok "$label: it logs [$(grep -m1 -o 'The data directory is free after.*' "$d/b.log")]" || bad "$label: no 'free after' line"
        n2=$(dbsize 6382); [ "$n2" = "$n1" ] && ok "$label: the second server has the first one's $n1 keys" || bad "$label: keys after takeover" "first $n1, second $n2"
      else bad "$label: second server never took over after SHUTDOWN" "$(tail -3 "$d/b.log")"; fi
      # now the second holds it; start a third, kill -9 the second
      (cd "$data" && exec "$bin" --protected-mode no --port 6383 --dir "$data" --aof --update-check no > "$d/c.log" 2>&1) & pc=$!
      sleep 2; pong 6383 && bad "$label: third server serves while the second holds the dir" || ok "$label: a third server waits while the second holds the dir"
      kill -9 $pb; wait $pb 2>/dev/null
      up 6383 40 && ok "$label: after kill -9 of the holder, the waiting server takes over" || bad "$label: no takeover after kill -9" "$(tail -3 "$d/c.log")"
      n3=$(dbsize 6383); [ "$n3" = "$n1" ] && ok "$label: and has the $n1 keys (AOF replay)" || bad "$label: keys after kill -9 takeover" "want $n1 got $n3"
      [ -f "$data/kividb.lock" ] && ok "$label: kividb.lock is in the data directory" || bad "$label: no kividb.lock" "$(ls "$data")"
      redis-cli -p 6383 SHUTDOWN >/dev/null 2>&1; wait $pc 2>/dev/null
      cp "$d"/b.log "$OUT/tarball-second-server.log"; sed -i "s#$d#<dir>#g" "$OUT/tarball-second-server.log"
    else
      redis-cli -p 6381 SHUTDOWN >/dev/null 2>&1; wait $pa 2>/dev/null
    fi
  else bad "$label: start" "$(tail -5 "$d/a.log")"; fi
  kill -9 $pa 2>/dev/null; wait 2>/dev/null; rm -rf "$d"
done
} 2>&1 | tee "$OUT/tarballs.log"

# ---------------------------------------------------------------- 1+2b. images
{
for reg in ghcr.io/kividbio/kividb docker.io/kividb/kividb; do
  short=$( [ "${reg%%/*}" = ghcr.io ] && echo ghcr || echo hub)
  for v in default lua tls full; do
    ref=$reg:$TAG$( [ $v = default ] || echo "-$v"); label=image-$short-$v; cn=r10-$label
    docker pull -q "$ref" >/dev/null 2>&1
    docker rm -f "$cn" >/dev/null 2>&1
    docker run -d --name "$cn" -p 127.0.0.1:6381:6380 "$ref" --protected-mode no --port 6380 >/dev/null
    if up 6381; then cfg "$label" 6381; else bad "$label: start" "$(docker logs "$cn" 2>&1 | tail -3)"; fi
    docker rm -f "$cn" >/dev/null 2>&1
  done
done
# two containers on one volume (GHCR default image)
ref=ghcr.io/kividbio/kividb:$TAG; vol=r10-shared
docker rm -f r10-a r10-b >/dev/null 2>&1; docker volume rm $vol >/dev/null 2>&1
docker run -d --name r10-a -p 127.0.0.1:6381:6380 -v $vol:/home/kividb "$ref" --protected-mode no --port 6380 --aof >/dev/null
up 6381 && load 6381; n1=$(dbsize 6381)
docker run -d --name r10-b -p 127.0.0.1:6382:6380 -v $vol:/home/kividb "$ref" --protected-mode no --port 6380 --aof >/dev/null
sleep 3
docker logs r10-b 2>&1 | grep -q "$INUSE" && ok "image: a second container on the same volume logs [$(docker logs r10-b 2>&1 | grep -m1 -o 'The data directory.*waiting for it to be free')]" || bad "image: second container did not log in-use" "$(docker logs r10-b 2>&1 | tail -3)"
pong 6382 && bad "image: second container serves while the first holds the volume" || ok "image: the second container does not serve while it waits"
t0=$(date +%s.%N); docker stop r10-a >/dev/null; t=$(el $t0)
ec=$(docker inspect r10-a --format '{{.State.ExitCode}}')
[ "$ec" = 0 ] && ok "image: docker stop of the holder took ${t}s, exit code 0 (not killed at the 10 s grace)" || bad "image: docker stop" "exit $ec after ${t}s"
if up 6382 80; then
  ok "image: the second container takes over after the first stops"
  n2=$(dbsize 6382); [ "$n2" = "$n1" ] && ok "image: and has the first one's $n1 keys" || bad "image: keys after takeover" "first $n1 second $n2"
else bad "image: second container never took over" "$(docker logs r10-b 2>&1 | tail -3)"; fi
docker logs r10-b > "$OUT/image-second-container.log" 2>&1
t0=$(date +%s.%N); docker restart r10-b >/dev/null; up 6382 80 && ok "image: docker restart, back in $(el $t0)s" || bad "image: docker restart"
docker rm -f r10-a r10-b >/dev/null 2>&1; docker volume rm $vol >/dev/null 2>&1
} 2>&1 | tee "$OUT/images.log"

# ---------------------------------------------------------------- 2c. packages under systemd
# unit_checks <label> : uses ex (run as root on the host or in the container) and port 6380
unit_checks() {
  local label=$1 i t t0 n1 n2 st since
  # the host journal outlives dpkg --purge: read only this run's lines
  since=$(ex date '+%Y-%m-%d %H:%M:%S')
  ex systemctl is-active -q kividb && up 6380 && ok "$label: unit active and serving after install" || { bad "$label: unit not up" "$(ex systemctl status kividb --no-pager 2>&1 | tail -5)"; return; }
  cfg "$label" 6380
  ex test -f /var/lib/kividb/kividb.lock && ok "$label: /var/lib/kividb/kividb.lock exists ($(ex stat -c '%U %a' /var/lib/kividb/kividb.lock))" || bad "$label: no lock file" "$(ex ls -la /var/lib/kividb)"
  # persistence for the data checks, as an operator would turn it on
  ex sh -c 'grep -q "^appendonly yes" /etc/kividb/kividb.conf || echo "appendonly yes" >> /etc/kividb/kividb.conf'
  ex systemctl restart kividb; up 6380 || bad "$label: up after enabling the AOF"
  load 6380; n1=$(dbsize 6380); inf "$label: $n1 keys loaded"
  # 3 s apart: systemd's default start limit (StartLimitBurst=5 per StartLimitIntervalSec=10s) refuses a
  # sixth start inside 10 s with start-limit-hit, whatever the service (the first pass ran them back to back)
  for i in 1 2 3 4 5; do
    sleep 3; t0=$(date +%s.%N); timeout 60 "${EX[@]}" systemctl restart kividb; st=$?; t=$(el $t0)
    if [ $st = 0 ] && up 6380 80; then
      lt15 $t && ok "$label: systemctl restart #$i returned in ${t}s, serving" || bad "$label: systemctl restart #$i slow" "${t}s"
    else bad "$label: systemctl restart #$i hung or failed" "exit $st after ${t}s"; fi
  done
  n2=$(dbsize 6380); [ "$n2" = "$n1" ] && ok "$label: $n1 keys kept across 5 restarts" || bad "$label: keys across restarts" "$n1 -> $n2"
  ex journalctl -u kividb --no-pager --since "$since" 2>/dev/null | grep -q "$INUSE" && bad "$label: a restart had to wait for the lock" "$(ex journalctl -u kividb --no-pager --since "$since" | grep -m2 "$INUSE")" || ok "$label: no restart waited for the lock (journal has no in-use line)"
  t0=$(date +%s.%N); timeout 60 "${EX[@]}" systemctl stop kividb; st=$?; t=$(el $t0)
  [ $st = 0 ] && ! ex systemctl is-active -q kividb && lt15 $t && ok "$label: systemctl stop in ${t}s, unit inactive ($(ex systemctl show -p Result --value kividb))" || bad "$label: systemctl stop" "exit $st, ${t}s, $(ex systemctl is-active kividb)"
  sleep 3; ex systemctl start kividb; up 6380 && ok "$label: systemctl start again, serving" || bad "$label: start after stop"
  # SIGKILL: Restart=on-failure brings it back, the kernel has released the lock
  sleep 3; ex systemctl kill -s KILL kividb; sleep 0.5
  up 6380 60 && ok "$label: after SIGKILL, Restart=on-failure brings it back (lock released)" || bad "$label: no restart after SIGKILL" "$(ex systemctl status kividb --no-pager 2>&1 | tail -4)"
  n2=$(dbsize 6380); [ "$n2" = "$n1" ] && ok "$label: $n1 keys after SIGKILL + restart" || bad "$label: keys after SIGKILL" "$n1 -> $n2"
  # a second server by hand, as user kividb, in the unit's data directory
  ex rm -f /tmp/second.log
  exd "cd /var/lib/kividb && exec timeout 120 runuser -u kividb -- /usr/bin/kividb --port 6399 --bind 127.0.0.1 --dir /var/lib/kividb --aof --update-check no > /tmp/second.log 2>&1"
  sleep 3
  ex grep -q "$INUSE" /tmp/second.log && ok "$label: a second server started by hand logs [$(ex grep -m1 -o 'The data directory.*waiting for it to be free' /tmp/second.log)]" || bad "$label: second server did not log in-use" "$(ex tail -3 /tmp/second.log)"
  pong 6399 && bad "$label: the second server serves while the unit holds the dir" || ok "$label: the second server does not serve while it waits"
  pong 6380 && ok "$label: the unit keeps serving meanwhile" || bad "$label: the unit stopped serving"
  t0=$(date +%s.%N); timeout 60 "${EX[@]}" systemctl stop kividb; t=$(el $t0)
  if up 6399 80; then
    ok "$label: systemctl stop (${t}s) lets the waiting server take over"
    ex grep -q 'data directory is free after' /tmp/second.log && ok "$label: it logs [$(ex grep -m1 -o 'The data directory is free after.*' /tmp/second.log)]" || bad "$label: no 'free after' line"
    n2=$(dbsize 6399); [ "$n2" = "$n1" ] && ok "$label: the hand-started server has the $n1 keys" || bad "$label: keys in the hand-started server" "$n1 -> $n2"
  else bad "$label: the waiting server never took over" "$(ex tail -3 /tmp/second.log)"; fi
  # while the hand-started one holds the dir, `systemctl start` must wait (not fail), then come up once it exits
  ex systemctl start --no-block kividb; sleep 3
  pong 6380 && bad "$label: unit served while the hand-started server held the dir" || ok "$label: the unit waits while the hand-started server holds the dir ($(ex systemctl is-active kividb))"
  redis-cli -p 6399 SHUTDOWN >/dev/null 2>&1
  up 6380 80 && ok "$label: the unit comes up once the hand-started server exits" || bad "$label: unit did not come up" "$(ex systemctl status kividb --no-pager 2>&1 | tail -4)"
  ex journalctl -u kividb --no-pager --since "$since" 2>/dev/null | grep -q "$INUSE" && ok "$label: the unit's journal logs the wait" || bad "$label: no in-use line in the journal"
  ex cat /tmp/second.log > "$OUT/$label-second-server.log" 2>&1
  ex journalctl -u kividb --no-pager --since "$since" > "$OUT/$label-journal.log" 2>&1
}

{
deb=$B/kividb_$ARCH.deb
EX=(sudo); ex() { sudo "$@"; }; exd() { sudo setsid sh -c "$1" </dev/null >/dev/null 2>&1 & }
sudo dpkg --purge kividb >/dev/null 2>&1; sudo rm -rf /var/lib/kividb /etc/kividb
if sudo dpkg -i "$deb" > "$OUT/deb-install.log" 2>&1; then ok "deb: dpkg -i"; else bad "deb: dpkg -i" "$(tail -3 "$OUT/deb-install.log")"; fi
up 6380 && unit_checks deb
sudo dpkg --purge kividb >/dev/null 2>&1; sleep 1; pong 6380 && bad "deb: still serving after purge" || ok "deb: purge stops it"
sudo rm -rf /var/lib/kividb /etc/kividb
} 2>&1 | tee "$OUT/deb.log"

{
rpm=$(readlink -f "$B/kividb.$UN.rpm")
cn=r10-systemd; docker rm -f "$cn" >/dev/null 2>&1
docker run -d --name "$cn" --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw --tmpfs /run --tmpfs /tmp \
  --network host -v "$rpm":/opt/kividb.rpm:ro registry.access.redhat.com/ubi9/ubi-init >/dev/null
for _ in $(seq 1 40); do docker exec "$cn" systemctl is-system-running 2>/dev/null | grep -qE 'running|degraded' && break; sleep 0.5; done
EX=(docker exec "$cn"); ex() { docker exec "$cn" "$@"; }; exd() { docker exec -d "$cn" sh -c "$1"; }
docker exec "$cn" dnf install -y /opt/kividb.rpm procps-ng util-linux > "$OUT/rpm-systemd-install.log" 2>&1 && ok "rpm: dnf install under systemd (ubi9-init)" || bad "rpm: dnf install" "$(tail -3 "$OUT/rpm-systemd-install.log")"
up 6380 && unit_checks rpm
docker exec "$cn" dnf remove -y kividb >/dev/null 2>&1; sleep 1; pong 6380 && bad "rpm: still serving after dnf remove" || ok "rpm: dnf remove stops it"
docker rm -f "$cn" >/dev/null 2>&1
} 2>&1 | tee "$OUT/rpm-systemd.log"

# ---------------------------------------------------------------- 3. rpm on clean amazonlinux:2023
{
rpm=$(readlink -f "$B/kividb.$UN.rpm"); cn=r10-al2023; docker rm -f $cn >/dev/null 2>&1
docker run -d --name $cn --network host -v "$rpm":/tmp/kividb.rpm:ro amazonlinux:2023 sleep infinity >/dev/null
inf "useradd before install: $(docker exec $cn sh -c 'command -v useradd || echo absent')"
req=$(docker exec $cn rpm -qpR /tmp/kividb.rpm 2>&1 | tr '\n' ' '); inf "rpm -qpR: $req"
echo "$req" | grep -q shadow-utils && ok "rpm: declares shadow-utils" || bad "rpm: does not declare shadow-utils" "$req"
if docker exec $cn dnf install -y /tmp/kividb.rpm > "$OUT/rpm-al2023-install.log" 2>&1 && docker exec $cn rpm -q kividb >/dev/null 2>&1; then
  ok "rpm: dnf install on clean amazonlinux:2023 ($(docker exec $cn rpm -q kividb shadow-utils | tr '\n' ' '))"
  docker exec $cn id kividb >/dev/null 2>&1 && ok "rpm: the kividb user exists ($(docker exec $cn getent passwd kividb))" || bad "rpm: no kividb user"
  v=$(docker exec $cn /usr/bin/kividb --version 2>&1); echo "$v" | grep -q -- "$WANT" && ok "rpm: /usr/bin/kividb --version [$v]" || bad "rpm: version" "$v"
  docker exec -d -u kividb $cn sh -c 'cd /var/lib/kividb && exec /usr/bin/kividb --port 6384 --bind 127.0.0.1 --update-check no > /tmp/s.log 2>&1'
  up 6384 40; o=$(redis-cli -p 6384 PING 2>&1)
  [ "$o" = PONG ] && ok "rpm: the installed server runs as kividb on amazonlinux:2023 and answers PING" || bad "rpm: server on al2023" "[$o] $(docker exec $cn cat /tmp/s.log | tail -3)"
else bad "rpm: dnf install on clean amazonlinux:2023" "$(grep -m2 -i 'not found\|error' "$OUT/rpm-al2023-install.log")"; fi
docker rm -f $cn >/dev/null 2>&1
} 2>&1 | tee "$OUT/rpm-al2023.log"

p=$(cat "$OUT"/{tarballs,images,deb,rpm-systemd,rpm-al2023}.log | grep -c '^PASS'); f=$(cat "$OUT"/{tarballs,images,deb,rpm-systemd,rpm-al2023}.log | grep -c '^FAIL')
echo "RESULT rc10-$ARCH $p $f" | tee "$OUT/result.txt"
cat "$OUT"/{tarballs,images,deb,rpm-systemd,rpm-al2023}.log | grep '^FAIL' | tee -a "$OUT/result.txt"
echo RC10-DONE
