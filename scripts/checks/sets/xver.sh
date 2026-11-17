#!/usr/bin/env bash
# xver.sh: snapshots and AOFs written by v1.2.2 load here, and ones written here load in v1.2.2.
# Needs redis-cli and python3. OLD=<older binary> NEW=<binary under test> scripts/checks/sets/xver.sh
set -u
OLD=${OLD:?a kividb binary of the older release, e.g. built from the v1.2.2 tag}
NEW=${NEW:?the kividb binary under test}
HERE=$(cd "$(dirname "$0")" && pwd)
PORT=6610
up() { # bin dir extra...
  local bin=$1 dir=$2; shift 2
  "$bin" --port $PORT --bind 127.0.0.1 --dir "$dir" --loglevel warning "$@" > "$dir.log" 2>&1 &
  for _ in $(seq 200); do redis-cli -p $PORT ping 2>/dev/null | grep -q PONG && return; sleep 0.1; done
  echo "server did not start"; tail -3 "$dir.log"
}
down() { redis-cli -p $PORT shutdown nosave > /dev/null 2>&1; for _ in $(seq 100); do redis-cli -p $PORT ping > /dev/null 2>&1 || return; sleep 0.1; done; }
"$OLD" --version | head -1; "$NEW" --version | head -1
for dir in from-old to-old; do for kind in rdb aof; do
  d=/tmp/xver-$dir-$kind; rm -rf $d $d.log; mkdir -p $d
  if [ $dir = from-old ]; then W=$OLD R=$NEW; else W=$NEW R=$OLD; fi
  if [ $kind = aof ]; then A=(--aof); else A=(); fi
  up "$W" $d ${A[@]+"${A[@]}"}
  python3 "$HERE/xver.py" $PORT write $d/want.json
  [ $kind = rdb ] && redis-cli -p $PORT save > /dev/null
  down
  ls $d | tr '\n' ' '; echo
  up "$R" $d ${A[@]+"${A[@]}"}
  echo -n "$dir $kind: "; python3 "$HERE/xver.py" $PORT check $d/want.json
  if [ $kind = aof ]; then
    # And the AOF rewritten by the reader, replayed by the reader again.
    redis-cli -p $PORT bgrewriteaof > /dev/null; sleep 3
    down; up "$R" $d ${A[@]+"${A[@]}"}
    echo -n "$dir $kind rewritten by reader: "; python3 "$HERE/xver.py" $PORT check $d/want.json
  fi
  down
done; done
echo XVERDONE
