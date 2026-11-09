#!/usr/bin/env python3
"""Patch a copy of scripts/perf/measure.sh for the 1.2.1 profiling round.

    python3 patch_measure.py <copy of measure.sh>

Four additions; none changes how an existing workload is run or counted:
  1. keep each workload's callgrind file as /out/cg-<workload>.out (as in the
     1.2.0 suite's last round), so counts can be split by object and function;
  2. ONLY="W1 W2 ...": run only those workloads (for the repeats);
  3. PRELOAD2: a second preload command, run after PRELOAD (used by HSET-FTTL);
  4. five hash workloads after the 18 of WORKLOADS=full, three of them on
     hashes whose fields carry a TTL (hash-field expiry, which #531 changed):
       HSET         HSET h:<n> f <100 B> on hashes {f}
       HGET         HGET h:<n> f          on hashes {f}
       HSET-FTTL    HSET h:<n> f <100 B> on hashes {t (TTL), f}
       HGET-FTTL    HGET h:<n> f          on hashes {f (TTL)}
       HSETEX-FTTL  HSETEX h:<n> EX 1000000 FIELDS 1 f <100 B> on hashes {f (TTL)}
     The TTLs are 1,000,000 s, so nothing expires during a run. 10,000 hashes,
     loaded and warmed up as measure.sh does for every workload.
Each replacement must match exactly once, or the script stops.
"""
import sys

p = sys.argv[1]
s = open(p).read()


def sub(old, new):
    global s
    assert s.count(old) == 1, "anchor not found once: " + old[:60]
    s = s.replace(old, new)


# 1. keep the callgrind file
a = '  echo "$label $tag $(( ${total:-0} / N ))"\n'
sub(a, a + '  cp "$f" "/out/cg-$label.out"\n')

# 2. ONLY filter
a = "  local tag=$1 label=$2; shift 2\n"
sub(a, a + '  case " ${ONLY:- $label } " in *" $label "*) ;; *) return ;; esac\n')

# 3. PRELOAD2, after the preload and before the warm-up
a = '  redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n 2000 "$@" > /dev/null 2>&1\n'
sub(a, '  if [ -n "${PRELOAD2:-}" ]; then\n'
       '    # shellcheck disable=SC2086\n'
       '    redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n 20000 $PRELOAD2 > /dev/null 2>&1\n'
       '  fi\n' + a)

# 4. hash workloads
a = ' one "$tag" INCR-LFU -t incr\n  fi\n'
sub(a, ' one "$tag" INCR-LFU -t incr\n'
       '    # Hashes (added for 1.2.1): plain, and with field TTLs.\n'
       '    HS="HSET h:__rand_int__ f $V100"\n'
       '    FT="HSETEX h:__rand_int__ EX 1000000 FIELDS 1 f $V100"\n'
       '    PRELOAD="$HS" DSIZE=100 one "$tag" HSET HSET h:__rand_int__ f "$V100"\n'
       '    PRELOAD="$HS" DSIZE=100 one "$tag" HGET HGET h:__rand_int__ f\n'
       '    PRELOAD="HSETEX h:__rand_int__ EX 1000000 FIELDS 1 t x" PRELOAD2="$HS" DSIZE=100 \\\n'
       '      one "$tag" HSET-FTTL HSET h:__rand_int__ f "$V100"\n'
       '    PRELOAD="$FT" DSIZE=100 one "$tag" HGET-FTTL HGET h:__rand_int__ f\n'
       '    PRELOAD="$FT" DSIZE=100 \\\n'
       '      one "$tag" HSETEX-FTTL HSETEX h:__rand_int__ EX 1000000 FIELDS 1 f "$V100"\n'
       '  fi\n')

open(p, "w").write(s)
print("patched", p)
