#!/usr/bin/env bash
# cg-split.sh: per kept callgrind file, instructions per command in total, in glibc's __memcmp_avx2_movbe, and without it.
cd ~/cgprof
for f in *.out; do
  t=$(callgrind_annotate "$f" 2>/dev/null | grep -m1 "PROGRAM TOTALS" | tr -d , | awk '{print $1}')
  m=$(callgrind_annotate --inclusive=no "$f" 2>/dev/null | grep -m1 "__memcmp_avx2_movbe" | tr -d , | awk '{print $1}')
  printf "%s total %.1f memcmp %.1f without_memcmp %.1f\n" "${f%.out}" "$(echo "$t/60000" | bc -l)" "$(echo "${m:-0}/60000" | bc -l)" "$(echo "($t-${m:-0})/60000" | bc -l)"
done
