#!/bin/bash
# Client-side job queue: runs the lines of /var/tmp/mem/queue.txt one at a time, top first, each exactly once
# (the line's text is recorded in done.txt), so the file can be edited or reordered while it runs.
# A line "STOP" ends it. Each line is a command run from /opt/mem, e.g. "./growth.sh redis 100 1000000,5000000".
Q=/var/tmp/mem/queue.txt; DONE=/var/tmp/mem/done.txt; LOG=/var/tmp/mem/queue.log
mkdir -p /var/tmp/mem; touch $Q $DONE
cd /opt/mem
while true; do
  line=$(grep -vxF -f $DONE $Q | grep -v '^\s*$' | head -1)
  if [ -z "$line" ]; then sleep 10; continue; fi
  [ "$line" = STOP ] && { echo "STOP $(date -u +%FT%TZ)" >> $LOG; exit 0; }
  echo "### $(date -u +%FT%TZ) start: $line" >> $LOG
  bash -c "$line" >> $LOG 2>&1 < /dev/null
  echo "### $(date -u +%FT%TZ) end (exit $?): $line" >> $LOG
  echo "$line" >> $DONE
done
