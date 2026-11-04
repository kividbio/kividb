#!/usr/bin/env bash
# Run with sudo: stop the e2e matrix, reset, start it again in the background.
for p in $(pgrep -f "run_all.sh$"); do [ "$p" != "$$" ] && kill "$p"; done
for p in $(pgrep -f "/home/azureuser/e2e.py "); do kill "$p"; done
sleep 1
bash /home/azureuser/reset.sh
nohup bash /home/azureuser/run_all.sh > /home/azureuser/e2e.log 2>&1 < /dev/null &
echo restarted
