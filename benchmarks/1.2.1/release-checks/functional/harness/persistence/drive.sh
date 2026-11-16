#!/usr/bin/env bash
# drive.sh: on the server VM after setup.sh. Every build x io model: fsync counting, then persist.py (all checks).
# Data dirs on /data (premium SSD P30, host caching None).
ulimit -n 65535
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q redis-tools bc > /dev/null 2>&1
grep -q ' /data ' /etc/fstab || echo "UUID=$(sudo blkid -s UUID -o value $(readlink -f /dev/disk/azure/scsi1/lun0)) /data ext4 noatime,nofail 0 2" | sudo tee -a /etc/fstab
mkdir -p /data/out
for b in default full; do for io in uring tokio; do
  o=/data/out/$b-$io; rm -rf $o; mkdir -p $o
  timeout 900 bash ~/harness/fsynccount.sh ~/bin/kividb-$b $io $o/fsync.txt > $o/fsync-console.log 2>&1 < /dev/null
  pkill -9 -f "^$HOME/bin/kividb" 2>/dev/null
  (cd /tmp && timeout 3000 ~/venv/bin/python -I ~/harness/persist.py ~/bin/kividb-$b $io $o > $o/console.log 2>&1 < /dev/null)
  pkill -9 -f "^$HOME/bin/kividb" 2>/dev/null
  echo "$b $io: $(tail -1 $o/console.log)"
done; done
for b in default full; do
  echo "== fieldttl_drift $b"; timeout 300 bash ~/harness/fieldttl_drift.sh ~/bin/kividb-$b
  echo "== hdigest_repro $b"; timeout 300 bash ~/harness/hdigest_repro.sh ~/bin/kividb-$b
done > /data/out/drift-digest.txt 2>&1 < /dev/null
echo DRIVE-DONE
