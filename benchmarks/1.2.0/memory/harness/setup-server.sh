#!/bin/bash
# Engine VM: format and mount the local NVMe disk for data files, pull the pinned engine images, and build
# KiviDB from a git ref with the repo's own Dockerfile (default variant) when no release image is given.
#   setup-server.sh [kividb-git-ref=master | release]   (release: pull kividb/kividb:v1.2.0-rc4, build nothing)
set -euxo pipefail
REF=${1:-master}
# local NVMe temp disk: the one whole disk with no partitions and no mount
if ! mountpoint -q /mnt/nvme; then
  DEV=$(lsblk -dpno NAME,TYPE | awk '$2=="disk"{print $1}' | while read -r d; do
    [ "$(lsblk -no NAME "$d" | wc -l)" = 1 ] && ! findmnt -S "$d" >/dev/null && echo "$d"; done | head -1)
  mkfs.ext4 -F -q -E nodiscard "$DEV"
  mkdir -p /mnt/nvme && mount -o noatime "$DEV" /mnt/nvme
fi
mkdir -p /mnt/nvme/data && chmod 777 /mnt/nvme/data
df -h /mnt/nvme
for i in redis:8.10.3 valkey/valkey:9.1.3 docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2 kividb/kividb:v1.1.3; do
  docker pull "$i"
done
if [ "$REF" = release ]; then
  # the published release image of the build under test; nothing is built
  docker pull kividb/kividb:v1.2.0-rc4
  echo kividb/kividb:v1.2.0-rc4 > /var/tmp/kividb-rc4-image
  docker images --digests > /var/tmp/engine-images.txt
  uname -a > /var/tmp/kernel.txt; lscpu > /var/tmp/lscpu.txt; free -g > /var/tmp/free.txt
  touch /var/tmp/engine-ready; exit 0
fi
rm -rf /opt/kividb
git clone https://github.com/kividbio/kividb.git /opt/kividb
cd /opt/kividb && git checkout "$REF"
SHA=$(git rev-parse --short=12 HEAD)
git log -1 --format='%H %cI %s' > /var/tmp/kividb-build-commit
docker build -t "kividb/kividb:build-$SHA" . > /var/tmp/kividb-build.log 2>&1
echo "kividb/kividb:build-$SHA" > /var/tmp/kividb-build-image
# until the release image exists, the engine scripts use this build (and the warefeats master variant too)
[ -f /var/tmp/kividb-rc4-image ] || cp /var/tmp/kividb-build-image /var/tmp/kividb-rc4-image
cp /var/tmp/kividb-build-image /var/tmp/kividb-master-image
docker images --digests > /var/tmp/engine-images.txt
uname -a > /var/tmp/kernel.txt; lscpu > /var/tmp/lscpu.txt; free -g > /var/tmp/free.txt
touch /var/tmp/engine-ready
