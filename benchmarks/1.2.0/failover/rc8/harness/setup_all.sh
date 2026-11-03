#!/usr/bin/env bash
# From the operator's machine: copy the harness, base8.sh and an internal key (VM-to-VM ssh) to every VM, start
# base8.sh per role; on foc/fod/pfc also the server set (hosts.env); on bld also build8.sh and the host list.
set -u
W=$(cd "$(dirname "$0")" && pwd)
K=${SSH_KEY:-$HOME/.ssh/id_ed25519}
role() { case $1 in fo[1-6]) echo fo ;; foc|fod) echo foc ;; pf1) echo pf ;; *) echo $1 ;; esac; }
while read -r n pub priv; do
  (
    S="ssh -i $K -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=20 -o LogLevel=ERROR azureuser@$pub"
    for i in $(seq 1 20); do $S true && break; sleep 10; done
    extra=""; [ -f $W/hosts-$n.env ] && extra="$W/hosts-$n.env"; [ $n = bld ] && extra="$W/build8.sh $W/hosts.txt"
    scp -q -i $K -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $W/b.tgz $W/base8.sh $W/ik $W/ik.pub $extra azureuser@$pub:
    post="true"; [ -f $W/hosts-$n.env ] && post="cp ~/hosts-$n.env ~/fo/harness/hosts.env"
    [ $n = bld ] && post="nohup bash ~/build8.sh > /dev/null 2>&1 < /dev/null &"
    $S "mkdir -p ~/.ssh && cat ~/ik.pub >> ~/.ssh/authorized_keys && mv ~/ik ~/.ssh/id_ed25519 && mv ~/ik.pub ~/.ssh/id_ed25519.pub && chmod 600 ~/.ssh/id_ed25519 && printf 'Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n' > ~/.ssh/config && (nohup bash -c 'bash base8.sh $(role $n); $post' > /dev/null 2>&1 < /dev/null &)"
    echo "started base on $n"
  ) &
done < $W/vms.tsv
wait
