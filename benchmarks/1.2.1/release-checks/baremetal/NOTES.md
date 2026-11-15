# Notes (bare metal, 1.2.1 35a413c0 vs 1.2.0)

- AWS account <aws account>, us-east-1f (same AZ as the 2026-11-10 rerun). Everything tagged Name=kividb-c3-baremetal: key pair kividb-c3-baremetal (key-02a385c2a90f090df), security group sg-01ef7a6bd294fc887 (SSH 22 from <laptop ip> only; 6379-6380 and ICMP from the group itself), cluster placement group kividb-c3-baremetal (pg-03e499adff3557687), instances i-0596bdc6392cceea8 (server) and i-0da839701db526eca (client), c7gn.metal, Ubuntu 22.04 arm64 ami-009a0aab2094cd1f4, 100 GB gp3 root (delete on termination).
- Launched 03:52 UTC; build done ~04:07; session 04:08-04:28 UTC; instances halted from inside (`shutdown -h`) at 04:29 UTC, i.e. ~37 min x 2 c7gn.metal.
- TEARDOWN NOT FINISHED: at 04:29 UTC the AWS CLI SSO session had expired (`aws login` needed; a 10-minute browser login attempt was not completed). The instances were stopped from inside the OS (compute billing stops; EBS remains). Still to do with a fresh `aws login`:
  `aws ec2 terminate-instances --region us-east-1 --instance-ids i-0596bdc6392cceea8 i-0da839701db526eca`, wait, then
  `aws ec2 delete-placement-group --region us-east-1 --group-name kividb-c3-baremetal`,
  `aws ec2 delete-security-group --region us-east-1 --group-id sg-01ef7a6bd294fc887`,
  `aws ec2 delete-key-pair --region us-east-1 --key-name kividb-c3-baremetal`,
  and confirm with `aws ec2 describe-instances --region us-east-1 --filters Name=tag:Name,Values=kividb-c3-baremetal`.
- The tree was scp'd from the laptop (comb.tgz), never cloned or uploaded anywhere.
- build-c3.sh stopped (set -e) at the checksum grep: SHA256SUMS.txt lists `./kividb-linux-aarch64.tar.gz`; the remaining checks were run by hand and appended to build.log (fixed in the script afterwards). The checksum matched.
- Garnet / Dragonfly / Redis reused from 2026-11-10 (same setup). The KiviDB 1.2.0 control reproduced that session within 0.7% on the pipelined rows, so reuse is fair.
- The +6-9% unpipelined deltas vs 1.2.0 are within each build's own round-to-round spread (up to 18%); the same pattern showed in the 2026-11-10 session in the other direction.
