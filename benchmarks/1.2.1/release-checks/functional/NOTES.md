# 1.2.1 (35a413c0) functional release checks: progress notes

- 03:55Z RG kividb-c3-func (centralindia): fp D16ds_v5 + P30 1 TiB (build + persistence), pcli D4s_v5 (power-loss
  client, snapshot compat), fe D8s_v5 (expiry), fs D16s_v5 (Tcl suite). Private 10.0.0.4-7.
- RG kividb-c3-func2 (metadata eastus2): eastus2 had no D8s_v5 capacity (SkuNotAvailable), swedencentral's regional
  cores were taken by others (64/65), southindia had no D8s_v5 or D8s_v6 capacity, uksouth none; **westus2** worked:
  cluster set cl-c1..c3 + cl-cc (10.82.1.11-14) and replication set cl + n1..n3 (10.92.1.10-13), all D8s_v5.
  The replication client `cl` is a D8s_v5 (earlier pass: D16s_v5) to fit the 65-core regional limit.
- fp: data disk is /dev/sda on this VM (resource disk is sdc): build_release.sh, fsynccount.sh and drive.sh resolve
  /dev/disk/azure/scsi1/lun0 instead of hard-coding sdc. First build attempt failed for that reason (no /data).
- Build per release.yml linux-amd64: amazonlinux:2023 container, rustup stable (rustc 1.99.1), `cargo build --release`
  (fat LTO, codegen-units 1 from Cargo.toml). default 370 s, full (lua,tls) 391 s, in parallel. `--version`:
  `kividb 1.2.1-rc2` (expected). sha256 default b7ebacb76961b4b8b52981728b4cddae9c65f17a71cbb2d0c022736afed86b0f,
  full 8e9225fe721029070ff4ababdb723144055464acc5f20fa1bf95518a63d21a32. These two binaries are used everywhere
  (copied through the laptop); test binaries for the cargo tests on fe are built there (release, LTO off, lua,tls).
- 1.2.0 release binaries: SHA256SUMS.txt checked on every VM (`sha256sum -c --ignore-missing`); default binary
  sha256 a211d544be79bb78...
- snapshot harness: KiviDB takes `appendonly` only from a config file (first run passed `--appendonly`: refused by
  the CLI parser, harness fixed); redis-py parses XPENDING/XINFO into dicts, so the fingerprint uses unparsed replies;
  a variable clash broke S5 in the second run. Third run classifies two known differences separately (#612 TS label
  order; hash-field deadlines 1 ms early in 1.2.0-written files) and adds S6.
- Runs: Tcl suite 04:28-04:39Z (fs deleted 04:42); snapshot compat done 04:41; persistence drive.sh ~04:28-05:00,
  power loss 05:00-05:05, tmpleft re-run, fp+pcli deleted 05:06; cluster drive35 04:28-05:08 (deleted 05:09);
  expiry run_all 04:2x-05:28 + idle2 to ~05:40 (fe deleted 05:53); replication chain 04:34-05:47 (deleted 05:50).
- 1b WAIT unavailability 6.8 s (earlier 1.0 s): wait_probe.sh shows WAIT 1 200 without replicas now takes 209 ms
  (1.2.0: 6 ms) = documented CHANGELOG change. Writers resumed 21.03 s at 203 ms per cycle.
- Cluster f5 stale=1: slot 9900 of the killed source, ack at 21.155 s, first connection errors 21.187 s (kill landed
  ~0.58 s after the event log line, ssh latency): async window.
- Replica hide check: 1 HEXISTS vs HGETALL mismatch in 1 of 9 checks; probes multi_clock*.py 80 tries 0: open.
- Snapshot: 1.2.0-written field deadlines -1 ms (S4b one -2 ms) on load; 1.2.0 own reload same; 1.2.1 own exact.
- Approx VM hours: fp 1.2 h x16 vCPU, pcli 1.2 h x4, fe 2.0 h x8, fs 0.8 h x16, westus2 8 x D8s_v5 ~1.5 h each.
