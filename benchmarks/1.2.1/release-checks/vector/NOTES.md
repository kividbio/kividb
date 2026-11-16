# Notes: vector suite on 1.2.1 (35a413c0), 2026-11-12

- RG `kividb-c3-vec`, swedencentral (the README's region; DPSv5 quota 65 vCPU there, so one server+client pair
  (48 vCPU) plus the D8ps builder; a second pair did not fit, so datasets ran sequentially on one pair).
- Image: Ubuntu 22.04 arm64, as the vector README and its `create_vms.sh` use (the BRIEF's default 24.04 was not
  used, to keep the platform identical to the comparison run). NSG: one rule, SSH from the laptop /32.
- Competitors skipped (README comparison is between KiviDB builds; Redis/Dragonfly rows of the earlier session stand).
- 1.2.1 built on the builder in `amazonlinux:2023` exactly as release.yml (stable = rustc 1.99.1, the same compiler
  the earlier 1.2.1 build used; +lse; fat LTO from Cargo.toml). Build: 6 min 13 s. `--version` prints
  `kividb 1.2.1-rc2` (expected for this tree). The tarball itself is kept outside `out/`
  (`comb3/vector-build/`), since it is a build of unpublished code.
- 1.2.0 release binary hashes match the earlier session's exactly.
- Session 04:05-06:03 UTC, all 26 runs exit 0, `client_saturated` never set.
- VM time: server D32ps_v5 + client D16ps_v5 ~2 h 25 min each; builder D8ps_v5 ~25 min.
- `create_vms.sh` quirk: `BUILDER_SIZE=` does not skip the builder (`:-` default). Harmless here.
- No bugs found.
