#!/usr/bin/env bash
# on srv: build rc10 then rc8 with harness/build.sh, one after the other, into ~/build.log
set -u
cd ~
source "$HOME/.cargo/env"
{
  rustup show active-toolchain
  rustc --version
  cargo --version
  bash harness/build.sh rc10 39bfacd2
  bash harness/build.sh rc8 082dab3d
  (cd kividb && git rev-parse 'v1.2.0-rc10^{commit}' && git rev-parse 082dab3d)
  sha256sum ~/bin/kividb-rc10 ~/bin/kividb-rc8
  echo BUILD-DONE
} > build.log 2>&1
