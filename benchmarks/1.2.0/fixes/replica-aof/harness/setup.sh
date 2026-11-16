#!/usr/bin/env bash
# setup.sh build|check|load|replica  -- runs on the VM under nohup, log in ~/setup.log
set -u
ROLE=$1
export DEBIAN_FRONTEND=noninteractive
exec > "$HOME/setup.log" 2>&1
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential cmake clang pkg-config git curl redis-tools sysstat python3 python3-venv python3-pip linux-tools-common linux-tools-azure > /dev/null
sudo chown azureuser /mnt
ulimit -n 65535
src() {
  rm -rf /mnt/src && git clone -q https://github.com/kividbio/kividb /mnt/src && cd /mnt/src && git checkout -q a6b1bf38 || { echo CLONE-FAIL; exit 1; }
  git config user.name verify; git config user.email verify@example.invalid
}
rust() {
  [ -x "$HOME/.cargo/bin/cargo" ] || curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal > /dev/null
  source "$HOME/.cargo/env"; rustup update stable > /dev/null; rustup component add rustfmt clippy
  rustc --version
}
case $ROLE in
build)
  rust; src
  export CARGO_TARGET_DIR=/mnt/target
  git log -1 --oneline
  cargo build --release 2>&1 | tail -3
  mkdir -p /mnt/bin/rc4 /mnt/bin/fixed && cp /mnt/target/release/kividb /mnt/bin/rc4/kividb
  git am --keep-cr ~/fix.patch || { echo AM-FAIL; exit 1; }
  git log -2 --oneline
  cargo build --release 2>&1 | tail -3
  cp /mnt/target/release/kividb /mnt/bin/fixed/kividb
  sha256sum /mnt/bin/*/kividb; /mnt/bin/rc4/kividb --version | head -1; /mnt/bin/fixed/kividb --version | head -1
  echo BUILD-DONE
  ;;
check)
  rust; src
  git am --keep-cr ~/fix.patch || { echo AM-FAIL; exit 1; }
  git log -2 --oneline
  export CARGO_TARGET_DIR=/mnt/target
  echo "== fmt"; cargo fmt --all -- --check > ~/fmt.txt 2>&1; echo "fmt rc=$?"
  echo "== clippy"; cargo clippy --all-targets -- -D warnings > ~/clippy.txt 2>&1; echo "clippy rc=$?"
  echo "== clippy lua,tls"; cargo clippy --all-targets --features lua,tls -- -D warnings > ~/clippy-lua.txt 2>&1; echo "clippy-lua rc=$?"
  echo "== new test"; cargo test --release --lib backlog_tests > ~/newtest.txt 2>&1 < /dev/null; echo "newtest rc=$?"
  echo "== test aof"; cargo test --release aof --no-fail-fast > ~/test-aof.txt 2>&1 < /dev/null; echo "test-aof rc=$?"
  for t in $(ls tests/*.rs | xargs -n1 basename | sed 's/\.rs$//' | grep -E 'repl|resync|backup|psync|failover|aof'); do
    cargo test --release --no-fail-fast --test "$t" > ~/test-$t.txt 2>&1 < /dev/null; echo "test $t rc=$?"
  done
  echo "== lib replication"; cargo test --release --lib --no-fail-fast replication > ~/test-lib-replication.txt 2>&1 < /dev/null; echo "lib-replication rc=$?"
  echo CHECK-DONE
  ;;
load)
  cd /tmp && curl -sSL https://go.dev/dl/go1.23.4.linux-amd64.tar.gz -o go.tgz && sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go.tgz
  export PATH=$PATH:/usr/local/go/bin
  cd ~/soakgen-src && (go mod init soakgen 2>/dev/null; go mod tidy) && go build -o ~/soakgen . && ls -la ~/soakgen
  python3 -m venv ~/venv && ~/venv/bin/pip -q install redis hiredis matplotlib
  echo LOAD-DONE
  ;;
replica)
  echo REPLICA-DONE
  ;;
esac
