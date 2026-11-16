#!/usr/bin/env bash
# Put the three builds on the server, one after the other:
#   ~/bin/kividb-120rel  1.2.0 release asset kividb-linux-aarch64.tar.gz, checked against SHA256SUMS.txt
#   ~/bin/kividb-121     878746e4 (tree ~/src-878746e4.tgz), built the way release.yml builds arm64
#   ~/bin/kividb-120src  tag v1.2.0, built the same way
# "The way release.yml builds arm64" (linux-arm64 job, default variant): inside amazonlinux:2023,
# dnf gcc/cmake/perl/openssl-devel, rustup stable (minimal), RUSTFLAGS="-C target-feature=+lse",
# cargo build --release (Cargo.toml [profile.release]: opt-level 3, lto "fat", codegen-units 1).
set -uo pipefail
REPO=https://github.com/kividbio/kividb
mkdir -p ~/bin ~/src
exec > ~/builds.txt 2>&1

echo "== 1.2.0 release asset"
cd ~/src && rm -rf rel && mkdir rel && cd rel
curl -fsSL -o kividb-linux-aarch64.tar.gz "$REPO/releases/download/v1.2.0/kividb-linux-aarch64.tar.gz"
curl -fsSL -o SHA256SUMS.txt "$REPO/releases/download/v1.2.0/SHA256SUMS.txt"
# SHA256SUMS.txt lists "./<file>"; the first run grepped without the "./" and so reported a
# mismatch; the hashes were then compared by hand (results/builds.txt) and match.
grep " \./kividb-linux-aarch64.tar.gz$" SHA256SUMS.txt
grep " \./kividb-linux-aarch64.tar.gz$" SHA256SUMS.txt | sha256sum -c - && echo "tarball checksum OK" || echo "TARBALL CHECKSUM MISMATCH"
mkdir x && tar -xzf kividb-linux-aarch64.tar.gz -C x
install -m 0755 "$(find x -type f -name kividb | head -1)" ~/bin/kividb-120rel

build() { # <name> <srcdir>
  local name=$1 dir=$2
  echo "== build $name in $dir"
  sudo docker run --rm -v "$dir:/workspace" -w /workspace amazonlinux:2023 bash -c '
    set -euo pipefail
    dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig >/dev/null
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
    sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable >/dev/null 2>&1
    source "$HOME/.cargo/env"
    echo "container: $(cat /etc/system-release); gcc $(gcc -dumpfullversion); glibc $(ldd --version | head -1)"
    rustc --version; cargo --version
    export RUSTFLAGS="-C target-feature=+lse"
    cargo build --release > /workspace/build.log 2>&1 || { tail -40 /workspace/build.log; exit 1; }
    tail -2 /workspace/build.log
  ' || { echo "BUILD FAILED: $name"; return 1; }
  sudo install -m 0755 -o "$USER" "$dir/target/release/kividb" ~/bin/kividb-"$name"
}

echo "== 1.2.1 tree"
rm -rf ~/src/k121 && mkdir -p ~/src/k121 && tar -xzf ~/src-878746e4.tgz -C ~/src/k121
build 121 ~/src/k121

echo "== v1.2.0 tag"
rm -rf ~/src/k120 && git clone -q --depth 1 --branch v1.2.0 "$REPO" ~/src/k120
echo "v1.2.0 commit: $(git -C ~/src/k120 rev-parse HEAD)"
echo "release.yml linux-arm64 build step, v1.2.0 vs 878746e4 (diff, empty = same):"
diff <(sed -n '/linux-arm64:/,/Build .deb/p' ~/src/k120/.github/workflows/release.yml) \
     <(sed -n '/linux-arm64:/,/Build .deb/p' ~/src/k121/.github/workflows/release.yml)
echo "Cargo.toml [profile.release] diff (empty = same):"
diff <(sed -n '/\[profile.release\]/,/^$/p' ~/src/k120/Cargo.toml) <(sed -n '/\[profile.release\]/,/^$/p' ~/src/k121/Cargo.toml)
build 120src ~/src/k120

echo "== versions and sha256"
cd ~/bin
for b in kividb-120rel kividb-121 kividb-120src; do
  echo "$b: $(./$b --version 2>&1 | head -1)"
done
sha256sum kividb-120rel kividb-121 kividb-120src
file kividb-120rel kividb-121 kividb-120src
echo BUILDS-DONE
