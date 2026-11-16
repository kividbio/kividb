#!/bin/sh
# Writes a Homebrew formula of the tap (kividbio/homebrew-tap) to stdout:
#
#   formula.sh VARIANT VERSION REPOSITORY ARM_SHA256 INTEL_SHA256
#
# VARIANT     default (Formula/kividb.rb: no Lua, no TLS) or full
#             (Formula/kividb-full.rb: Lua and TLS)
# VERSION     the release, without the v: 1.3.0
# REPOSITORY  where the release is: kividbio/kividb
# *_SHA256    the checksums of the release's macOS tarballs of that variant,
#             kividb[-full]-darwin-aarch64.tar.gz and -x86_64.tar.gz
#
# The release workflow (homebrew-tap-update) writes both formulas with it.
# Both install bin/kividb and the same service, so each conflicts with the
# other: `brew uninstall kividb && brew install kividbio/tap/kividb-full` to
# switch (docs/INSTALL.md, "Homebrew").
set -eu

variant="${1:?usage: formula.sh default|full VERSION REPOSITORY ARM_SHA256 INTEL_SHA256}"
version="${2:?usage: formula.sh default|full VERSION REPOSITORY ARM_SHA256 INTEL_SHA256}"
repo="${3:?usage: formula.sh default|full VERSION REPOSITORY ARM_SHA256 INTEL_SHA256}"
arm_sha="${4:?usage: formula.sh default|full VERSION REPOSITORY ARM_SHA256 INTEL_SHA256}"
intel_sha="${5:?usage: formula.sh default|full VERSION REPOSITORY ARM_SHA256 INTEL_SHA256}"

case "$version" in '' | *[!0-9A-Za-z.-]*) echo "bad version: $version" >&2; exit 1 ;; esac
case "$repo" in */*) ;; *) echo "bad repository: $repo" >&2; exit 1 ;; esac
for sha in "$arm_sha" "$intel_sha"; do
    if ! printf '%s\n' "$sha" | grep -qx '[0-9a-f]\{64\}'; then
        echo "bad sha256: '$sha'" >&2
        exit 1
    fi
done

case "$variant" in
    default)
        class=Kividb
        desc="Fast Redis-compatible in-memory store written in Rust"
        prefix=kividb
        other_name=kividb-full
        ;;
    full)
        class=KividbFull
        desc="Fast Redis-compatible in-memory store (with Lua and TLS)"
        prefix=kividb-full
        other_name=kividb
        ;;
    *)
        echo "variant must be default or full, not $variant" >&2
        exit 1
        ;;
esac

base="https://github.com/${repo}/releases/download/v${version}"

cat <<EOF
class ${class} < Formula
  desc "${desc}"
  homepage "https://kividb.io/"
  license "AGPL-3.0-only"
  version "${version}"

  on_macos do
    on_arm do
      url "${base}/${prefix}-darwin-aarch64.tar.gz"
      sha256 "${arm_sha}"
    end
    on_intel do
      url "${base}/${prefix}-darwin-x86_64.tar.gz"
      sha256 "${intel_sha}"
    end
  end

  # Both install bin/kividb and the same service.
  conflicts_with "${other_name}", because: "both install a \`kividb\` binary"

  def install
    bin.install "kividb"
    doc.install "LICENSE", "NOTICE"
  end

  service do
    run [opt_bin/"kividb"]
    keep_alive true
    log_path var/"log/kividb.log"
    error_log_path var/"log/kividb.error.log"
  end

  test do
    system "#{bin}/kividb", "--version"
  end
end
EOF
