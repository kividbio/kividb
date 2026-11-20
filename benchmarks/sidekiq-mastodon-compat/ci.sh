#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              Sidekiq 8.1.7 (Gemfile.lock) and Mastodon v4.7.6's config/sidekiq.yml, in the pinned Ruby image
#   ci.sh run DIR PORT OUT.xml   sidekiq_mastodon_compat.rb against 127.0.0.1:PORT, as JUnit XML
# Sidekiq run as Mastodon runs it (its queues and weights) at 25, 100, 500 and 1,000 threads: every
# job must run exactly once. Sidekiq's scheduler needs scripting, so the KiviDB side runs the `lua`
# image variant (kividb-variant). Needs Docker. Any port works.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../ci/ruby.sh
. "$here/../ci/ruby.sh"
MASTODON_TAG=v4.7.6
SIDEKIQ_YML_SHA256=ccecd543c14d5602e3f08a120a09e6a3df5f70f90bb3a3457b0c1a9ffb20c183
cmd=${1:-} dir=${2:-}
case $cmd in
  setup)
    rm -rf "$dir/src" "$dir/bundle" "$dir/bundle-config"
    mkdir -p "$dir/src"
    cp "$here/Gemfile" "$here/Gemfile.lock" "$here/worker.rb" "$here/sidekiq_mastodon_compat.rb" "$dir/src/"
    curl -fsSL --retry 3 -o "$dir/src/sidekiq.yml" \
      "https://raw.githubusercontent.com/mastodon/mastodon/$MASTODON_TAG/config/sidekiq.yml"
    echo "$SIDEKIQ_YML_SHA256  $dir/src/sidekiq.yml" | sha256sum -c --quiet -
    docker pull -q "$RUBY_IMAGE" >/dev/null
    ruby_exec "$dir" bundle install --jobs 4 --quiet
    ;;
  run)
    port=$3 out=$4
    rm -f "$dir/junit.xml" "$out"
    # The harness exits 0 whatever the results: they are in OUT.xml.
    timeout 900 bash -c 'source "$1"; ruby_exec "$2" env REDIS_URL="redis://127.0.0.1:$3/0" bundle exec ruby sidekiq_mastodon_compat.rb --config sidekiq.yml --junitxml /work/junit.xml' \
      _ "$here/../ci/ruby.sh" "$dir" "$port" || true
    test -s "$dir/junit.xml" && cp "$dir/junit.xml" "$out"
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
