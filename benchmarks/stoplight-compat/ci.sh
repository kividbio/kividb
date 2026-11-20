#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              clone bolshakov/stoplight at v5.8.3 and bundle install it, in the pinned Ruby image
#   ci.sh run DIR PORT OUT.xml   its rspec suite against 127.0.0.1:6379, as JUnit XML
# Stoplight 5.8.3, the version Mastodon uses; its whole rspec suite (765 examples).
# The suite hardcodes localhost:6379 in places, so it runs with host networking on port 6379
# (benchmarks/ci/ruby.sh). Needs Docker. The checks that fail on Redis too (environment) are
# compared as they are: only a check that passes on Redis and fails on KiviDB fails the job.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../ci/ruby.sh
. "$here/../ci/ruby.sh"
cmd=${1:-} dir=${2:-}
case $cmd in
  setup) ruby_setup "$dir" bolshakov/stoplight v5.8.3 ;;
  run) ruby_run "$dir" "$3" "$4" spec ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
