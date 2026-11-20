#!/usr/bin/env bash
# Sourced by the ci.sh of the Ruby harnesses (stoplight, sidekiq-unique-jobs,
# sidekiq-scheduler, sidekiq-mastodon): Ruby runs in a pinned image, so the
# runner needs Docker and nothing else, and the suites find the server where
# they look for it (localhost:6379): the servers run with host networking.
#
#   ruby_setup DIR REPO TAG   clone REPO at TAG into DIR/src, add rspec_junit_formatter to its
#                             Gemfile, bundle install into DIR/bundle
#   ruby_run DIR PORT OUT     run `bundle exec rspec` of DIR/src, JUnit XML to OUT
#   ruby_exec DIR CMD...      run CMD in the image, in DIR/src, with the bundle
#
# Exit codes follow the harness contract: `ruby_run` exits 0 when the suite ran, whatever
# its results, and non-zero only when no results were written.

RUBY_IMAGE=${RUBY_IMAGE:-ruby:3.3.14@sha256:fcff19134fc79e007cd44c25b93aa98261c98b0986db93901b24d1f94f5c8f45}
JUNIT_GEM='gem "rspec_junit_formatter", "0.6.0"'

_ruby_docker() {
  local dir=$1
  shift
  docker run --rm --network host --user "$(id -u):$(id -g)" \
    -e HOME=/tmp -e BUNDLE_PATH=/work/bundle -e BUNDLE_APP_CONFIG=/work/bundle-config \
    -v "$dir":/work -w /work/src "$RUBY_IMAGE" "$@"
}

ruby_exec() {
  local dir=$1
  shift
  _ruby_docker "$dir" "$@"
}

ruby_setup() {
  local dir=$1 repo=$2 tag=$3
  rm -rf "$dir/src" "$dir/bundle" "$dir/bundle-config"
  mkdir -p "$dir"
  git clone -q --depth 1 --branch "$tag" "https://github.com/$repo.git" "$dir/src" 2>&1 | grep -v 'detached HEAD' || true
  [ -f "$dir/src/Gemfile" ] || return 1
  printf '\n%s\n' "$JUNIT_GEM" >>"$dir/src/Gemfile"
  docker pull -q "$RUBY_IMAGE" >/dev/null
  _ruby_docker "$dir" bundle install --jobs 4 --quiet
}

# ruby_run DIR PORT OUT [rspec args...]: the Ruby suites hardcode port 6379.
ruby_run() {
  local dir=$1 port=$2 out=$3
  shift 3
  if [ "$port" != 6379 ]; then
    echo "the suite expects the server on port 6379, not $port" >&2
    return 2
  fi
  rm -f "$dir/junit.xml" "$out"
  _ruby_docker "$dir" bundle exec rspec "$@" --format progress \
    --format RspecJunitFormatter --out /work/junit.xml || true
  # Some example names carry an object address (#<Proc:0x00007c59...>) or a time: they differ every run.
  test -s "$dir/junit.xml" && sed -E 's/0x[0-9a-f]{12,16}/0x0/g; s/20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?( [+-][0-9]{4})?/TIME/g; s/20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9:]{8}Z/TIME/g; s/object_id=[0-9]+/object_id=N/g; s/current_redis_version=&gt;&quot;[0-9.]+&quot;/current_redis_version=\&gt;V/g; s/[0-9a-f]{24}/ID/g; s/[0-9]{10}.[0-9]+/TS/g' "$dir/junit.xml" >"$out"
  test -s "$out"
}
