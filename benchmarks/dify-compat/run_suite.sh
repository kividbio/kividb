#!/usr/bin/env bash
# Dify's own Redis tests (api/tests/e2e_tests/containers/libs), run against one server image with
# compat_redis_image.py. Needs Dify's dev environment: `cd $DIFY/api && uv sync --group dev`.
#
#   ./run_suite.sh redis8       redis:8
#   ./run_suite.sh kividb       kividb/kividb:v1.2.0-lua
#   ./run_suite.sh kividb-nolua kividb/kividb:v1.2.0
#
# test_api_token_cache_integration.py goes last: it needs Dify's Redis client set up by the Flask app
# fixture of another file, and fails on any server ("Redis client is not initialized") when it runs
# first.
set -euo pipefail
LABEL=${1:?label}
IMAGE=${2:?image}
HERE=$(cd "$(dirname "$0")" && pwd)
DIFY=${DIFY:-$HOME/dify}
cd "$DIFY/api"
L=tests/e2e_tests/containers/libs
COMPAT_REDIS_IMAGE=$IMAGE PYTHONPATH=$HERE .venv/bin/pytest -p compat_redis_image -p no:cacheprovider -q \
  --junitxml="$HERE/suite-$LABEL.xml" \
  $L/broadcast_channel/redis/test_channel.py \
  $L/broadcast_channel/redis/test_sharded_channel.py \
  $L/broadcast_channel/redis/test_streams_channel.py \
  $L/test_auto_renew_redis_lock_integration.py \
  $L/test_rate_limiter_integration.py \
  $L/test_redis_counter.py \
  $L/test_api_token_cache_integration.py
