#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              fetch the Spring Cloud Gateway script (v5.1.0) and Traefik's lua.go
#                                (v3.7.17) into DIR, SHA-256 checked
#   ci.sh run DIR PORT OUT.xml   ratelimit_scripts_compat.py against 127.0.0.1:PORT, as JUnit XML
# The rate limiters' scripts (Spring Cloud Gateway, Traefik, Kong) called as their gateways call
# them, checked reply by reply. They need scripting, so the KiviDB side runs the `lua` image
# variant (kividb-variant). Any port works.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCG_TAG=v5.1.0
SCG_SHA256=90b870d1ad945adf704a86e744a31baa617bcc2882de15776666a4e60092093a
TRAEFIK_TAG=v3.7.17
TRAEFIK_SHA256=e13ac2f4d5d9c09830ac7ac9b6bd2af8d8174c3a3d4b37bfdd3aa0b2287a318f
cmd=${1:-} dir=${2:-}
case $cmd in
  setup)
    mkdir -p "$dir"
    curl -fsSL --retry 3 -o "$dir/request_rate_limiter.lua" \
      "https://raw.githubusercontent.com/spring-cloud/spring-cloud-gateway/$SCG_TAG/spring-cloud-gateway-server-webflux/src/main/resources/META-INF/scripts/request_rate_limiter.lua"
    curl -fsSL --retry 3 -o "$dir/lua.go" \
      "https://raw.githubusercontent.com/traefik/traefik/$TRAEFIK_TAG/pkg/middlewares/ratelimiter/lua.go"
    echo "$SCG_SHA256  $dir/request_rate_limiter.lua" | sha256sum -c --quiet -
    echo "$TRAEFIK_SHA256  $dir/lua.go" | sha256sum -c --quiet -
    ;;
  run)
    port=$3 out=$4
    timeout 300 python3 "$here/ratelimit_scripts_compat.py" --scg "$dir/request_rate_limiter.lua" \
      --traefik "$dir/lua.go" --port "$port" --junitxml "$out" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
