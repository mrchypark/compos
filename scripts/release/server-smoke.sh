#!/bin/sh
# Remote runner only: health is public; unconfigured authority must fail closed.
set -eu
image=${1:?server image required}
results=${2:?results directory required}
mkdir -p "$results"
container=
cleanup() {
  if [ -n "$container" ]; then
    docker logs "$container" > "$results/image.log" 2>&1 || :
    docker rm -f -v "$container" >/dev/null
    if docker container inspect "$container" >/dev/null 2>&1; then
      echo 'smoke container cleanup failed' >&2
      return 1
    fi
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
container=$(docker run -d --network bridge -p 127.0.0.1::7878 "$image")
port=$(docker port "$container" 7878/tcp | sed -n '1s/.*://p')
[ -n "$port" ]
base="http://127.0.0.1:$port"
attempt=0
until curl --max-time 2 -fsS "$base/api/v1/health" > "$results/health.json"; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 60 ] || exit 1
  sleep 1
done
status=$(curl --max-time 5 -sS -o "$results/authority.json" -w '%{http_code}' \
  -H 'Authorization: Bearer forged' -H 'X-Principal-ID: admin' \
  -H 'Content-Type: application/json' -d '{"key":"smoke","name":"Smoke"}' \
  "$base/api/v1/workspaces")
[ "$status" = 503 ]
jq -e '.. | objects | select(.code? == "external_authority_unavailable")' "$results/authority.json" >/dev/null
printf '%s\n' 'health=passed' 'authority_fail_closed=passed' \
  'public_crud=not_tested' 'provider_authority_integration=not_tested' > "$results/smoke.env"
