#!/bin/sh
# Remote runner only: verify health and credential-free workspace provisioning.
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
status=$(curl --max-time 5 -sS -o "$results/workspace.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' \
  -d '{"key":"smoke","name":"Smoke","idempotency_key":"smoke-workspace"}' \
  "$base/api/v1/workspaces")
[ "$status" = 201 ]
workspace=$(jq -er '.data.id' "$results/workspace.json")
jq -e '.data | [has("creator_person_object_instance_id"), has("works_for_object_relation_id"), has("workspace_company_object_instance_id"), has("works_for_relation_definition_id")] | all(. == false)' "$results/workspace.json" >/dev/null
curl --max-time 5 -fsS "$base/api/v1/workspaces/$workspace/command-surface" > "$results/surface.json"
jq -e '.nouns == []' "$results/surface.json" >/dev/null
curl --max-time 5 -fsS "$base/api/v1/workspaces/$workspace/objects" > "$results/objects.json"
jq -e '.data == []' "$results/objects.json" >/dev/null
printf '%s\n' 'health=passed' 'workspace_create=passed' 'workspace_empty=passed' > "$results/smoke.env"
