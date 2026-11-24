#!/bin/sh
set -eu
results=${1:?results directory required}
container=
cleanup() {
  status=$?
  trap - EXIT
  if [ -n "$container" ]; then
    if ! docker rm -f "$container" >/dev/null; then echo "failed to remove container $container" >&2; status=1; fi
    if docker inspect "$container" >/dev/null 2>&1; then echo "container remains: $container" >&2; status=1; fi
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
mkdir -p "$results"
container=$(docker run -d --rm -p 127.0.0.1::4566 floci/floci@sha256:a0fbf944d7f338cc0856f1ef83831ea749ff2be71956eb7a7fce08fc21458e19)
endpoint=$(docker port "$container" 4566/tcp)
ready=0
attempt=0
while [ "$attempt" -lt 60 ]; do
  attempt=$((attempt + 1))
  if curl -s --max-time 1 -o /dev/null "http://$endpoint/"; then ready=1; break; fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then echo "HA provider did not start: $endpoint" >&2; exit 1; fi
status=0
COMPOS_RHIZA_CLUSTER_TEST_ENDPOINT="http://$endpoint" go test -json -count=1 -timeout=5m ./internal/store \
  -run '^TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops$' \
  > "$results/ha-quorum.jsonl" 2> "$results/ha-quorum-stderr.log" || status=$?
cat "$results/ha-quorum-stderr.log" >&2
cat "$results/ha-quorum.jsonl"
[ "$status" -eq 0 ] || exit "$status"
if ! jq -se '
  any(.[]; .Action == "pass" and .Test == "TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops") and
  all(.[]; (((.Test // "") | . == "TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops" or startswith("TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops/")) | not) or (.Action != "skip" and .Action != "fail")) and
  any(.[]; .Test == "TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops" and ((.Output // "") | contains("without quorum: write="))) and
  any(.[]; .Test == "TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops" and ((.Output // "") | contains("acknowledged control request resolved as committed"))) and
  any(.[]; .Test == "TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops" and ((.Output // "") | contains("unacknowledged quorum-loss request resolved:") or contains("quorum-loss request remains unknown or expired;")))
' "$results/ha-quorum.jsonl" >/dev/null; then
  echo 'BLOCKED: HA quorum-loss test missing, skipped, failed, or missing recovery evidence' >&2
  exit 1
fi
