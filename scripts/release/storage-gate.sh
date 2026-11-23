#!/bin/sh
# Real tests must exercise filesystem and each SDK/emulator; absence or skips block release.
set -eu
results=${1:?results directory required}
mkdir -p "$results"
status=0
go test -json -count=1 -timeout=15m ./internal/store \
  -run '^TestReleaseSingleNode(ArchiveRecovery|Replay|ProviderFencing)$' \
  > "$results/storage.jsonl" 2>&1 || status=$?
cat "$results/storage.jsonl"
blocked=0
: > "$results/storage-coverage.env"
for name in TestReleaseSingleNodeArchiveRecovery TestReleaseSingleNodeReplay TestReleaseSingleNodeProviderFencing; do
  if ! jq -se --arg name "$name" \
    'any(.[]; .Action == "pass" and .Test == $name) and
     all(.[]; (((.Test // "") | . == $name or startswith($name + "/")) | not) or (.Action != "skip" and .Action != "fail"))' "$results/storage.jsonl" >/dev/null; then
    echo "BLOCKED: required real storage test missing, skipped, or failed: $name" >&2
    blocked=1
  fi
  for provider in filesystem s3 gcs azure; do
    if jq -se --arg name "$name/$provider" \
      'any(.[]; .Action == "pass" and .Test == $name) and
       all(.[]; (((.Test // "") | . == $name or startswith($name + "/")) | not) or (.Action != "skip" and .Action != "fail"))' "$results/storage.jsonl" >/dev/null; then
      printf '%s=%s\n' "$name/$provider" passed >> "$results/storage-coverage.env"
    else
      printf '%s=%s\n' "$name/$provider" blocked >> "$results/storage-coverage.env"
      echo "BLOCKED: real $provider evidence unavailable, skipped, or failed: $name" >&2
      blocked=1
    fi
  done
done
cat "$results/storage-coverage.env"
[ "$status" -eq 0 ] || exit "$status"
[ "$blocked" -eq 0 ]
