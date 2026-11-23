#!/bin/sh
# Real tests must exercise filesystem and each SDK/emulator; absence or skips block release.
set -eu
results=${1:?results directory required}
mode=${2:-full}
case "$mode" in
  recovery) tests='ArchiveRecovery|Replay|CrashRecovery'; names='ArchiveRecovery Replay CrashRecovery' ;;
  full) tests='ArchiveRecovery|Replay|CrashRecovery|ProviderFencing'; names='ArchiveRecovery Replay CrashRecovery ProviderFencing' ;;
  *) echo "unknown storage gate mode: $mode" >&2; exit 2 ;;
esac
mkdir -p "$results"
status=0
go test -json -count=1 -timeout=15m ./internal/store \
  -run "^TestReleaseSingleNode($tests)$" \
  > "$results/storage.jsonl" 2> "$results/storage-stderr.log" || status=$?
cat "$results/storage-stderr.log" >&2
cat "$results/storage.jsonl"
blocked=0
: > "$results/storage-coverage.env"
for suffix in $names; do
  name=TestReleaseSingleNode$suffix
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
