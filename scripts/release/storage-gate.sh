#!/bin/sh
# The storage owner must supply these real tests; absence or skips block release.
set -eu
results=${1:?results directory required}
mkdir -p "$results"
status=0
go test -json -count=1 -timeout=15m ./internal/store \
  -run '^TestReleaseSingleNode(ArchiveFreshDiskACK|Replay|ProviderFencing)$' \
  > "$results/storage.jsonl" 2>&1 || status=$?
cat "$results/storage.jsonl"
[ "$status" -eq 0 ] || exit "$status"
for name in TestReleaseSingleNodeArchiveFreshDiskACK TestReleaseSingleNodeReplay TestReleaseSingleNodeProviderFencing; do
  if ! jq -se --arg name "$name" \
    'any(.[]; .Action == "pass" and .Test == $name) and
     all(.[]; .Action != "skip" and .Action != "fail")' "$results/storage.jsonl" >/dev/null; then
    echo "BLOCKED: required real storage test missing, skipped, or failed: $name" >&2
    exit 1
  fi
done
