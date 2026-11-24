#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir "$tmp/bin" "$tmp/results"
cat > "$tmp/bin/go" <<'EOF'
#!/bin/sh
echo 'go: downloading example.test/dependency' >&2
case "$*" in
  *'TestReleaseSingleNode(ArchiveRecovery|Replay|CrashRecovery)'*) ;;
  *) exit 2 ;;
esac
for test in ArchiveRecovery Replay CrashRecovery; do
  name="TestReleaseSingleNode$test"
  printf '{"Action":"pass","Test":"%s"}\n' "$name"
  for provider in filesystem s3 gcs azure; do
    [ "${MISSING:-}" != "$name/$provider" ] || continue
    [ "${CASE:-}" != missing ] || [ "$name/$provider" != TestReleaseSingleNodeReplay/gcs ] || continue
    action=pass
    if [ "${CASE:-}" = skipped ] && [ "$name/$provider" = TestReleaseSingleNodeCrashRecovery/azure ]; then action=skip; fi
    if [ "${CASE:-}" = result_failed ] && [ "$name/$provider" = TestReleaseSingleNodeCrashRecovery/azure ]; then action=fail; fi
    printf '{"Action":"%s","Test":"%s/%s"}\n' "$action" "$name" "$provider"
  done
done
[ "${CASE:-}" != failed ]
EOF
chmod +x "$tmp/bin/go"
PATH="$tmp/bin:$PATH"; export PATH
cd "$root"
CASE=pass sh scripts/release/storage-gate.sh "$tmp/results" >/dev/null 2>&1
CASE=pass sh scripts/release/storage-gate.sh "$tmp/results" recovery >/dev/null 2>&1
grep -qx 'go: downloading example.test/dependency' "$tmp/results/storage-stderr.log"
for test in ArchiveRecovery Replay CrashRecovery; do
  for provider in filesystem s3 gcs azure; do
    for mode in full recovery; do
      if MISSING="TestReleaseSingleNode$test/$provider" sh scripts/release/storage-gate.sh "$tmp/results" "$mode" >/dev/null 2>&1; then
        echo "$mode gate accepted missing $test/$provider" >&2
        exit 1
      fi
    done
  done
done
for scenario in missing skipped failed result_failed; do
  if CASE=$scenario sh scripts/release/storage-gate.sh "$tmp/results" >/dev/null 2>&1; then
    echo "storage gate accepted $scenario evidence" >&2
    exit 1
  fi
  if [ "$scenario" = result_failed ]; then
    grep -qx 'TestReleaseSingleNodeArchiveRecovery/s3=passed' "$tmp/results/storage-coverage.env"
    grep -qx 'TestReleaseSingleNodeCrashRecovery/azure=blocked' "$tmp/results/storage-coverage.env"
  fi
done
