#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir "$tmp/bin" "$tmp/results"
cat > "$tmp/bin/docker" <<'EOF'
#!/bin/sh
case "$1" in
  run) echo test-container ;;
  port) echo 127.0.0.1:12345 ;;
  rm) exit 0 ;;
  inspect) exit 1 ;;
  *) exit 2 ;;
esac
EOF
cat > "$tmp/bin/curl" <<'EOF'
#!/bin/sh
exit 0
EOF
cat > "$tmp/bin/go" <<'EOF'
#!/bin/sh
case "$*" in
  *"-run ^TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops$") ;;
  *) exit 2 ;;
esac
name=TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops
[ "${CASE:-}" != missing ] || exit 0
printf '{"Action":"output","Test":"%s","Output":"without quorum: write=timeout"}\n' "$name"
if [ "${CASE:-}" != no_control ]; then
  printf '{"Action":"output","Test":"%s","Output":"acknowledged control request resolved as committed"}\n' "$name"
fi
if [ "${CASE:-}" != no_resolution ]; then
  printf '{"Action":"output","Test":"%s","Output":"quorum-loss request remains unknown or expired;"}\n' "$name"
fi
if [ "${CASE:-}" = skipped ]; then
  printf '{"Action":"skip","Test":"%s/child"}\n' "$name"
fi
if [ "${CASE:-}" = failed ]; then
  printf '{"Action":"fail","Test":"%s"}\n' "$name"
fi
printf '{"Action":"pass","Test":"%s"}\n' "$name"
[ "${CASE:-}" != exit_failed ]
EOF
chmod +x "$tmp/bin/docker" "$tmp/bin/curl" "$tmp/bin/go"
PATH="$tmp/bin:$PATH"; export PATH
cd "$root"
sh scripts/release/ha-quorum-gate.sh "$tmp/results" >/dev/null
for scenario in missing no_control no_resolution skipped failed exit_failed; do
  if CASE=$scenario sh scripts/release/ha-quorum-gate.sh "$tmp/results" >/dev/null 2>&1; then
    echo "HA gate accepted $scenario evidence" >&2
    exit 1
  fi
done
