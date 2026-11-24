#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
GITHUB_SHA=0123456789abcdef0123456789abcdef01234567
export GITHUB_SHA
cd "$tmp"
for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
  name="composd-$target"
  printf '%s\n' "$target" > "$name.tar.gz"
  sha256sum "$name.tar.gz" > "$name.sha256"
  printf '%s\n' "$GITHUB_SHA" > "$name.source"
done
mv composd-linux-amd64.source missing.source
if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
  echo 'missing source passed aggregation' >&2
  exit 1
fi
mv missing.source composd-linux-amd64.source
printf 'wrong-sha\n' > composd-linux-amd64.source
if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
  echo 'wrong source SHA passed aggregation' >&2
  exit 1
fi
printf '%s\n' "$GITHUB_SHA" > composd-linux-amd64.source
cp composd-darwin-arm64.tar.gz original.tar.gz
printf 'corrupted\n' >> composd-darwin-arm64.tar.gz
if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
  echo 'corrupted archive passed aggregation' >&2
  exit 1
fi
mv original.tar.gz composd-darwin-arm64.tar.gz
sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null
test "$(cat SOURCE_SHA)" = "$GITHUB_SHA"
test "$(wc -l < SHA256SUMS | tr -d ' ')" -eq 4
sha256sum -c SHA256SUMS >/dev/null
printf 'corrupted\n' >> composd-darwin-arm64.tar.gz
if sha256sum -c SHA256SUMS >/dev/null 2>&1; then
  echo 'corrupted archive passed checksum verification' >&2
  exit 1
fi
