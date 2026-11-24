#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
GITHUB_SHA=0123456789abcdef0123456789abcdef01234567
export GITHUB_SHA
cd "$tmp"
for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64 windows-arm64; do
  name="composd-$target"
  case "$target" in windows-*) archive="$name.zip";; *) archive="$name.tar.gz";; esac
  printf '%s\n' "$target" > "$archive"
  case "$target" in
    windows-*) sha256sum "$archive" | sed 's/$/\r/' > "$name.sha256"; printf '%s\r\n' "$GITHUB_SHA" > "$name.source";;
    *) sha256sum "$archive" > "$name.sha256"; printf '%s\n' "$GITHUB_SHA" > "$name.source";;
  esac
done
cr=$(printf '\r')
grep -q "${cr}$" composd-windows-amd64.sha256
grep -q "${cr}$" composd-windows-arm64.sha256
cp composd-windows-arm64.zip original.zip
printf 'corrupted\n' >> composd-windows-arm64.zip
if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
  echo 'corrupted archive passed aggregation' >&2
  exit 1
fi
mv original.zip composd-windows-arm64.zip
sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null
test "$(cat SOURCE_SHA)" = "$GITHUB_SHA"
test "$(wc -l < SHA256SUMS | tr -d ' ')" -eq 6
sha256sum -c SHA256SUMS >/dev/null
printf 'corrupted\n' >> composd-windows-arm64.zip
if sha256sum -c SHA256SUMS >/dev/null 2>&1; then
  echo 'corrupted archive passed checksum verification' >&2
  exit 1
fi
