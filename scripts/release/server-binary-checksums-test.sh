#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
hold=$(mktemp -d)
trap 'rm -rf "$tmp" "$hold"' EXIT HUP INT TERM
GITHUB_SHA=0123456789abcdef0123456789abcdef01234567
export GITHUB_SHA
cd "$tmp"
for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
  name="composd-$target"
  printf '%s\n' "$target" > "$name.tar.gz"
  sha256sum "$name.tar.gz" > "$name.sha256"
  printf '%s\n' "$GITHUB_SHA" > "$name.source"
done
for suffix in source sha256 tar.gz; do
  mv "composd-linux-amd64.$suffix" "$hold/"
  if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
    echo "missing $suffix passed aggregation" >&2
    exit 1
  fi
  mv "$hold/composd-linux-amd64.$suffix" .
done
mv composd-linux-amd64.source composd-intruder.source
if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
  echo 'renamed source passed aggregation' >&2
  exit 1
fi
mv composd-intruder.source composd-linux-amd64.source
for suffix in sha256 tar.gz; do
  mv "composd-linux-amd64.$suffix" "composd-intruder.$suffix"
  if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
    echo "renamed $suffix passed aggregation" >&2
    exit 1
  fi
  mv "composd-intruder.$suffix" "composd-linux-amd64.$suffix"
done
printf 'extra\n' > composd-extra.tar.gz
if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
  echo 'extra archive passed aggregation' >&2
  exit 1
fi
rm composd-extra.tar.gz
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
printf 'corrupted\n' >> composd-darwin-arm64.tar.gz
if sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null 2>&1; then
  echo 'corrupted archive passed aggregation' >&2
  exit 1
fi
printf 'darwin-arm64\n' > composd-darwin-arm64.tar.gz
sh "$root/scripts/release/server-binary-checksums.sh" "$tmp" >/dev/null
test "$(cat SOURCE_SHA)" = "$GITHUB_SHA"
test "$(wc -l < SHA256SUMS | tr -d ' ')" -eq 4
sha256sum -c SHA256SUMS >/dev/null
printf 'corrupted\n' >> composd-darwin-arm64.tar.gz
if sha256sum -c SHA256SUMS >/dev/null 2>&1; then
  echo 'corrupted archive passed checksum verification' >&2
  exit 1
fi
