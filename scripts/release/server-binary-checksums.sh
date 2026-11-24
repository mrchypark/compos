#!/bin/sh
set -eu
cd "$1"
test "$(find . -name 'composd-*.source' | wc -l | tr -d ' ')" -eq 4
for source in composd-*.source; do
  test "$(tr -d '\r\n' < "$source")" = "$GITHUB_SHA"
done
printf '%s\n' "$GITHUB_SHA" > SOURCE_SHA
cat composd-*.sha256 | tr -d '\r' | sort -k 2 > SHA256SUMS
test "$(wc -l < SHA256SUMS)" -eq 4
sha256sum -c SHA256SUMS
for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
  test -f "composd-$target.tar.gz"
  grep -Fx "$(sha256sum "composd-$target.tar.gz")" SHA256SUMS
done
rm composd-*.sha256 composd-*.source
