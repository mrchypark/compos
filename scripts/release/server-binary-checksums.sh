#!/bin/sh
set -eu
cd "$1"
test "$(find . ! -name . | wc -l | tr -d ' ')" -eq 12
for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
  name="composd-$target"
  test -f "$name.source"
  test -f "$name.sha256"
  test -f "$name.tar.gz"
  test "$(tr -d '\r\n' < "$name.source")" = "$GITHUB_SHA"
  test "$(wc -l < "$name.sha256")" -eq 1
  test "$(tr -d '\r\n' < "$name.sha256")" = "$(sha256sum "$name.tar.gz")"
done
printf '%s\n' "$GITHUB_SHA" > SOURCE_SHA
cat composd-*.sha256 | tr -d '\r' | sort -k 2 > SHA256SUMS
test "$(wc -l < SHA256SUMS)" -eq 4
sha256sum -c SHA256SUMS
rm composd-*.sha256 composd-*.source
