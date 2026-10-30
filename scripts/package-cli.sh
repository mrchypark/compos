#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="${DIST_DIR:-$ROOT_DIR/dist}"
VERSION="${COMPOS_PACKAGE_VERSION:-$(git -C "$ROOT_DIR" describe --tags --always --dirty)}"
GOOS_VALUE="${GOOS:-$(go env GOOS)}"
GOARCH_VALUE="${GOARCH:-$(go env GOARCH)}"
PACKAGE_OS="${COMPOS_PACKAGE_OS:-$GOOS_VALUE}"
EXE=""
if [ "$GOOS_VALUE" = "windows" ]; then
  EXE=".exe"
fi

PACKAGE_NAME="compos_${VERSION}_${PACKAGE_OS}_${GOARCH_VALUE}"
WORK_DIR="$DIST_DIR/.package/$PACKAGE_NAME"
COMMAND_PATH="bin/compos$EXE"

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/bin" "$DIST_DIR"

echo "building compos"
(cd "$ROOT_DIR" && GOOS="$GOOS_VALUE" GOARCH="$GOARCH_VALUE" go build -trimpath -ldflags "-X main.version=$VERSION" -o "$WORK_DIR/bin/compos$EXE" ./cmd/compos)

cat >"$WORK_DIR/README.md" <<EOF
# Compos CLI package

This package ships:

- $COMMAND_PATH

Compos uses an embedded bbolt request ledger for local CLI durability. Normal
mutation commands only need a Compos login session:

    $COMMAND_PATH login --server https://compos.example.com --workspace wks_default
    $COMMAND_PATH object-type define --key contracts --singular contract --plural contracts --field-schema '{"type":"object","properties":{}}'
    $COMMAND_PATH contracts create --title Acme

Use ledger list to inspect request status without exposing command payloads.
Use ledger retry to recover unsent/unknown submissions with their original identity.
No export command or remote synchronization service is required.
EOF

(cd "$DIST_DIR/.package" && tar -czf "$DIST_DIR/$PACKAGE_NAME.tar.gz" "$PACKAGE_NAME")
rm -rf "$DIST_DIR/.package"

printf '%s\n' "$DIST_DIR/$PACKAGE_NAME.tar.gz"
