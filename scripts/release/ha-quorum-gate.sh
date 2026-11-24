#!/bin/sh
set -eu
: "${1:?results directory required}"
echo 'BLOCKED: HA quorum-loss and unknown-commit recovery test is not yet available' >&2
exit 1
