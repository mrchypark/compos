#!/bin/sh
set -eu

ROOT_DIR="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Authentication is external to Compos. These journeys exercise the default
# credential-free HTTP contract and do not claim upstream gateway verification.
printf '%s\n' 'Running credential-free server application journeys.'
go test ./internal/server -run 'TestHTTPWorkspaceRegistryAndCommandCompatibility|TestWorkflow' -count=1
