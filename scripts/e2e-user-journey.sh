#!/bin/sh
set -eu

ROOT_DIR="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

# External Goauthy/Grant adapters are not wired. These journeys inject authority
# only in test binaries; the standalone server must continue to fail closed.
printf '%s\n' 'Running application journeys with an injected test authority (not a live Goauthy/Grant verification).'
go test ./cmd/compos -run 'TestDynamicObjectTypeCreateUsesLiveCommandSurface|TestWorkspaceCreate_explicitServerRequiresExternalIdentity|TestObjectsList_explicitServerRequiresExternalIdentity' -count=1
go test ./internal/server -run 'TestExternalAuthority|TestWorkspace|TestWorkflow' -count=1
