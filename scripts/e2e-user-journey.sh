#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMP_ROOT="${TMP_ROOT:-$ROOT_DIR/.tmp/compos-user-journey-e2e}"
BIN_DIR="$TMP_ROOT/bin"
COMPOS_BIN="$BIN_DIR/compos"
SERVER_BIN="$BIN_DIR/compos-server"
SERVER_PORT="${SERVER_PORT:-49470}"
SERVER_URL="http://127.0.0.1:$SERVER_PORT"
SESSION_PATH="$TMP_ROOT/auth.json"
BOOTSTRAP_SESSION_PATH="$TMP_ROOT/bootstrap-auth.json"
DB_PATH="$TMP_ROOT/compos.db"
SERVER_PID=""
E2E_LOG_PREFIX="compos-user-journey-e2e"
E2E_WAIT_HTTP_DUMP_LOGS=1
# shellcheck source=scripts/e2e-lib.sh
. "$ROOT_DIR/scripts/e2e-lib.sh"
trap cleanup EXIT INT TERM

command -v jq >/dev/null 2>&1 || {
  echo "jq is required" >&2
  exit 1
}

json_assert() {
  jq -e "$1" >/dev/null
}

reset_tmp_root

log "building Go CLI and server"
build_compos

log "starting Compos server"
COMPOS_SQLITE_PATH="$DB_PATH" \
COMPOS_BIND="127.0.0.1:$SERVER_PORT" \
COMPOS_AUTH_MODE=none \
COMPOS_ADMIN_PRINCIPALS=dev \
"$SERVER_BIN" >"$TMP_ROOT/compos-server.log" 2>&1 &
SERVER_PID=$!
E2E_CLEANUP_PIDS="$SERVER_PID"
wait_http "$SERVER_URL/api/v1/health" "compos server"

log "creating workspace"
write_none_session "$BOOTSTRAP_SESSION_PATH" "$SERVER_URL"
workspace_json="$(COMPOS_SESSION_PATH="$BOOTSTRAP_SESSION_PATH" "$COMPOS_BIN" workspace create --server "$SERVER_URL" --name "E2E Workspace" --key e2e)"
workspace_id="$(printf '%s' "$workspace_json" | jq -er '.data.workspace_id')"
write_none_session "$SESSION_PATH" "$SERVER_URL" "$workspace_id"
export COMPOS_SESSION_PATH="$SESSION_PATH"
export COMPOS_LEDGER_PATH="$TMP_ROOT/ledger.db"
"$COMPOS_BIN" whoami | json_assert ".workspace_id == \"$workspace_id\""

contract_schema='{"type":"object","properties":{"amount":{"type":"number"}},"state_machine":{"initial":"draft","transitions":{"draft":["review"],"review":["approved"],"approved":[]}}}'

log "defining and using a dynamic business command"
"$COMPOS_BIN" object-type define \
  --key contracts --singular contract --plural contracts --field-schema "$contract_schema" >/dev/null
contract_json="$("$COMPOS_BIN" contracts create --title "Acme contract" --values '{"amount":120000000}')"
contract_id="$(printf '%s' "$contract_json" | jq -er '.aggregate.id')"
"$COMPOS_BIN" contracts move --id "$contract_id" --version 1 --to-state review >/dev/null
"$COMPOS_BIN" contracts move --id "$contract_id" --version 2 --to-state approved >/dev/null
"$COMPOS_BIN" contracts get --id "$contract_id" | json_assert '.data.state_key == "approved" and .data.version == 3'
"$COMPOS_BIN" objects list | json_assert '.data | map(select(.object_type_key == "contracts")) | length == 1'

log "checking stale precondition rejection"
if "$COMPOS_BIN" contracts move --id "$contract_id" --version 1 --to-state review >"$TMP_ROOT/stale.out" 2>"$TMP_ROOT/stale.err"; then
  cat "$TMP_ROOT/stale.out" >&2
  echo "expected stale contract move to fail" >&2
  exit 1
fi

log "ok workspace=$workspace_id contract=$contract_id"
