# shellcheck shell=sh

cleanup() {
  code=$?
  trap - EXIT INT TERM
  for pid in ${E2E_CLEANUP_PIDS:-}; do
    stop_process "$pid"
  done
  exit "$code"
}

log() {
  printf '[%s] %s\n' "$E2E_LOG_PREFIX" "$*"
}

reset_tmp_root() {
  case "$TMP_ROOT" in
    *"/../"*|*"../"*|*"/..")
      printf 'TMP_ROOT must not contain .. path segments, got %s\n' "$TMP_ROOT" >&2
      exit 1
      ;;
  esac
  case "$TMP_ROOT" in
    "$ROOT_DIR"/.tmp|"$ROOT_DIR"/.tmp/*) ;;
    *)
      printf 'TMP_ROOT must be under %s/.tmp, got %s\n' "$ROOT_DIR" "$TMP_ROOT" >&2
      exit 1
      ;;
  esac
  rm -rf "$TMP_ROOT"
  mkdir -p "$BIN_DIR" "$@"
}

wait_http() {
  url=$1
  name=$2
  i=0
  while [ "$i" -lt "${E2E_WAIT_HTTP_ATTEMPTS:-160}" ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.25
  done
  printf '%s did not become ready: %s\n' "$name" "$url" >&2
  if [ "${E2E_WAIT_HTTP_DUMP_LOGS:-0}" = "1" ]; then
    for log_path in "$TMP_ROOT"/*.log; do
      [ -f "$log_path" ] || continue
      printf '\n--- %s ---\n' "$log_path" >&2
      tail -80 "$log_path" >&2 || true
    done
  fi
  return 1
}

stop_process() {
  pid=$1
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

build_compos() {
  (cd "$ROOT_DIR" && go build -o "$COMPOS_BIN" ./cmd/compos)
  (cd "$ROOT_DIR" && go build -o "$SERVER_BIN" ./cmd/server)
}
