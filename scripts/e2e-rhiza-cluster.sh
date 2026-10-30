#!/bin/sh
set -eu

ROOT_DIR="$(cd -- "$(dirname -- "$0")/.." && pwd)"
NAME="compos-rhiza-e2e-$$"
MINIO_IMAGE="${MINIO_IMAGE:-ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9}"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker network rm "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker network create "$NAME" >/dev/null
docker run -d --name "$NAME" --network "$NAME" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=compos \
  -e MINIO_ROOT_PASSWORD=compos-secret \
  "$MINIO_IMAGE" server /data >/dev/null

PORT="$(docker port "$NAME" 9000/tcp | sed -n 's/.*://p' | head -1)"
attempt=0
until curl --max-time 2 -fsS "http://127.0.0.1:$PORT/minio/health/live" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    docker logs "$NAME" >&2
    exit 1
  fi
  sleep 1
done

cd "$ROOT_DIR"
COMPOS_RHIZA_CLUSTER_TEST_ENDPOINT="http://127.0.0.1:$PORT" \
  go test ./internal/store \
  -run TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops \
  -count=1 -v
