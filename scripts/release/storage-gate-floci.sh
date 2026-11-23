#!/bin/sh
set -eu
results=${1:?results directory required}
s3=
gcs=
azure=
cleanup() {
  status=$?
  trap - EXIT
  for container in "$s3" "$gcs" "$azure"; do
    if [ -n "$container" ]; then
      if ! docker rm -f "$container" >/dev/null; then echo "failed to remove container $container" >&2; status=1; fi
      if docker inspect "$container" >/dev/null 2>&1; then echo "container remains: $container" >&2; status=1; fi
    fi
  done
  exit "$status"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
s3=$(docker run -d --rm -p 127.0.0.1::4566 floci/floci@sha256:a0fbf944d7f338cc0856f1ef83831ea749ff2be71956eb7a7fce08fc21458e19)
gcs=$(docker run -d --rm -p 127.0.0.1::4588 floci/floci-gcp@sha256:a59403d43d524b1a08282d67d541a6d37bf7868cbd9d1311caa0d2983adf6f46)
azure=$(docker run -d --rm -p 127.0.0.1::4577 floci/floci-az@sha256:40f1bbf1ef13569c6eacb47e565bf4cd28785ca833208dcc074910e13aec64ea)
COMPOS_RELEASE_S3_ENDPOINT=$(docker port "$s3" 4566/tcp)
COMPOS_RELEASE_GCS_ENDPOINT=$(docker port "$gcs" 4588/tcp)
COMPOS_RELEASE_AZURE_ENDPOINT=$(docker port "$azure" 4577/tcp)
export COMPOS_RELEASE_S3_ENDPOINT COMPOS_RELEASE_GCS_ENDPOINT COMPOS_RELEASE_AZURE_ENDPOINT
for endpoint in "$COMPOS_RELEASE_S3_ENDPOINT" "$COMPOS_RELEASE_GCS_ENDPOINT" "$COMPOS_RELEASE_AZURE_ENDPOINT"; do
  ready=0
  for attempt in $(seq 1 60); do
    if curl -s --max-time 1 -o /dev/null "http://$endpoint/"; then ready=1; break; fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then echo "provider did not start: $endpoint" >&2; exit 1; fi
done
sh scripts/release/storage-gate.sh "$results" "${2:-full}"
