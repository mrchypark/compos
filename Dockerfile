# syntax=docker/dockerfile:1.25

FROM golang:1.27.4-trixie AS go-build
WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/compos-server ./cmd/server

FROM go-build AS go-tools
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/compos-search-syncer ./cmd/search-syncer

FROM debian:trixie-slim AS server

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 compos \
    && useradd --system --uid 10001 --gid compos --home-dir /app --shell /usr/sbin/nologin compos \
    && mkdir -p /app /data \
    && chown -R compos:compos /app /data

COPY --from=go-build /out/compos-server /usr/local/bin/compos-server
COPY LICENSE THIRD_PARTY_NOTICES.md THIRD_PARTY_LICENSES.txt /usr/share/doc/compos/

USER compos
WORKDIR /app

ENV COMPOS_BIND=0.0.0.0:7878
ENV COMPOS_SQLITE_PATH=/data/compos.sqlite
EXPOSE 7878

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:7878/api/v1/health >/dev/null || exit 1

CMD ["compos-server"]

FROM gcr.io/distroless/static-debian13:nonroot AS search-syncer

COPY --from=go-tools /out/compos-search-syncer /usr/local/bin/compos-search-syncer
COPY LICENSE THIRD_PARTY_NOTICES.md THIRD_PARTY_LICENSES.txt /usr/share/doc/compos/

ENTRYPOINT ["/usr/local/bin/compos-search-syncer"]
