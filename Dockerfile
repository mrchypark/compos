# syntax=docker/dockerfile:1.25

FROM golang:1.27.0-trixie AS go-build
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
    go build -trimpath -ldflags="-s -w" -o /out/compos-storage-import ./cmd/storage-import \
    && go build -trimpath -ldflags="-s -w" -o /out/compos ./cmd/compos \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/compos-search-syncer ./cmd/search-syncer

FROM rust:1.96.1-slim-trixie AS operator-build
WORKDIR /app

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential ca-certificates cmake git \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release -p compos-operator \
    && mkdir -p /out \
    && cp /app/target/release/compos-operator /out/compos-operator

FROM debian:trixie-slim AS server

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 compos \
    && useradd --system --uid 10001 --gid compos --home-dir /app --shell /usr/sbin/nologin compos \
    && mkdir -p /app /data \
    && chown -R compos:compos /app /data

COPY --from=go-build /out/compos-server /usr/local/bin/compos-server

USER compos
WORKDIR /app

ENV COMPOS_BIND=0.0.0.0:7878
ENV COMPOS_SQLITE_PATH=/data/compos.sqlite
EXPOSE 7878

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:7878/api/v1/health >/dev/null || exit 1

CMD ["compos-server"]

# Offline logical migration only; this image does not start an API or scheduler.
FROM server AS storage-import
COPY --from=go-tools /out/compos-storage-import /usr/local/bin/compos-storage-import
HEALTHCHECK NONE
CMD ["compos-storage-import"]

FROM debian:trixie-slim AS cli

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 compos \
    && useradd --system --uid 10001 --gid compos --home-dir /home/compos --create-home --shell /usr/sbin/nologin compos

COPY --from=go-tools /out/compos /usr/local/bin/compos

USER compos
WORKDIR /work
ENV HOME=/home/compos

ENTRYPOINT ["compos"]

FROM debian:trixie-slim AS operator

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 compos \
    && useradd --system --uid 10001 --gid compos --home-dir /app --shell /usr/sbin/nologin compos \
    && mkdir -p /app \
    && chown -R compos:compos /app

COPY --from=operator-build /out/compos-operator /usr/local/bin/compos-operator

USER compos
WORKDIR /app
ENTRYPOINT ["compos-operator"]

FROM gcr.io/distroless/static-debian13:nonroot AS search-syncer

COPY --from=go-tools /out/compos-search-syncer /usr/local/bin/compos-search-syncer

ENTRYPOINT ["/usr/local/bin/compos-search-syncer"]
