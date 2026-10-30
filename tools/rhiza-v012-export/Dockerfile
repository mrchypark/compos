FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /out/rhiza-v012-export .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tar && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/rhiza-v012-export /rhiza-v012-export
ENTRYPOINT ["/rhiza-v012-export"]
