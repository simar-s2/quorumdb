# The build stage runs on the build machine's own CPU type and cross-compiles for the target, so
# building the linux/amd64 image for GKE on an arm64 laptop needs no CPU emulation.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/quorumdb ./cmd/quorumdb

FROM alpine:3.22 AS server
# tini runs as PID 1 and starts quorumdb as its child. The kernel ignores SIGSTOP and SIGKILL sent to
# PID 1 from inside the container, so this is what lets the chaos test freeze or kill the server with
# `kubectl exec`. tini also forwards SIGTERM to quorumdb for a clean shutdown.
RUN apk add --no-cache redis tini \
    && adduser -D -H -u 10001 quorumdb \
    && mkdir /data && chown quorumdb /data
COPY --from=build /out/quorumdb /usr/local/bin/quorumdb
USER 10001
VOLUME /data
EXPOSE 6379 7000
ENTRYPOINT ["/sbin/tini", "--", "quorumdb"]
