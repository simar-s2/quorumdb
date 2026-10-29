# The build stage runs on the build machine's own CPU type and cross-compiles for the target, so
# building the linux/amd64 image for GKE on an arm64 laptop needs no CPU emulation.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN for program in quorumdb benchmark chaos; do \
      CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/$program ./cmd/$program || exit 1; \
    done

# The image for the benchmark and chaos clients: redis-benchmark plus the Go tools, and kubectl,
# which the chaos test uses to kill pods, freeze processes and apply NetworkPolicies.
FROM alpine:3.22 AS tools
ARG TARGETARCH
ARG KUBECTL_VERSION=v1.36.6
RUN apk add --no-cache redis && adduser -D -H -u 10001 tools \
    && wget -q -O /usr/local/bin/kubectl "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/$TARGETARCH/kubectl" \
    && wget -q -O /tmp/kubectl.sha256 "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/$TARGETARCH/kubectl.sha256" \
    && echo "$(cat /tmp/kubectl.sha256)  /usr/local/bin/kubectl" | sha256sum -c - \
    && chmod +x /usr/local/bin/kubectl && rm /tmp/kubectl.sha256
COPY --from=build /out/benchmark /out/chaos /usr/local/bin/
USER 10001

# The QuorumDB server. It is the last stage, so a plain `docker build` produces it.
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
EXPOSE 6379 7000 8080
ENTRYPOINT ["/sbin/tini", "--", "quorumdb"]
