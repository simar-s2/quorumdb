FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/quorumdb ./cmd/quorumdb

FROM alpine:3.22
RUN apk add --no-cache redis
COPY --from=build /out/quorumdb /usr/local/bin/quorumdb
VOLUME /data
EXPOSE 6379 7000
ENTRYPOINT ["quorumdb"]
