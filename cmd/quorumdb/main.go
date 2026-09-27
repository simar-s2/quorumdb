// Command quorumdb runs one node of a QuorumDB cluster.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/simar-s2/quorumdb/internal/raft"
	"github.com/simar-s2/quorumdb/internal/server"
)

func main() {
	var (
		id         = flag.String("id", "", "unique node ID (required)")
		redisAddr  = flag.String("redis-addr", ":6379", "address for Redis clients")
		raftAddr   = flag.String("raft-addr", ":7000", "address for peer (Raft) traffic")
		peers      = flag.String("peers", "", "comma-separated id=host:port for every member, e.g. n1=n1:7000,n2=n2:7000,n3=n3:7000")
		dataDir    = flag.String("data", "./data", "directory for the write-ahead log and snapshots")
		heartbeat  = flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
		election   = flag.Duration("election-timeout", 300*time.Millisecond, "minimum election timeout (randomized up to 2x)")
		snapEvery  = flag.Uint64("snapshot-threshold", 10000, "applied entries between snapshots")
		fsync      = flag.Bool("fsync", true, "fsync the log before acknowledging (disable only for experiments)")
		cmdTimeout = flag.Duration("command-timeout", 5*time.Second, "how long a client command may wait for commit")
		logLevel   = flag.String("log-level", "info", "debug, info, warn or error")

		unsafeLocalReads = flag.Bool("unsafe-local-reads", false, "UNSAFE: serve reads from local state without ReadIndex (negative control for the chaos test)")
		unsafeEarlyAck   = flag.Bool("unsafe-early-ack", false, "UNSAFE: acknowledge SET before it commits (negative control for the chaos test)")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fatal("bad -log-level: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if *id == "" {
		fatal("-id is required")
	}
	members, err := parsePeers(*peers)
	if err != nil {
		fatal("bad -peers: %v", err)
	}

	srv, err := server.New(server.Config{
		ID:        *id,
		RedisAddr: *redisAddr,
		RaftAddr:  *raftAddr,
		Peers:     members,
		DataDir:   *dataDir,
		Raft: raft.Config{
			HeartbeatInterval: *heartbeat,
			ElectionTimeout:   *election,
			SnapshotThreshold: *snapEvery,
			NoSync:            !*fsync,
		},
		CommandTimeout:   *cmdTimeout,
		Logger:           logger,
		UnsafeLocalReads: *unsafeLocalReads,
		UnsafeEarlyAck:   *unsafeEarlyAck,
	})
	if err != nil {
		fatal("starting node: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	logger.Info("shutting down", "signal", s.String())
	srv.Close()
}

func parsePeers(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("%q is not id=host:port", part)
		}
		out[id] = addr
	}
	return out, nil
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "quorumdb: "+format+"\n", args...)
	os.Exit(1)
}
