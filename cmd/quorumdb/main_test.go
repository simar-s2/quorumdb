package main

import (
	"flag"
	"testing"
	"time"
)

func TestSetFlagsFromEnvironment(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	id := flags.String("id", "", "")
	raftAddr := flags.String("raft-addr", ":7000", "")
	heartbeat := flags.Duration("heartbeat", 50*time.Millisecond, "")
	dataDir := flags.String("data", "./data", "")

	t.Setenv("QUORUMDB_ID", "quorumdb-1")
	t.Setenv("QUORUMDB_RAFT_ADDR", ":7100")
	t.Setenv("QUORUMDB_HEARTBEAT", "20ms")
	t.Setenv("QUORUMDB_DATA", "/from-environment")

	if err := setFlagsFromEnvironment(flags); err != nil {
		t.Fatal(err)
	}
	// A flag on the command line overrides the environment.
	if err := flags.Parse([]string{"-data", "/from-command-line"}); err != nil {
		t.Fatal(err)
	}

	if *id != "quorumdb-1" || *raftAddr != ":7100" || *heartbeat != 20*time.Millisecond {
		t.Errorf("got id=%q raft-addr=%q heartbeat=%v, want values from the environment", *id, *raftAddr, *heartbeat)
	}
	if *dataDir != "/from-command-line" {
		t.Errorf("data = %q, want the command-line value", *dataDir)
	}
}

func TestSetFlagsFromEnvironmentRejectsBadValues(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	flags.Duration("heartbeat", 50*time.Millisecond, "")
	t.Setenv("QUORUMDB_HEARTBEAT", "fast")

	if err := setFlagsFromEnvironment(flags); err == nil {
		t.Fatal("expected an error for QUORUMDB_HEARTBEAT=fast")
	}
}
