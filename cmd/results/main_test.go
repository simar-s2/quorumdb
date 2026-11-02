package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/simar-s2/quorumdb/internal/results"
)

func TestFormatCount(t *testing.T) {
	for value, want := range map[float64]string{0: "0", 999: "999", 1000: "1,000", 38292.94: "38,293", 1428571.4: "1,428,571"} {
		if got := formatCount(value); got != want {
			t.Errorf("formatCount(%v) = %q, want %q", value, got, want)
		}
	}
}

func TestDescribeFailovers(t *testing.T) {
	c := &results.Chaos{
		FailoverMilliseconds: map[string][]float64{"kill": {400, 600}, "leader-zone": {500, 1200}},
		FailoverTimeouts:     map[string]int{"leader-zone": 1},
	}
	want := "550 ms typical, 1,200 ms slowest, **1 times no new leader within 10 s**"
	if got := describeFailovers(c); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestReplaceSection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "README.md")
	os.WriteFile(path, []byte("intro\n"+beginMarker+"\nold table\n"+endMarker+"\noutro\n"), 0o644)
	if err := replaceSection(path, "new table\n"); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	text := string(data)
	if strings.Contains(text, "old table") || !strings.Contains(text, "new table") || !strings.HasSuffix(text, endMarker+"\noutro\n") {
		t.Errorf("unexpected result:\n%s", text)
	}
}
