package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFormatCount(t *testing.T) {
	for value, want := range map[float64]string{0: "0", 999: "999", 1000: "1,000", 38292.94: "38,293", 1428571.4: "1,428,571"} {
		if got := formatCount(value); got != want {
			t.Errorf("formatCount(%v) = %q, want %q", value, got, want)
		}
	}
}

func TestWithRange(t *testing.T) {
	if got := withRange([]float64{3, 1, 2}, formatCount); got != "2 (1–3)" {
		t.Errorf("got %q", got)
	}
	if got := withRange([]float64{5}, formatCount); got != "5" {
		t.Errorf("a single value should have no range, got %q", got)
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
