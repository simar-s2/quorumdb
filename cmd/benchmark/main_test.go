package main

import "testing"

func TestParseCSV(t *testing.T) {
	output := []byte(`"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","38292.94","1.208","0.320","1.111","2.159","5.127","13.287"
"GET","116482.23","0.371","0.104","0.343","0.519","0.671","2.111"
`)
	rows, err := parseCSV(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	set := rows[0]
	if set.Test != "SET" || set.RequestsPerSecond != 38292.94 || set.P50Ms != 1.111 || set.P99Ms != 5.127 || set.MaxMs != 13.287 {
		t.Errorf("SET row = %+v", set)
	}
	if rows[1].Test != "GET" || rows[1].AverageMs != 0.371 || rows[1].P95Ms != 0.519 {
		t.Errorf("GET row = %+v", rows[1])
	}
}

func TestParseNodes(t *testing.T) {
	nodes, err := parseNodes("n1=n1:6379, n2=n2:6379")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 || nodes[1].ID != "n2" || nodes[1].Address != "n2:6379" {
		t.Errorf("got %+v", nodes)
	}
	if _, err := parseNodes("n1"); err == nil {
		t.Error("expected an error for a node without an address")
	}
}
