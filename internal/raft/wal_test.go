package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func mkEntries(from, to, term uint64) []Entry {
	var es []Entry
	for i := from; i <= to; i++ {
		es = append(es, Entry{Index: i, Term: term, Type: EntryCommand, Data: []byte(fmt.Sprintf("%d@%d", i, term))})
	}
	return es
}

func describe(es []Entry) string {
	s := ""
	for _, e := range es {
		s += fmt.Sprintf("%d:%d ", e.Index, e.Term)
	}
	return s
}

func mustAppend(t *testing.T, w *wal, es []Entry) {
	t.Helper()
	seq, err := w.append(es)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.syncTo(seq); err != nil {
		t.Fatal(err)
	}
}

func TestWALReplayAppliesImplicitTruncation(t *testing.T) {
	dir := t.TempDir()
	w, ents, err := openWAL(dir, true, 0, 0)
	if err != nil || len(ents) != 0 {
		t.Fatal(err, ents)
	}
	mustAppend(t, w, mkEntries(1, 5, 1))
	mustAppend(t, w, mkEntries(3, 4, 2)) // overwrites 3..5
	mustAppend(t, w, mkEntries(5, 6, 2))
	w.close()

	_, ents, err = openWAL(dir, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := describe(ents), "1:1 2:1 3:2 4:2 5:2 6:2 "; got != want {
		t.Fatalf("replayed %q, want %q", got, want)
	}
}

func TestWALTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	w, _, err := openWAL(dir, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, mkEntries(1, 3, 1))
	w.close()

	// Simulate a crash halfway through writing a record.
	seqs, _ := listSegments(dir)
	path := segPath(dir, seqs[len(seqs)-1])
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	rec := appendEntryRecord(nil, &Entry{Index: 4, Term: 1, Data: []byte("half-written")})
	f.Write(rec[:len(rec)-3])
	f.Close()

	w, ents, err := openWAL(dir, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := describe(ents); got != "1:1 2:1 3:1 " {
		t.Fatalf("replayed %q after torn write", got)
	}
	// Appending after recovery and reopening again must work.
	mustAppend(t, w, mkEntries(4, 4, 1))
	w.close()
	if _, ents, err = openWAL(dir, true, 0, 0); err != nil || describe(ents) != "1:1 2:1 3:1 4:1 " {
		t.Fatalf("second reopen: %v %q", err, describe(ents))
	}
}

func TestWALCorruptionInOldSegmentIsAnError(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := openWAL(dir, true, 0, 0)
	mustAppend(t, w, mkEntries(1, 3, 1))
	w.rotate()
	mustAppend(t, w, mkEntries(4, 5, 1))
	w.close()

	seqs, _ := listSegments(dir)
	path := segPath(dir, seqs[0])
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xff
	os.WriteFile(path, b, 0o644)
	if _, _, err := openWAL(dir, true, 0, 0); err == nil {
		t.Fatal("expected an error for a corrupt record in a sealed segment")
	}
}

func TestWALCompactionWithSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := openWAL(dir, true, 0, 0)
	mustAppend(t, w, mkEntries(1, 10, 1))
	w.rotate() // segment 2 starts at index 10
	mustAppend(t, w, mkEntries(11, 20, 1))
	w.rotate() // segment 3 starts at index 20
	mustAppend(t, w, mkEntries(21, 25, 1))

	// A snapshot at 15 needs segment 2 onwards; at 20, segment 3 onwards.
	if got := w.pickStart(15); got != 2 {
		t.Fatalf("pickStart(15) = %d, want 2", got)
	}
	start := w.pickStart(20)
	if start != 3 {
		t.Fatalf("pickStart(20) = %d, want 3", start)
	}
	w.removeBefore(start)
	w.close()
	if seqs, _ := listSegments(dir); seqs[0] != 3 {
		t.Fatalf("segments after compaction: %v", seqs)
	}
	_, ents, err := openWAL(dir, true, start, 20)
	if err != nil {
		t.Fatal(err)
	}
	if got := describe(ents); got != "21:1 22:1 23:1 24:1 25:1 " {
		t.Fatalf("replay after compaction = %q", got)
	}
}

// Replay must not resurrect entries replaced by a record at or below the snapshot index.
func TestWALTruncationBelowSnapshotPoint(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := openWAL(dir, true, 0, 0)
	mustAppend(t, w, mkEntries(1, 10, 1))
	mustAppend(t, w, mkEntries(5, 7, 2)) // log is now 1..4@1, 5..7@2
	w.close()
	_, ents, err := openWAL(dir, true, 0, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("stale entries resurrected after snapshot at 7: %q", describe(ents))
	}
}

func TestWALResetDiscardsLog(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := openWAL(dir, true, 0, 0)
	mustAppend(t, w, mkEntries(1, 10, 3))
	start, err := w.reset(50) // replaced by a snapshot at 50
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, mkEntries(51, 52, 4))
	w.close()
	_, ents, err := openWAL(dir, true, start, 50)
	if err != nil {
		t.Fatal(err)
	}
	if got := describe(ents); got != "51:4 52:4 " {
		t.Fatalf("replay after reset = %q", got)
	}
}

func TestStorageMetaAndSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, rec, err := openStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if rec.term != 0 || rec.snapData != nil {
		t.Fatalf("fresh storage recovered %+v", rec)
	}
	if err := s.saveMeta(7, "n2"); err != nil {
		t.Fatal(err)
	}
	if err := s.saveSnapshot(snapshotMeta{Index: 0, Term: 0, WALStart: 1}, []byte("state")); err != nil {
		t.Fatal(err)
	}
	s.close()
	_, rec, err = openStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if rec.term != 7 || rec.votedFor != "n2" || string(rec.snapData) != "state" || rec.snap.WALStart != 1 {
		t.Fatalf("recovered %+v", rec)
	}
	// A corrupt meta file must be detected, not silently read as term 0.
	os.WriteFile(filepath.Join(dir, "meta"), []byte("garbage-garbage"), 0o644)
	if _, _, err := openStorage(dir, true); err == nil {
		t.Fatal("expected error for corrupt meta")
	}
}
