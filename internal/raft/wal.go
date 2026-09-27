package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// The write-ahead log is a directory of segment files named <seq>.wal. Each
// file is a sequence of records:
//
//	| crc32c(payload) uint32 | len(payload) uint32 | payload |
//
// The first record of a segment is a header {seq, startIndex}, where
// startIndex is the last log index at the moment the segment was created.
// Every other record is one log entry {index, term, type, data}.
//
// Truncation is implicit. When a follower replaces a conflicting suffix, the
// new entries are appended with indexes lower than the previous record, and
// replay applies the rule "an entry at index i replaces everything from i
// onwards". Replaying the records in order therefore rebuilds the in-memory
// log exactly, with no separate truncate operation to get wrong.
//
// Compaction works in whole segments. A snapshot at index S records walStart,
// the first segment needed to rebuild the log after S. Any segment whose
// startIndex is <= S qualifies: everything after S was appended after that
// segment was created. Older segments are deleted once the snapshot is
// durable. The WAL is rotated after every snapshot so the next snapshot can
// drop the current segment.

const (
	recHeader byte = 1
	recEntry  byte = 2

	recHeaderSize = 8
	maxBuffered   = 1 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var errCorruptWAL = errors.New("raft: corrupt write-ahead log")

type segmentInfo struct {
	seq   uint64
	start uint64
}

type wal struct {
	dir  string
	sync bool

	syncMu sync.Mutex // serializes fsync and rotation
	mu     sync.Mutex // guards everything below
	f      *os.File
	segs   []segmentInfo // ordered by seq; the last one is open for appends
	buf    []byte        // encoded records not yet written to f
	// written counts append calls. A caller that wants its records durable
	// remembers the count its append returned and passes it to syncTo.
	written uint64
	// lastIdx is the index of the last entry written, which always equals
	// the last index of the in-memory log.
	lastIdx uint64
	synced  atomic.Uint64
}

func segPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016d.wal", seq))
}

// openWAL replays segments from walStart onwards and returns the entries
// after snapIndex. A torn record at the end of the last segment (a crash in
// the middle of a write) is cut off; damage anywhere else is an error.
func openWAL(dir string, sync bool, walStart, snapIndex uint64) (*wal, []Entry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	seqs, err := listSegments(dir)
	if err != nil {
		return nil, nil, err
	}
	w := &wal{dir: dir, sync: sync}
	var tail []Entry
	for i, seq := range seqs {
		path := segPath(dir, seq)
		if seq < walStart {
			// Covered by the snapshot; left over from a crash before cleanup.
			if err := os.Remove(path); err != nil {
				return nil, nil, err
			}
			continue
		}
		info, ok, err := replaySegment(path, seq, i == len(seqs)-1, snapIndex, &tail, sync)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			// The last segment was created but its header never hit disk.
			if err := os.Remove(path); err != nil {
				return nil, nil, err
			}
			continue
		}
		w.segs = append(w.segs, info)
	}
	w.lastIdx = snapIndex + uint64(len(tail))

	// Always continue in a fresh segment so that only the newest segment can
	// ever contain a torn write.
	next := walStart
	if len(w.segs) > 0 && w.segs[len(w.segs)-1].seq >= next {
		next = w.segs[len(w.segs)-1].seq + 1
	}
	if next == 0 {
		next = 1
	}
	if err := w.createSegmentLocked(next, w.lastIdx); err != nil {
		return nil, nil, err
	}
	return w, tail, nil
}

func listSegments(dir string) ([]uint64, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var seqs []uint64
	for _, de := range des {
		name := de.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(name, ".wal"), 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

func replaySegment(path string, seq uint64, isLast bool, snapIndex uint64, tail *[]Entry, sync bool) (segmentInfo, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return segmentInfo{}, false, err
	}
	var info segmentInfo
	haveHeader := false
	off := 0
	for off < len(data) {
		payload, n := decodeRecord(data[off:])
		if n == 0 {
			if !isLast {
				return info, false, fmt.Errorf("%w: bad record in %s at offset %d", errCorruptWAL, path, off)
			}
			if err := truncateFile(path, int64(off), sync); err != nil {
				return info, false, err
			}
			break
		}
		switch {
		case payload[0] == recHeader && len(payload) == 17 && off == 0:
			info.seq = binary.LittleEndian.Uint64(payload[1:])
			info.start = binary.LittleEndian.Uint64(payload[9:])
			if info.seq != seq {
				return info, false, fmt.Errorf("%w: %s has header for segment %d", errCorruptWAL, path, info.seq)
			}
			haveHeader = true
		case payload[0] == recEntry && len(payload) >= 18 && haveHeader:
			e := Entry{
				Index: binary.LittleEndian.Uint64(payload[1:]),
				Term:  binary.LittleEndian.Uint64(payload[9:]),
				Type:  EntryType(payload[17]),
				Data:  append([]byte(nil), payload[18:]...),
			}
			if e.Index <= snapIndex {
				// Everything after this index was replaced, including
				// whatever followed the snapshot point.
				*tail = (*tail)[:0]
				break
			}
			pos := e.Index - snapIndex - 1
			if pos > uint64(len(*tail)) {
				return info, false, fmt.Errorf("%w: gap before index %d in %s", errCorruptWAL, e.Index, path)
			}
			*tail = append((*tail)[:pos], e)
		default:
			return info, false, fmt.Errorf("%w: unexpected record in %s at offset %d", errCorruptWAL, path, off)
		}
		off += n
	}
	return info, haveHeader, nil
}

// decodeRecord returns the payload and total size of the record at the start
// of b, or n == 0 if the record is incomplete or fails its checksum.
func decodeRecord(b []byte) (payload []byte, n int) {
	if len(b) < recHeaderSize {
		return nil, 0
	}
	sum := binary.LittleEndian.Uint32(b)
	l := int(binary.LittleEndian.Uint32(b[4:]))
	if l == 0 || recHeaderSize+l > len(b) {
		return nil, 0
	}
	payload = b[recHeaderSize : recHeaderSize+l]
	if crc32.Checksum(payload, crcTable) != sum {
		return nil, 0
	}
	return payload, recHeaderSize + l
}

func appendRecord(b []byte, payload func([]byte) []byte) []byte {
	start := len(b)
	b = append(b, make([]byte, recHeaderSize)...)
	b = payload(b)
	p := b[start+recHeaderSize:]
	binary.LittleEndian.PutUint32(b[start:], crc32.Checksum(p, crcTable))
	binary.LittleEndian.PutUint32(b[start+4:], uint32(len(p)))
	return b
}

func appendEntryRecord(b []byte, e *Entry) []byte {
	return appendRecord(b, func(b []byte) []byte {
		b = append(b, recEntry)
		b = binary.LittleEndian.AppendUint64(b, e.Index)
		b = binary.LittleEndian.AppendUint64(b, e.Term)
		b = append(b, byte(e.Type))
		return append(b, e.Data...)
	})
}

func appendHeaderRecord(b []byte, seq, start uint64) []byte {
	return appendRecord(b, func(b []byte) []byte {
		b = append(b, recHeader)
		b = binary.LittleEndian.AppendUint64(b, seq)
		return binary.LittleEndian.AppendUint64(b, start)
	})
}

// append buffers records for ents and returns a sequence number to pass to
// syncTo. It only copies bytes; the caller holds the Raft lock, so it must be
// cheap.
func (w *wal) append(ents []Entry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range ents {
		w.buf = appendEntryRecord(w.buf, &ents[i])
	}
	if len(ents) > 0 {
		w.lastIdx = ents[len(ents)-1].Index
	}
	w.written++
	if len(w.buf) >= maxBuffered {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
	}
	return w.written, nil
}

func (w *wal) writtenSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written
}

func (w *wal) flushLocked() error {
	if len(w.buf) == 0 {
		return nil
	}
	_, err := w.f.Write(w.buf)
	if cap(w.buf) > 4*maxBuffered {
		w.buf = nil
	} else {
		w.buf = w.buf[:0]
	}
	return err
}

// syncTo makes every record from append calls up to seq durable. Concurrent
// callers share fsyncs: whoever gets syncMu flushes everything buffered so
// far, so one fsync can acknowledge many writers (group commit).
func (w *wal) syncTo(seq uint64) error {
	if w.synced.Load() >= seq {
		return nil
	}
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	if w.synced.Load() >= seq {
		return nil
	}
	w.mu.Lock()
	err := w.flushLocked()
	target, f := w.written, w.f
	w.mu.Unlock()
	if err != nil {
		return err
	}
	if w.sync {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	w.synced.Store(target)
	return nil
}

// rotate seals the current segment and starts a new one whose startIndex is
// the current last index. It returns the new segment's sequence number.
func (w *wal) rotate() (uint64, error) { return w.rotateTo(false, 0) }

// reset starts a new segment for a log that was replaced wholesale by a
// snapshot at index. Replay from that segment begins with an empty log after
// index, so none of the discarded entries can come back.
func (w *wal) reset(index uint64) (uint64, error) { return w.rotateTo(true, index) }

func (w *wal) rotateTo(reset bool, index uint64) (uint64, error) {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.flushLocked(); err != nil {
		return 0, err
	}
	if w.sync {
		if err := w.f.Sync(); err != nil {
			return 0, err
		}
	}
	if err := w.f.Close(); err != nil {
		return 0, err
	}
	if reset {
		w.lastIdx = index
	}
	seq := w.segs[len(w.segs)-1].seq + 1
	if err := w.createSegmentLocked(seq, w.lastIdx); err != nil {
		return 0, err
	}
	w.synced.Store(w.written)
	return seq, nil
}

func (w *wal) createSegmentLocked(seq, start uint64) error {
	f, err := os.OpenFile(segPath(w.dir, seq), os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(appendHeaderRecord(nil, seq, start)); err != nil {
		f.Close()
		return err
	}
	if w.sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		syncDir(w.dir)
	}
	w.f = f
	w.segs = append(w.segs, segmentInfo{seq: seq, start: start})
	return nil
}

// pickStart returns the newest segment that is enough, together with a
// snapshot at snapIndex, to rebuild the log.
func (w *wal) pickStart(snapIndex uint64) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.segs) - 1; i >= 0; i-- {
		if w.segs[i].start <= snapIndex {
			return w.segs[i].seq
		}
	}
	return w.segs[0].seq
}

// removeBefore deletes segments older than seq. Only call it once a snapshot
// with walStart >= seq is durable.
func (w *wal) removeBefore(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	keep := w.segs[:0]
	var firstErr error
	for i, s := range w.segs {
		if s.seq >= seq || i == len(w.segs)-1 {
			keep = append(keep, s)
			continue
		}
		if err := os.Remove(segPath(w.dir, s.seq)); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	w.segs = keep
	return firstErr
}

func (w *wal) segmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segs)
}

func (w *wal) close() error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.flushLocked()
	if err == nil && w.sync {
		err = w.f.Sync()
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

func truncateFile(path string, size int64, sync bool) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	if sync {
		return f.Sync()
	}
	return nil
}

// syncDir makes a create, rename or delete in dir durable. Errors are
// ignored because not every platform supports fsync on a directory.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
