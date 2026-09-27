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

// The WAL is segment files of CRC-framed records; on replay an entry at index i replaces everything from i.

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
	// written counts append calls; callers pass the returned count to syncTo.
	written uint64
	// lastIdx is the index of the last entry written, which equals the in-memory log's last index.
	lastIdx uint64
	synced  atomic.Uint64
}

func segPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016d.wal", seq))
}

// openWAL replays segments from walStart, returns entries after snapIndex and cuts a torn tail.
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

	// Always continue in a fresh segment so only the newest segment can hold a torn write.
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
				// A record at or below the snapshot index replaced everything after the snapshot point.
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

// decodeRecord returns the payload and size of the first record, or n == 0 if it is torn or corrupt.
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

// append buffers records and returns a sequence number for syncTo; it only copies bytes.
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

// syncTo makes appends up to seq durable; one fsync covers every waiting writer (group commit).
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

// rotate seals the current segment and starts one whose startIndex is the current last index.
func (w *wal) rotate() (uint64, error) { return w.rotateTo(false, 0) }

// reset starts a new segment after the whole log was replaced by a snapshot at index.
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

// pickStart returns the newest segment that, with a snapshot at snapIndex, can rebuild the log.
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

// removeBefore deletes segments older than seq, once a snapshot with walStart >= seq is durable.
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

// syncDir fsyncs a directory so creates, renames and deletes are durable (errors are ignored).
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
