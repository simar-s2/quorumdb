package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// storage owns a node's data directory:
//
//	meta      current term and vote (Figure 2's persistent state)
//	snapshot  latest state machine snapshot plus its index, term and walStart
//	wal/      log segments, see wal.go
//
// meta and snapshot are replaced atomically: write a temp file, fsync it,
// rename it over the old one, fsync the directory.
type storage struct {
	dir  string
	sync bool
	wal  *wal
}

type snapshotMeta struct {
	Index    uint64
	Term     uint64
	WALStart uint64
}

// recovered is everything read back from disk at startup.
type recovered struct {
	term     uint64
	votedFor string
	snap     snapshotMeta
	snapData []byte // nil if there is no snapshot
	entries  []Entry
}

var snapMagic = []byte("QSNP")

func openStorage(dir string, sync bool) (*storage, *recovered, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	s := &storage{dir: dir, sync: sync}
	rec := &recovered{}
	var err error
	rec.term, rec.votedFor, err = s.loadMeta()
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	rec.snap, rec.snapData, err = s.loadSnapshot()
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	s.wal, rec.entries, err = openWAL(filepath.Join(dir, "wal"), sync, rec.snap.WALStart, rec.snap.Index)
	if err != nil {
		return nil, nil, err
	}
	return s, rec, nil
}

func (s *storage) metaPath() string     { return filepath.Join(s.dir, "meta") }
func (s *storage) snapshotPath() string { return filepath.Join(s.dir, "snapshot") }

// saveMeta encodes crc32c | term | votedFor.
func (s *storage) saveMeta(term uint64, votedFor string) error {
	body := binary.LittleEndian.AppendUint64(nil, term)
	body = append(body, votedFor...)
	b := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, crcTable))
	return writeFileAtomic(s.metaPath(), append(b, body...), s.sync)
}

func (s *storage) loadMeta() (uint64, string, error) {
	b, err := os.ReadFile(s.metaPath())
	if err != nil {
		return 0, "", err
	}
	if len(b) < 12 || crc32.Checksum(b[4:], crcTable) != binary.LittleEndian.Uint32(b) {
		return 0, "", errors.New("raft: corrupt meta file")
	}
	return binary.LittleEndian.Uint64(b[4:]), string(b[12:]), nil
}

// saveSnapshot encodes magic | crc32c | index | term | walStart | data.
func (s *storage) saveSnapshot(m snapshotMeta, data []byte) error {
	body := make([]byte, 0, 24+len(data))
	body = binary.LittleEndian.AppendUint64(body, m.Index)
	body = binary.LittleEndian.AppendUint64(body, m.Term)
	body = binary.LittleEndian.AppendUint64(body, m.WALStart)
	body = append(body, data...)
	b := append([]byte(nil), snapMagic...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(body, crcTable))
	return writeFileAtomic(s.snapshotPath(), append(b, body...), s.sync)
}

func (s *storage) loadSnapshot() (snapshotMeta, []byte, error) {
	b, err := os.ReadFile(s.snapshotPath())
	if err != nil {
		return snapshotMeta{}, nil, err
	}
	if len(b) < 32 || string(b[:4]) != string(snapMagic) || crc32.Checksum(b[8:], crcTable) != binary.LittleEndian.Uint32(b[4:]) {
		return snapshotMeta{}, nil, fmt.Errorf("raft: corrupt snapshot file %s", s.snapshotPath())
	}
	m := snapshotMeta{
		Index:    binary.LittleEndian.Uint64(b[8:]),
		Term:     binary.LittleEndian.Uint64(b[16:]),
		WALStart: binary.LittleEndian.Uint64(b[24:]),
	}
	return m, b[32:], nil
}

func (s *storage) close() error { return s.wal.close() }

func writeFileAtomic(path string, data []byte, sync bool) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if sync {
		syncDir(filepath.Dir(path))
	}
	return nil
}
