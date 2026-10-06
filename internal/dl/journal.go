package dl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Journal is the crash-safe resume record for one snapshot download.
//
// It is an append-only JSONL file with a header line and one entry per
// completed block (ordinal, length, SHA-256). Entries are buffered in
// memory and flushed by Checkpoint, which the manager calls only after
// fsyncing the image file. That ordering guarantees: whenever the journal
// says a block is done, its bytes are durable on disk. Blocks written but
// not yet checkpointed are simply re-downloaded after a crash.
//
// A zero SHA is a clear marker (paranoid resume invalidated the block);
// later lines override earlier ones on replay.
type Journal struct {
	mu         sync.Mutex
	path       string
	snapshotID string
	blocks     int
	base       string // lineage base for incremental runs
	entries    map[int]JournalEntry
	pending    []JournalEntry
	f          *os.File
	w          *bufio.Writer
	dirty      bool
}

// JournalEntry records the completion of one block ordinal.
type JournalEntry struct {
	Ordinal int    `json:"o"`
	Length  int64  `json:"len"`
	SHA256  string `json:"sha"`
}

type journalHeader struct {
	V int `json:"v"`
	// Base is the lineage base snapshot for incremental runs ("" for
	// full downloads). A resume with a different base is refused.
	Base   string `json:"base,omitempty"`
	ID     string `json:"id"`
	Blocks int    `json:"blocks"`
}

// JournalMismatchError reports a journal belonging to a different
// download (different snapshot or block count).
type JournalMismatchError struct{ Path, Detail string }

func (e *JournalMismatchError) Error() string {
	return fmt.Sprintf("journal %s: %s", e.Path, e.Detail)
}

// OpenJournal opens (creating if absent) the journal at path. When the
// file exists its header must match snapshotID and blockCount.
func OpenJournal(path, snapshotID string, blockCount int, baseSnapshotID string) (*Journal, error) {
	j := &Journal{
		path:       path,
		snapshotID: snapshotID,
		blocks:     blockCount,
		base:       baseSnapshotID,
		entries:    make(map[int]JournalEntry),
	}

	if _, err := os.Stat(path); err == nil {
		if err := j.replay(); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	j.f = f
	j.w = bufio.NewWriter(f)
	if len(j.entries) == 0 && j.headerNeeded() {
		if err := j.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
	}
	return j, nil
}

// headerNeeded reports whether the file on disk lacks a header line
// (fresh journal that was created but never checkpointed).
func (j *Journal) headerNeeded() bool {
	st, err := os.Stat(j.path)
	return err != nil || st.Size() == 0
}

func (j *Journal) writeHeader() error {
	h := journalHeader{V: 1, Base: j.base, ID: j.snapshotID, Blocks: j.blocks}
	line, err := json.Marshal(h)
	if err != nil {
		return err
	}
	j.w.Write(line)
	j.w.WriteByte('\n')
	j.dirty = true
	return nil
}

// replay reads an existing journal file into memory.
func (j *Journal) replay() error {
	f, err := os.Open(j.path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 64*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if lineNo == 1 {
			var h journalHeader
			if err := json.Unmarshal(line, &h); err != nil {
				return &JournalMismatchError{Path: j.path, Detail: fmt.Sprintf("bad header: %v", err)}
			}
			if h.V != 1 {
				return &JournalMismatchError{Path: j.path, Detail: fmt.Sprintf("unsupported version %d", h.V)}
			}
			if h.ID != j.snapshotID {
				return &JournalMismatchError{Path: j.path, Detail: fmt.Sprintf("belongs to snapshot %s, not %s", h.ID, j.snapshotID)}
			}
			if h.Base != j.base {
				return &JournalMismatchError{Path: j.path, Detail: fmt.Sprintf("is an incremental sync from base %q, not %q", h.Base, j.base)}
			}
			if h.Blocks != j.blocks {
				return &JournalMismatchError{Path: j.path, Detail: fmt.Sprintf("expects %d blocks, listing produced %d", h.Blocks, j.blocks)}
			}
			continue
		}
		var e JournalEntry
		if err := json.Unmarshal(line, &e); err != nil {
			// Truncated tail from a crash mid-append: ignore, the block
			// is re-downloaded.
			continue
		}
		if e.Ordinal < 0 || e.Ordinal >= j.blocks {
			continue
		}
		if e.SHA256 == "" {
			delete(j.entries, e.Ordinal) // clear marker
		} else {
			j.entries[e.Ordinal] = e
		}
	}
	return sc.Err()
}

// Has reports whether the given ordinal has a recorded completion.
func (j *Journal) Has(ordinal int) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	_, ok := j.entries[ordinal]
	return ok
}

// Get returns the recorded entry for an ordinal.
func (j *Journal) Get(ordinal int) (JournalEntry, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e, ok := j.entries[ordinal]
	return e, ok
}

// Count returns how many blocks are recorded complete.
func (j *Journal) Count() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.entries)
}

// Record buffers the completion of a block. It becomes durable at the
// next Checkpoint (which the manager performs after fsyncing the image).
func (j *Journal) Record(e JournalEntry) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries[e.Ordinal] = e
	j.pending = append(j.pending, e)
	j.dirty = true
}

// Clear invalidates a recorded completion (paranoid resume found corrupt
// data); buffered as an override entry.
func (j *Journal) Clear(ordinal int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.entries, ordinal)
	j.pending = append(j.pending, JournalEntry{Ordinal: ordinal})
	j.dirty = true
}

// Checkpoint flushes buffered entries to disk with fsync. The caller
// MUST have fsynced the image file before calling.
func (j *Journal) Checkpoint() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.dirty {
		return nil
	}
	for _, e := range j.pending {
		line, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := j.w.Write(line); err != nil {
			return err
		}
		if err := j.w.WriteByte('\n'); err != nil {
			return err
		}
	}
	j.pending = j.pending[:0]
	if err := j.w.Flush(); err != nil {
		return err
	}
	if err := j.f.Sync(); err != nil {
		return err
	}
	j.dirty = false
	return nil
}

// Close flushes and closes the journal file (keeping the file on disk).
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.w.Flush(); err != nil {
		j.f.Close()
		return err
	}
	return j.f.Close()
}

// Remove deletes the journal file (after a fully successful run).
func (j *Journal) Remove() error {
	if j.f != nil {
		j.f.Close()
		j.f = nil
	}
	err := os.Remove(j.path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// TempName returns a scratch path next to the journal.
func TempName(forPath string) string {
	return filepath.Join(filepath.Dir(forPath), "."+filepath.Base(forPath)+".tmp")
}
