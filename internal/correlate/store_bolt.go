package correlate

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketTraces        = []byte("traces")
	bucketDownloadIndex = []byte("dl_index")
	bucketDedup         = []byte("dedup")
	bucketClosed        = []byte("closed")
)

// boltStore satisfies Store with BoltDB persistence. All write paths fan
// out to disk inside a single bbolt transaction; reads hit the in-memory
// cache that mirrors the on-disk state.
type boltStore struct {
	mu        sync.Mutex
	db        *bolt.DB
	dedupTTL  time.Duration
	recallTTL time.Duration

	// In-memory mirror — populated at Open time and kept consistent on each
	// write. Avoids a bbolt round-trip on every Get.
	byKey  map[MediaKey]*TraceState
	byDLID map[string]MediaKey
	closed map[MediaKey]ClosedTrace
}

// OpenBoltStore opens or creates a BoltDB file at path and rehydrates the
// in-memory cache from disk.
func OpenBoltStore(path string, dedupTTL, recallTTL time.Duration) (Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt: %w", err)
	}
	s := &boltStore{
		db:        db,
		dedupTTL:  dedupTTL,
		recallTTL: recallTTL,
		byKey:     map[MediaKey]*TraceState{},
		byDLID:    map[string]MediaKey{},
		closed:    map[MediaKey]ClosedTrace{},
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketTraces, bucketDownloadIndex, bucketDedup, bucketClosed} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.load(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("rehydrate: %w", err)
	}
	return s, nil
}

func (s *boltStore) load() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(bucketTraces); b != nil {
			if err := b.ForEach(func(k, v []byte) error {
				var ts TraceState
				if err := json.Unmarshal(v, &ts); err != nil {
					return fmt.Errorf("decode trace %s: %w", k, err)
				}
				s.byKey[ts.Key] = &ts
				return nil
			}); err != nil {
				return err
			}
		}
		if b := tx.Bucket(bucketDownloadIndex); b != nil {
			_ = b.ForEach(func(k, v []byte) error {
				var mk MediaKey
				if err := json.Unmarshal(v, &mk); err == nil {
					s.byDLID[string(k)] = mk
				}
				return nil
			})
		}
		if b := tx.Bucket(bucketClosed); b != nil {
			_ = b.ForEach(func(k, v []byte) error {
				var ct ClosedTrace
				if err := json.Unmarshal(v, &ct); err != nil {
					return nil
				}
				mk, err := decodeMediaKey(string(k))
				if err == nil {
					s.closed[mk] = ct
				}
				return nil
			})
		}
		return nil
	})
}

func (s *boltStore) Close() error { return s.db.Close() }

// encodeMediaKey produces a deterministic string form for use as a bbolt key.
// Format: <type>|<tmdb>|<tvdb>|<season>|<episode>
func encodeMediaKey(k MediaKey) string {
	return fmt.Sprintf("%s|%d|%d|%d|%d", k.Type, k.TMDB, k.TVDB, k.Season, k.Episode)
}

func decodeMediaKey(s string) (MediaKey, error) {
	var k MediaKey
	parts := strings.SplitN(s, "|", 5)
	if len(parts) != 5 {
		return k, fmt.Errorf("bad media key %q", s)
	}
	k.Type = MediaType(parts[0])
	if _, err := fmt.Sscanf(parts[1]+" "+parts[2]+" "+parts[3]+" "+parts[4], "%d %d %d %d",
		&k.TMDB, &k.TVDB, &k.Season, &k.Episode); err != nil {
		return k, fmt.Errorf("decode media key %q: %w", s, err)
	}
	return k, nil
}

func (s *boltStore) Get(k MediaKey) (*TraceState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, ok := s.byKey[k]
	return ts, ok
}

func (s *boltStore) GetByDownloadID(id string) (*TraceState, bool) {
	if id == "" {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mk, ok := s.byDLID[id]
	if !ok {
		return nil, false
	}
	ts, ok := s.byKey[mk]
	return ts, ok
}

func (s *boltStore) Put(ts *TraceState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey[ts.Key] = ts
	if ts.DownloadID != "" {
		s.byDLID[ts.DownloadID] = ts.Key
	}
	keyEnc := []byte(encodeMediaKey(ts.Key))
	val, err := json.Marshal(ts)
	if err != nil {
		return // serialisation should not fail on our types; log-and-continue is the right call
	}
	_ = s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketTraces).Put(keyEnc, val); err != nil {
			return err
		}
		if ts.DownloadID != "" {
			mkJSON, _ := json.Marshal(ts.Key)
			if err := tx.Bucket(bucketDownloadIndex).Put([]byte(ts.DownloadID), mkJSON); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *boltStore) Delete(k MediaKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dlid string
	if ts, ok := s.byKey[k]; ok {
		dlid = ts.DownloadID
	}
	delete(s.byKey, k)
	if dlid != "" {
		delete(s.byDLID, dlid)
	}
	_ = s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketTraces).Delete([]byte(encodeMediaKey(k))); err != nil {
			return err
		}
		if dlid != "" {
			_ = tx.Bucket(bucketDownloadIndex).Delete([]byte(dlid))
		}
		return nil
	})
}

func (s *boltStore) Stale(ttl time.Duration, now time.Time) []MediaKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := now.Add(-ttl)
	var out []MediaKey
	for k, ts := range s.byKey {
		if ts.UpdatedAt.Before(cutoff) {
			out = append(out, k)
		}
	}
	return out
}

func (s *boltStore) SeenEvent(src Source, id string, now time.Time) bool {
	if id == "" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dedupKey := []byte(string(src) + "|" + id)
	var seen bool
	cutoff := now.Add(-s.dedupTTL)
	_ = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDedup)
		if v := b.Get(dedupKey); v != nil {
			t := time.Unix(int64(binary.BigEndian.Uint64(v)), 0)
			if t.After(cutoff) {
				seen = true
				return nil
			}
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(now.Unix()))
		return b.Put(dedupKey, buf)
	})
	return !seen
}

func (s *boltStore) RememberClosed(k MediaKey, tid [16]byte, attempt int, outcome Outcome, when time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct := ClosedTrace{TraceID: tid, Attempt: attempt, Outcome: outcome, ClosedAt: when}
	s.closed[k] = ct
	val, _ := json.Marshal(ct)
	_ = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketClosed).Put([]byte(encodeMediaKey(k)), val)
	})
}

func (s *boltStore) RecallClosed(k MediaKey, now time.Time) (ClosedTrace, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.closed[k]
	if !ok && k.Type == MediaTypeTV {
		if c2, ok2 := s.closed[k.SeriesKey()]; ok2 {
			c, ok = c2, true
		}
	}
	if !ok {
		return ClosedTrace{}, false
	}
	if c.ClosedAt.Before(now.Add(-s.recallTTL)) {
		delete(s.closed, k)
		_ = s.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketClosed).Delete([]byte(encodeMediaKey(k)))
		})
		return ClosedTrace{}, false
	}
	return c, true
}

func (s *boltStore) LoadAll() []*TraceState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*TraceState, 0, len(s.byKey))
	for _, ts := range s.byKey {
		ts.Resumed = true
		out = append(out, ts)
	}
	return out
}
