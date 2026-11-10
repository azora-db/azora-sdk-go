package azora

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/azora-db/azora-go/internal/azp"
)

// shapeKey names a table version.
type shapeKey struct {
	table string
	tv    int64
}

// shapeInfo is a table version's columns and primary key.
type shapeInfo struct{ cols, pk []string }

// shapeCache holds column layouts by (table, version). Versions are never
// reused for another layout, so the cache survives reconnects.
type shapeCache struct {
	mu      sync.Mutex
	m       map[shapeKey]*shapeInfo
	changed chan struct{}
}

// put stores a Shape and wakes waiters.
func (sc *shapeCache) put(s *azp.Shape) {
	sc.mu.Lock()
	sc.m[shapeKey{strings.ToLower(s.Table), s.TV}] = &shapeInfo{cols: s.Columns, pk: s.PK}
	close(sc.changed)
	sc.changed = make(chan struct{})
	sc.mu.Unlock()
}

// get returns a shape (nil if unknown) and a channel closed at the next put.
func (sc *shapeCache) get(table string, tv int64) (*shapeInfo, <-chan struct{}) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.m[shapeKey{strings.ToLower(table), tv}], sc.changed
}

// wait returns a shape, waiting a little for one not received yet.
func (sc *shapeCache) wait(ctx context.Context, table string, tv int64, d time.Duration) *shapeInfo {
	var timer *time.Timer
	for {
		si, ch := sc.get(table, tv)
		if si != nil {
			return si
		}
		if timer == nil {
			timer = time.NewTimer(d)
			defer timer.Stop()
		}
		select {
		case <-ch:
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}
