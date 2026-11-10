package azora

import (
	"iter"
	"sort"
	"strings"
	"sync"

	"github.com/azora-db/azora-go/internal/azp"
	"github.com/azora-db/azora-go/internal/codec"
)

// Replica is an in-memory copy of a subscription's rows, built by applying
// its updates. Tables are keyed by lowercased name, rows by primary key.
// It is safe for concurrent use.
type Replica struct {
	mu     sync.RWMutex
	tables map[string]*replicaTable
}

type replicaTable struct {
	name    string
	columns []string
	pk      []string
	rows    map[string]replicaRow
}

type replicaRow struct {
	key, row []any
	hash     uint64
}

// NewReplica returns an empty replica.
func NewReplica() *Replica { return &Replica{tables: map[string]*replicaTable{}} }

// Apply applies an update: Reset drops every row first; inserts and
// updates store the new row, deletes remove it.
func (r *Replica) Apply(u Update) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if u.Reset {
		r.tables = map[string]*replicaTable{}
	}
	for i := range u.Changes {
		c := &u.Changes[i]
		k := strings.ToLower(c.Table)
		t := r.tables[k]
		if t == nil {
			t = &replicaTable{name: c.Table, rows: map[string]replicaRow{}}
			r.tables[k] = t
		}
		if c.Columns != nil {
			t.columns, t.pk = c.Columns, c.PK
		}
		key := encodeKey(c.Key)
		switch c.Op {
		case OpInsert, OpUpdate:
			if c.New == nil {
				continue
			}
			t.rows[key] = replicaRow{key: c.Key, row: c.New, hash: azp.RowHash(c.Table, c.Key, c.New)}
		case OpDelete:
			delete(t.rows, key)
		}
	}
}

// encodeKey makes a map key of primary key values (normalized, so int and
// int64 keys are the same row).
func encodeKey(key []any) string {
	var b []byte
	for _, v := range key {
		n, err := Normalize(v)
		if err != nil {
			n = nil
		}
		b, err = codec.AppendValue(b, n)
		if err != nil {
			b = append(b, 0xff)
		}
	}
	return string(b)
}

// Get returns the row of table with the given primary key values.
func (r *Replica) Get(table string, key ...any) ([]any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t := r.tables[strings.ToLower(table)]
	if t == nil {
		return nil, false
	}
	row, ok := t.rows[encodeKey(key)]
	return row.row, ok
}

// Rows iterates over a table's rows as (key, row) pairs, in no particular
// order, over a snapshot taken when iteration starts.
func (r *Replica) Rows(table string) iter.Seq2[[]any, []any] {
	return func(yield func([]any, []any) bool) {
		r.mu.RLock()
		t := r.tables[strings.ToLower(table)]
		var rows []replicaRow
		if t != nil {
			rows = make([]replicaRow, 0, len(t.rows))
			for _, x := range t.rows {
				rows = append(rows, x)
			}
		}
		r.mu.RUnlock()
		for _, x := range rows {
			if !yield(x.key, x.row) {
				return
			}
		}
	}
}

// Len returns the number of rows of table.
func (r *Replica) Len(table string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if t := r.tables[strings.ToLower(table)]; t != nil {
		return len(t.rows)
	}
	return 0
}

// Tables returns the lowercased names of the tables with rows, sorted.
func (r *Replica) Tables() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for k, t := range r.tables {
		if len(t.rows) > 0 {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// Columns returns the column names of table's rows (from the last change
// applied), and its primary key columns.
func (r *Replica) Columns(table string) (columns, pk []string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if t := r.tables[strings.ToLower(table)]; t != nil {
		return t.columns, t.pk
	}
	return nil, nil
}

// Checksum returns the row count and the sum (mod 2^64) of the protocol's
// row hash over every row: what Subscription.Verify compares with the
// server.
func (r *Replica) Checksum() (rows int64, sum uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.tables {
		for _, x := range t.rows {
			rows++
			sum += x.hash
		}
	}
	return rows, sum
}
