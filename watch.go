package azora

import (
	"context"
	"iter"
	"strings"
)

// Watcher delivers "these tables changed" notices: a cheap way to know
// when to run a query again. It needs no publication (any table can be
// watched), carries no rows and costs the server almost nothing. Make
// one with DB.Watch.
//
// Notices are signals, not a log: after a reconnect, or when the
// consumer fell far behind, a single notice with Everything set stands
// for whatever was missed.
type Watcher struct {
	sub *Subscription
}

// Notice says which tables were written.
type Notice struct {
	// Tables are the names of the tables a transaction wrote.
	Tables []string
	// Everything reports that any table may have changed: notices were
	// missed (a reconnect, a consumer that fell behind).
	Everything bool
}

// Has reports whether table may have changed: it is named (ignoring
// case) or Everything is set.
func (n Notice) Has(table string) bool {
	if n.Everything {
		return true
	}
	for _, t := range n.Tables {
		if strings.EqualFold(t, table) {
			return true
		}
	}
	return false
}

// Watch subscribes to change notices for the given tables (none: every
// table of the database):
//
//	w, err := db.Watch(ctx, "orders", "order_items")
//	defer w.Close()
//	for n, err := range w.Notices(ctx) {
//		if err != nil { break }
//		refresh() // run the query again
//	}
func (db *DB) Watch(ctx context.Context, tables ...string) (*Watcher, error) {
	sub, err := db.Subscribe(ctx, NotifySpec(tables...))
	if err != nil {
		return nil, err
	}
	return &Watcher{sub: sub}, nil
}

// Next waits for the next notice. One goroutine at a time.
func (w *Watcher) Next(ctx context.Context) (Notice, error) {
	u, err := w.sub.Next(ctx)
	if err != nil {
		return Notice{}, err
	}
	n := Notice{Tables: u.Tables}
	for _, t := range u.Tables {
		if t == "*" {
			n.Everything = true
		}
	}
	return n, nil
}

// Notices iterates over the notices until the loop breaks; it ends with
// a non-nil error when ctx ends or the watcher is closed (ErrClosed).
func (w *Watcher) Notices(ctx context.Context) iter.Seq2[Notice, error] {
	return func(yield func(Notice, error) bool) {
		for {
			n, err := w.Next(ctx)
			if !yield(n, err) || err != nil {
				return
			}
		}
	}
}

// Close ends the watcher. Next returns ErrClosed afterwards.
func (w *Watcher) Close() error { return w.sub.Close() }
