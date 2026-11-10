package azora

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

// Record is a row as a map from column name to value: the row type of
// DB.Live and a valid T for Live, All and One.
type Record = map[string]any

// LiveTable is a live local copy of the rows a subscription covers: a
// table, a filtered part of one, or a single row. It applies the
// server's changes in the background, one consistent step (a database
// transaction boundary) at a time, so readers always see a state the
// database really was in. It survives reconnects and resynchronizes by
// itself when the server can no longer resume it.
//
// Rows are decoded into T once, when they change: a struct (columns map
// to fields as in Rows.ScanStruct) or a Record. Every method is safe for
// concurrent use.
type LiveTable[T any] struct {
	sub  *Subscription
	plan *typePlan
	opts LiveOptions

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed when the apply loop ended
	ready  chan struct{} // closed when the first snapshot was applied

	mu      sync.RWMutex
	rows    map[string]*liveRow[T]
	order   []*liveRow[T] // rows in primary key order; nil when stale
	sum     uint64        // checksum of the rows
	bind    *binding
	cols    []string
	pk      []string
	cp      uint64
	epoch   int64
	version uint64
	isReady bool
	err     error

	notifyMu  sync.Mutex // orders "apply and notify" against a listener's first snapshot
	lmu       sync.Mutex
	listeners []*liveListener[T]

	imu       sync.Mutex
	interrupt context.CancelFunc // cancels the apply loop's wait for the next update
	verifies  []*verifyReq

	mismatches uint64 // guarded by mu
}

type liveRow[T any] struct {
	key  []any
	raw  []any
	val  T
	hash uint64
}

type liveListener[T any] struct {
	fn      func(LiveUpdate[T])
	stopped bool // guarded by LiveTable.lmu
}

// LiveOptions tune a LiveTable.
type LiveOptions struct {
	// VerifyEvery, when positive, compares the view's checksum with the
	// server's at this interval and resynchronizes (a new snapshot) on a
	// mismatch. A mismatch means a bug or corruption somewhere, never
	// normal operation: OnMismatch reports it.
	VerifyEvery time.Duration
	// OnMismatch runs when a checksum comparison failed, before the view
	// resynchronizes. It must not block.
	OnMismatch func(VerifyResult)
}

// LiveUpdate is one consistent step applied to a LiveTable.
type LiveUpdate[T any] struct {
	// Reset reports that the view was emptied first: Changes then holds
	// every row of the new snapshot, as inserts.
	Reset bool
	// Changes are the row changes of the step, in the order the database
	// made them.
	Changes []LiveChange[T]
	// Checkpoint and Epoch identify the state the view is in afterwards.
	Checkpoint uint64
	Epoch      int64
}

// LiveChange is one row change of a LiveUpdate.
type LiveChange[T any] struct {
	Op  Op
	Key []any
	// New is the row after an insert or update, Old the row before an
	// update or delete (the zero T otherwise). An update that moves a row
	// into or out of the subscription's filter arrives as an insert or a
	// delete.
	New, Old T
}

// Live subscribes to spec (a TableSpec, RowSpec or FilesSpec) and
// returns a live view of its rows decoded as T:
//
//	orders, err := azora.Live[Order](ctx, db, azora.TableSpec("orders", azora.Eq("status", "open")))
//	defer orders.Close()
//	if err := orders.Wait(ctx); err != nil { ... } // first snapshot
//	for _, o := range orders.Rows() { ... }
//
// It returns once the server accepted the subscription; the rows arrive
// in the background (Wait). The table must be published for realtime.
func Live[T any](ctx context.Context, db *DB, spec Spec, opts ...LiveOptions) (*LiveTable[T], error) {
	switch {
	case spec.Kind != KindTable && spec.Kind != KindRow && spec.Kind != KindFiles:
		return nil, fmt.Errorf("azora: Live needs a table, row or files subscription, not %s (use Subscribe with a Replica)", spec.Kind)
	case spec.Ops != 0 && spec.Ops != OpsAll:
		return nil, errors.New("azora: Live needs every operation (Spec.Ops would leave the view stale)")
	case spec.NoSnapshot || spec.Since != 0:
		return nil, errors.New("azora: Live starts from a snapshot (Spec.NoSnapshot and Spec.Since do not apply)")
	case spec.ManualResync:
		return nil, errors.New("azora: Live resynchronizes by itself (Spec.ManualResync does not apply)")
	}
	plan := planFor(reflect.TypeFor[T]())
	if plan.err != nil {
		return nil, plan.err
	}
	if plan.scalar != nil {
		return nil, fmt.Errorf("azora: Live rows cannot be decoded into %s: use a struct or azora.Record", plan.typ)
	}
	sub, err := db.Subscribe(ctx, spec)
	if err != nil {
		return nil, err
	}
	lt := &LiveTable[T]{sub: sub, plan: plan, rows: map[string]*liveRow[T]{}, done: make(chan struct{}), ready: make(chan struct{})}
	if len(opts) > 0 {
		lt.opts = opts[0]
	}
	lt.ctx, lt.cancel = context.WithCancel(db.ctx)
	if !db.goTracked(lt.run) {
		lt.cancel()
		sub.Close()
		return nil, ErrClosed
	}
	if lt.opts.VerifyEvery > 0 {
		db.goTracked(lt.autoVerify)
	}
	return lt, nil
}

// Live is the untyped form of the generic Live function: a live view
// whose rows are Records (maps from column name to value).
func (db *DB) Live(ctx context.Context, spec Spec, opts ...LiveOptions) (*LiveTable[Record], error) {
	return Live[Record](ctx, db, spec, opts...)
}

// Wait blocks until the first snapshot was applied (the view then holds
// the subscription's rows), the view failed, or ctx ended.
func (lt *LiveTable[T]) Wait(ctx context.Context) error {
	select {
	case <-lt.ready:
		return nil
	default:
	}
	select {
	case <-lt.ready:
		return nil
	case <-lt.done:
		return lt.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Ready reports whether the first snapshot was applied.
func (lt *LiveTable[T]) Ready() bool {
	select {
	case <-lt.ready:
		return true
	default:
		return false
	}
}

// Len returns the number of rows.
func (lt *LiveTable[T]) Len() int {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return len(lt.rows)
}

// Get returns the row with the given primary key values (in key order).
func (lt *LiveTable[T]) Get(key ...any) (row T, ok bool) {
	k := encodeKey(key)
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	if r := lt.rows[k]; r != nil {
		return r.val, true
	}
	return row, false
}

// Rows returns a copy of the rows, ordered by primary key (the way
// ORDER BY the key columns orders them). The copy is one consistent
// state: later changes do not touch it.
func (lt *LiveTable[T]) Rows() []T {
	rows := lt.ordered()
	out := make([]T, len(rows))
	for i, r := range rows {
		out[i] = r.val
	}
	return out
}

// All iterates over the rows in primary key order, with their keys, over
// one consistent state taken when the iteration starts.
func (lt *LiveTable[T]) All() iter.Seq2[[]any, T] {
	return func(yield func([]any, T) bool) {
		for _, r := range lt.ordered() {
			if !yield(r.key, r.val) {
				return
			}
		}
	}
}

// ordered returns the rows in key order. The slice is shared and
// immutable: appliers replace it, never modify it.
func (lt *LiveTable[T]) ordered() []*liveRow[T] {
	lt.mu.RLock()
	rows := lt.order
	stale := rows == nil && len(lt.rows) > 0
	lt.mu.RUnlock()
	if !stale {
		return rows
	}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if lt.order == nil {
		rows = make([]*liveRow[T], 0, len(lt.rows))
		for _, r := range lt.rows {
			rows = append(rows, r)
		}
		slices.SortFunc(rows, func(a, b *liveRow[T]) int { return compareKeys(a.key, b.key) })
		lt.order = rows
	}
	return lt.order
}

// Columns returns the table's column names and primary key columns, as
// of the last change applied (nil before the first row arrived).
func (lt *LiveTable[T]) Columns() (columns, pk []string) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.cols, lt.pk
}

// Checkpoint returns the checkpoint and epoch of the state the view is
// in, and Version a counter that grows with every step applied: compare
// versions to know cheaply whether anything changed.
func (lt *LiveTable[T]) Checkpoint() (checkpoint uint64, epoch int64) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.cp, lt.epoch
}

// Version returns the number of steps applied so far.
func (lt *LiveTable[T]) Version() uint64 {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.version
}

// Checksum returns the row count and the protocol checksum of the view's
// rows (the sum of RowHash over them): what Verify compares with the
// server.
func (lt *LiveTable[T]) Checksum() (rows int64, sum uint64) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return int64(len(lt.rows)), lt.sum
}

// Mismatches returns how many checksum comparisons failed (see
// LiveOptions.VerifyEvery and Verify). It stays 0 in normal operation.
func (lt *LiveTable[T]) Mismatches() uint64 {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.mismatches
}

// Err returns why the view stopped: ErrClosed after Close (or after the
// DB closed), a server error when the subscription ended (the table was
// dropped, the credential lost access), or a decoding error when a row
// does not fit T. It is nil while the view is live.
func (lt *LiveTable[T]) Err() error {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	return lt.err
}

// Done is closed when the view stopped (see Err).
func (lt *LiveTable[T]) Done() <-chan struct{} { return lt.done }

// Resync drops the view's resume point and takes a new snapshot; the
// rows are replaced in one step when it arrives.
func (lt *LiveTable[T]) Resync() { lt.sub.Resync() }

// Close stops the view and its subscription. The rows stay readable.
func (lt *LiveTable[T]) Close() error {
	lt.cancel()
	<-lt.done
	return lt.sub.Close()
}

// compareKeys orders primary keys the way SQLite orders the key columns:
// column by column, NULL < numbers < text < blobs.
func compareKeys(a, b []any) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := compareValues(a[i], b[i]); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

// valueClass ranks a value's storage class.
func valueClass(v any) int {
	switch v.(type) {
	case nil:
		return 0
	case int64, float64, bool:
		return 1
	case string:
		return 2
	}
	return 3
}

func compareValues(a, b any) int {
	ca, cb := valueClass(a), valueClass(b)
	if ca != cb {
		return ca - cb
	}
	switch ca {
	case 1:
		return compareNumbers(a, b)
	case 2:
		return strings.Compare(a.(string), b.(string))
	case 3:
		ab, _ := a.([]byte)
		bb, _ := b.([]byte)
		return slices.Compare(ab, bb)
	}
	return 0
}

func compareNumbers(a, b any) int {
	ai, aInt := a.(int64)
	bi, bInt := b.(int64)
	if aInt && bInt {
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		}
		return 0
	}
	af, bf := toFloat(a), toFloat(b)
	switch {
	case af < bf:
		return -1
	case af > bf:
		return 1
	}
	return 0
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case float64:
		return x
	case bool:
		if x {
			return 1
		}
	}
	return 0
}
