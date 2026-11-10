package azora

// Spec describes a subscription. Build one with TableSpec, RowSpec,
// DatabaseSpec, FilesSpec or NotifySpec and refine it with the chaining
// methods, or fill the struct directly.
type Spec struct {
	Kind Kind
	// Table for table and row subscriptions.
	Table string
	// Key: a row subscription's primary key values (in key order); a
	// files subscription's object id.
	Key []any
	// Where filters a table subscription.
	Where *Filter
	// Ops limits operations (0 = all).
	Ops Ops
	// Since and Epoch resume from a checkpoint the application stored
	// (Update.Checkpoint and Update.Epoch); without Since the subscription
	// starts with a snapshot of the matching rows (unless NoSnapshot).
	Since uint64
	Epoch int64
	// NoSnapshot skips the initial snapshot: only changes from now on.
	// Until its first checkpoint arrives, a reconnect may miss changes.
	NoSnapshot bool
	// Bucket and PathPrefix narrow a files subscription.
	Bucket     string
	PathPrefix string
	// Tables narrows a notify subscription (empty = every table).
	Tables []string
	// ManualResync makes Next return ErrResync when the resume point is
	// gone, instead of taking a new snapshot automatically.
	ManualResync bool
}

// TableSpec subscribes to one published table, optionally narrowed by
// filters (several are combined with AND):
//
//	azora.TableSpec("orders", azora.Eq("status", "paid"), azora.Gt("total", 100))
func TableSpec(table string, where ...Filter) Spec {
	s := Spec{Kind: KindTable, Table: table}
	for _, f := range where {
		s = s.Filter(f)
	}
	return s
}

// RowSpec subscribes to one row of a published table by its primary key
// values, in key order.
func RowSpec(table string, key ...any) Spec {
	return Spec{Kind: KindRow, Table: table, Key: key}
}

// DatabaseSpec subscribes to every published table of the database.
func DatabaseSpec() Spec { return Spec{Kind: KindDatabase} }

// FilesSpec subscribes to the stored files of the database (the files
// publication must be enabled: `azora realtime publish <db> files`).
// Narrow it with InBucket, WithPathPrefix or Object.
func FilesSpec() Spec { return Spec{Kind: KindFiles} }

// NotifySpec subscribes to "these tables changed" signals for the given
// tables (none: every table). The tables need not be published; the
// updates carry no rows. See also DB.Watch.
func NotifySpec(tables ...string) Spec { return Spec{Kind: KindNotify, Tables: tables} }

// Filter narrows a table subscription to rows f matches. Called again, it
// combines the filters with AND.
func (s Spec) Filter(f Filter) Spec {
	if s.Where == nil {
		s.Where = &f
		return s
	}
	and := And(*s.Where, f)
	if s.Where.Op == "and" {
		and = And(append(append([]Filter(nil), s.Where.Args...), f)...)
	}
	s.Where = &and
	return s
}

// On limits the subscription to some operations, for example
// On(azora.OpsInsert|azora.OpsDelete).
func (s Spec) On(ops Ops) Spec { s.Ops = ops; return s }

// ResumeFrom resumes from a checkpoint and epoch stored from an earlier
// Update (or Subscription.Checkpoint) instead of taking a snapshot.
func (s Spec) ResumeFrom(checkpoint uint64, epoch int64) Spec {
	s.Since, s.Epoch = checkpoint, epoch
	return s
}

// WithoutSnapshot skips the initial snapshot: only changes from now on.
func (s Spec) WithoutSnapshot() Spec { s.NoSnapshot = true; return s }

// InBucket narrows a files subscription to one bucket.
func (s Spec) InBucket(bucket string) Spec { s.Bucket = bucket; return s }

// WithPathPrefix narrows a files subscription to paths starting with p.
func (s Spec) WithPathPrefix(p string) Spec { s.PathPrefix = p; return s }

// Object narrows a files subscription to one object id.
func (s Spec) Object(id string) Spec { s.Key = []any{id}; return s }

// Manual makes Subscription.Next return ErrResync when the resume point
// is gone, instead of taking a new snapshot automatically.
func (s Spec) Manual() Spec { s.ManualResync = true; return s }
