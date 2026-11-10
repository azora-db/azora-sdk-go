package azp

// Query runs a read.
type Query struct {
	ReqID   uint32
	SQL     string
	Args    []any
	MaxRows uint32
}

func (*Query) Type() MsgType { return TypeQuery }
func (m *Query) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.s(m.SQL)
	e.vals(m.Args)
	e.u(uint64(m.MaxRows))
}
func (m *Query) decode(d *decoder) {
	m.ReqID, m.SQL, m.Args, m.MaxRows = d.u32(), d.s(), d.vals(), d.u32()
}

// Exec runs a write. A non-empty MutationID makes it idempotent: a retry
// with the same id returns the first result without running again.
type Exec struct {
	ReqID      uint32
	MutationID string
	SQL        string
	Args       []any
}

func (*Exec) Type() MsgType { return TypeExec }
func (m *Exec) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.s(m.MutationID)
	e.s(m.SQL)
	e.vals(m.Args)
}
func (m *Exec) decode(d *decoder) {
	m.ReqID, m.MutationID, m.SQL, m.Args = d.u32(), d.s(), d.s(), d.vals()
}

// Statement is one statement of a Tx.
type Statement struct {
	SQL  string
	Args []any
}

// Tx runs statements in one write transaction (all or nothing).
type Tx struct {
	ReqID      uint32
	MutationID string
	Statements []Statement
}

func (*Tx) Type() MsgType { return TypeTx }
func (m *Tx) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.s(m.MutationID)
	e.u(uint64(len(m.Statements)))
	for _, s := range m.Statements {
		e.s(s.SQL)
		e.vals(s.Args)
	}
}
func (m *Tx) decode(d *decoder) {
	m.ReqID, m.MutationID = d.u32(), d.s()
	n := d.count()
	for i := 0; i < n && d.err == nil && d.d.Err() == nil; i++ {
		m.Statements = append(m.Statements, Statement{SQL: d.s(), Args: d.vals()})
	}
}

// Result answers Query, Exec and Tx.
type Result struct {
	ReqID        uint32
	Columns      []string
	Rows         [][]any
	Truncated    bool
	RowsAffected int64
	LastInsertID int64
	// Replayed reports an idempotent write answered from its first run.
	Replayed bool
}

func (*Result) Type() MsgType { return TypeResult }
func (m *Result) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.strs(m.Columns)
	e.u(uint64(len(m.Rows)))
	for _, r := range m.Rows {
		e.vals(r)
	}
	e.flag(m.Truncated)
	e.i(m.RowsAffected)
	e.i(m.LastInsertID)
	e.flag(m.Replayed)
}
func (m *Result) decode(d *decoder) {
	m.ReqID, m.Columns = d.u32(), d.strs()
	n := d.count()
	if n > 0 {
		m.Rows = make([][]any, 0, n)
	}
	for i := 0; i < n && d.err == nil && d.d.Err() == nil; i++ {
		r := d.vals()
		if r == nil {
			r = []any{}
		}
		m.Rows = append(m.Rows, r)
	}
	m.Truncated, m.RowsAffected, m.LastInsertID, m.Replayed = d.flag(), d.i(), d.i(), d.flag()
}

// Sign asks for a signed download URL of a stored object.
type Sign struct {
	ReqID      uint32
	ObjectID   string
	TTLSeconds uint32
}

func (*Sign) Type() MsgType       { return TypeSign }
func (m *Sign) encode(e *encoder) { e.u(uint64(m.ReqID)); e.s(m.ObjectID); e.u(uint64(m.TTLSeconds)) }
func (m *Sign) decode(d *decoder) { m.ReqID, m.ObjectID, m.TTLSeconds = d.u32(), d.s(), d.u32() }

// Signed answers Sign.
type Signed struct {
	ReqID       uint32
	URL         string
	ExpiresAtMS int64
}

func (*Signed) Type() MsgType       { return TypeSigned }
func (m *Signed) encode(e *encoder) { e.u(uint64(m.ReqID)); e.s(m.URL); e.i(m.ExpiresAtMS) }
func (m *Signed) decode(d *decoder) { m.ReqID, m.URL, m.ExpiresAtMS = d.u32(), d.s(), d.i() }
