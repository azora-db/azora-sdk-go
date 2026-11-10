package azp

// Filter is the wire form of a subscription filter (see realtime.Filter).
type Filter struct {
	Op   string
	Col  string
	Val  any
	Vals []any
	Args []Filter
}

// filter writes f as a presence flag then op, col, val, vals and args.
func (e *encoder) filter(f *Filter, depth int) {
	if f == nil {
		e.flag(false)
		return
	}
	e.flag(true)
	e.s(f.Op)
	e.s(f.Col)
	e.val(f.Val)
	e.vals(f.Vals)
	e.u(uint64(len(f.Args)))
	for i := range f.Args {
		e.filter(&f.Args[i], depth+1)
	}
}

// filter reads a filter written by encoder.filter, refusing trees deeper
// than maxFilterDepth or larger than maxFilterNodes.
func (d *decoder) filter(depth int, nodes *int) *Filter {
	if !d.flag() {
		return nil
	}
	*nodes++
	if depth > maxFilterDepth || *nodes > maxFilterNodes {
		d.fail("filter too large")
		return nil
	}
	f := &Filter{Op: d.s(), Col: d.s(), Val: d.d.Value(), Vals: d.vals()}
	n := d.count()
	for i := 0; i < n && d.err == nil && d.d.Err() == nil; i++ {
		a := d.filter(depth+1, nodes)
		if a == nil {
			d.fail("missing filter argument")
			return nil
		}
		f.Args = append(f.Args, *a)
	}
	return f
}

// Subscription kinds on the wire (same values as the engine's).
const (
	KindDatabase uint8 = 1
	KindTable    uint8 = 2
	KindRow      uint8 = 3
	KindFiles    uint8 = 4
	KindNotify   uint8 = 5
)

// Subscribe starts a subscription; SubID is chosen by the client and
// names it in later messages.
type Subscribe struct {
	ReqID      uint32
	SubID      uint32
	Kind       uint8
	Table      string
	Key        []any
	Where      *Filter
	Ops        uint8
	Since      uint64
	Epoch      int64
	NoSnapshot bool
	Bucket     string
	PathPrefix string
	Tables     []string
}

func (*Subscribe) Type() MsgType { return TypeSubscribe }
func (m *Subscribe) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.u(uint64(m.SubID))
	e.u(uint64(m.Kind))
	e.s(m.Table)
	e.vals(m.Key)
	e.filter(m.Where, 0)
	e.u(uint64(m.Ops))
	e.u(m.Since)
	e.i(m.Epoch)
	e.flag(m.NoSnapshot)
	e.s(m.Bucket)
	e.s(m.PathPrefix)
	e.strs(m.Tables)
}
func (m *Subscribe) decode(d *decoder) {
	m.ReqID, m.SubID = d.u32(), d.u32()
	k := d.u()
	if k > 255 {
		d.fail("kind %d", k)
	}
	m.Kind, m.Table, m.Key = uint8(k), d.s(), d.vals()
	nodes := 0
	m.Where = d.filter(0, &nodes)
	o := d.u()
	if o > 255 {
		d.fail("ops %d", o)
	}
	m.Ops, m.Since, m.Epoch, m.NoSnapshot = uint8(o), d.u(), d.i(), d.flag()
	m.Bucket, m.PathPrefix, m.Tables = d.s(), d.s(), d.strs()
}

// Subscribed confirms a subscription.
type Subscribed struct {
	ReqID uint32
	SubID uint32
}

func (*Subscribed) Type() MsgType       { return TypeSubscribed }
func (m *Subscribed) encode(e *encoder) { e.u(uint64(m.ReqID)); e.u(uint64(m.SubID)) }
func (m *Subscribed) decode(d *decoder) { m.ReqID, m.SubID = d.u32(), d.u32() }

// Unsubscribe ends a subscription.
type Unsubscribe struct {
	ReqID uint32
	SubID uint32
}

func (*Unsubscribe) Type() MsgType       { return TypeUnsubscribe }
func (m *Unsubscribe) encode(e *encoder) { e.u(uint64(m.ReqID)); e.u(uint64(m.SubID)) }
func (m *Unsubscribe) decode(d *decoder) { m.ReqID, m.SubID = d.u32(), d.u32() }

// Shape is the column layout of a table version (TV); changes and snapshot
// rows name their table version and are positional.
type Shape struct {
	Table   string
	TV      int64
	Columns []string
	PK      []string
}

func (*Shape) Type() MsgType { return TypeShape }
func (m *Shape) encode(e *encoder) {
	e.s(m.Table)
	e.i(m.TV)
	e.strs(m.Columns)
	e.strs(m.PK)
}
func (m *Shape) decode(d *decoder) { m.Table, m.TV, m.Columns, m.PK = d.s(), d.i(), d.strs(), d.strs() }

// Change operations on the wire.
const (
	OpInsert uint8 = 1
	OpUpdate uint8 = 2
	OpDelete uint8 = 3
)

// Change is one row change (or snapshot row, Seq 0).
type Change struct {
	Seq   uint64
	Table string
	TV    int64
	Op    uint8
	Key   []any
	New   []any
	Old   []any
}

// Batch carries a subscription's changes. Apply them when Checkpoint is
// set; store (Epoch, Checkpoint) to resume.
type Batch struct {
	SubID      uint32
	Snapshot   bool
	Reset      bool
	Checkpoint uint64
	Epoch      int64
	Tables     []string
	Changes    []Change
}

func (*Batch) Type() MsgType { return TypeBatch }
func (m *Batch) encode(e *encoder) {
	e.u(uint64(m.SubID))
	e.flag(m.Snapshot)
	e.flag(m.Reset)
	e.u(m.Checkpoint)
	e.i(m.Epoch)
	e.strs(m.Tables)
	e.u(uint64(len(m.Changes)))
	for _, c := range m.Changes {
		e.u(c.Seq)
		e.s(c.Table)
		e.i(c.TV)
		e.u(uint64(c.Op))
		e.vals(c.Key)
		e.vals(c.New)
		e.vals(c.Old)
	}
}
func (m *Batch) decode(d *decoder) {
	m.SubID, m.Snapshot, m.Reset, m.Checkpoint, m.Epoch, m.Tables = d.u32(), d.flag(), d.flag(), d.u(), d.i(), d.strs()
	n := d.count()
	if n > 0 {
		m.Changes = make([]Change, 0, n)
	}
	for i := 0; i < n && d.err == nil && d.d.Err() == nil; i++ {
		c := Change{Seq: d.u(), Table: d.s(), TV: d.i()}
		op := d.u()
		if op > 255 {
			d.fail("op %d", op)
		}
		c.Op, c.Key, c.New, c.Old = uint8(op), d.vals(), d.vals(), d.vals()
		m.Changes = append(m.Changes, c)
	}
}

// SubEnd reports that a subscription ended: Code "gone" means resubscribe
// without Since (take a new snapshot); "unavailable" means resume from the
// last checkpoint (the database was closed or reopened).
type SubEnd struct {
	SubID   uint32
	Code    string
	Message string
}

func (*SubEnd) Type() MsgType       { return TypeSubEnd }
func (m *SubEnd) encode(e *encoder) { e.u(uint64(m.SubID)); e.s(m.Code); e.s(m.Message) }
func (m *SubEnd) decode(d *decoder) { m.SubID, m.Code, m.Message = d.u32(), d.s(), d.s() }

// Ack confirms the client applied a subscription up to Checkpoint (it
// opens the flow-control window).
type Ack struct {
	SubID      uint32
	Checkpoint uint64
}

func (*Ack) Type() MsgType       { return TypeAck }
func (m *Ack) encode(e *encoder) { e.u(uint64(m.SubID)); e.u(m.Checkpoint) }
func (m *Ack) decode(d *decoder) { m.SubID, m.Checkpoint = d.u32(), d.u() }

// Verify asks for the checksum of a subscription's rows at the current
// checkpoint.
type Verify struct {
	ReqID uint32
	SubID uint32
}

func (*Verify) Type() MsgType       { return TypeVerify }
func (m *Verify) encode(e *encoder) { e.u(uint64(m.ReqID)); e.u(uint64(m.SubID)) }
func (m *Verify) decode(d *decoder) { m.ReqID, m.SubID = d.u32(), d.u32() }

// Verified answers Verify: Sum is the sum (mod 2^64) of RowHash over the
// subscription's rows at Checkpoint.
type Verified struct {
	ReqID      uint32
	SubID      uint32
	Epoch      int64
	Checkpoint uint64
	Rows       int64
	Sum        uint64
}

func (*Verified) Type() MsgType { return TypeVerified }
func (m *Verified) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.u(uint64(m.SubID))
	e.i(m.Epoch)
	e.u(m.Checkpoint)
	e.i(m.Rows)
	e.u(m.Sum)
}
func (m *Verified) decode(d *decoder) {
	m.ReqID, m.SubID, m.Epoch, m.Checkpoint, m.Rows, m.Sum = d.u32(), d.u32(), d.i(), d.u(), d.i(), d.u()
}
