package azp

// Call runs a published unit. Args is the JSON text of the unit's
// arguments ("" means none). A non-empty MutationID makes a mutation or
// action unit idempotent: a retry with the same id returns the first
// result without running it again.
type Call struct {
	ReqID      uint32
	Unit       string
	Args       string
	MutationID string
}

func (*Call) Type() MsgType { return TypeCall }
func (m *Call) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.s(m.Unit)
	e.s(m.Args)
	e.s(m.MutationID)
}
func (m *Call) decode(d *decoder) {
	m.ReqID, m.Unit, m.Args, m.MutationID = d.u32(), d.s(), d.s(), d.s()
}

// Returned answers Call. OK reports that the unit returned: Value is the
// JSON text of its result. Otherwise the unit threw: ErrCode, ErrMessage
// and ErrData (JSON text, "" for none) describe the error it threw.
// Failures outside the unit (no such unit, bad arguments, timeouts) are
// answered with Error instead.
type Returned struct {
	ReqID      uint32
	OK         bool
	Value      string
	Replayed   bool
	ErrCode    string
	ErrMessage string
	ErrData    string
}

func (*Returned) Type() MsgType { return TypeReturned }
func (m *Returned) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.flag(m.OK)
	e.s(m.Value)
	e.flag(m.Replayed)
	e.s(m.ErrCode)
	e.s(m.ErrMessage)
	e.s(m.ErrData)
}
func (m *Returned) decode(d *decoder) {
	m.ReqID, m.OK, m.Value, m.Replayed = d.u32(), d.flag(), d.s(), d.flag()
	m.ErrCode, m.ErrMessage, m.ErrData = d.s(), d.s(), d.s()
}

// UnitAdmin manages a database's units (scope ddl): Op names the
// operation and Body is its JSON request (see docs/units.md).
type UnitAdmin struct {
	ReqID uint32
	Op    string
	Body  string
}

func (*UnitAdmin) Type() MsgType { return TypeUnitAdmin }
func (m *UnitAdmin) encode(e *encoder) {
	e.u(uint64(m.ReqID))
	e.s(m.Op)
	e.s(m.Body)
}
func (m *UnitAdmin) decode(d *decoder) { m.ReqID, m.Op, m.Body = d.u32(), d.s(), d.s() }

// UnitAdminResult answers UnitAdmin with the JSON text of its result.
type UnitAdminResult struct {
	ReqID uint32
	Body  string
}

func (*UnitAdminResult) Type() MsgType       { return TypeUnitAdminResult }
func (m *UnitAdminResult) encode(e *encoder) { e.u(uint64(m.ReqID)); e.s(m.Body) }
func (m *UnitAdminResult) decode(d *decoder) { m.ReqID, m.Body = d.u32(), d.s() }
