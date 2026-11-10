package azora

import (
	"fmt"

	"github.com/azora-db/azora-go/internal/azp"
)

// Op is a change operation.
type Op uint8

// Change operations.
const (
	OpInsert Op = Op(azp.OpInsert)
	OpUpdate Op = Op(azp.OpUpdate)
	OpDelete Op = Op(azp.OpDelete)
)

// String returns "insert", "update" or "delete".
func (o Op) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	}
	return fmt.Sprintf("Op(%d)", uint8(o))
}

// Kind selects what a subscription covers.
type Kind uint8

// Subscription kinds.
const (
	// KindDatabase covers every published table of the database.
	KindDatabase Kind = Kind(azp.KindDatabase)
	// KindTable covers one table, optionally filtered (Spec.Where).
	KindTable Kind = Kind(azp.KindTable)
	// KindRow covers one row of a table, by primary key (Spec.Key).
	KindRow Kind = Kind(azp.KindRow)
	// KindFiles covers stored objects (Spec.Bucket, PathPrefix, or one
	// object id in Spec.Key); the files publication must be enabled.
	KindFiles Kind = Kind(azp.KindFiles)
	// KindNotify delivers "these tables changed" signals for any table,
	// published or not (Update.Tables). It has no rows, no snapshot and no
	// resume: after a reconnect it delivers Tables ["*"] (anything may
	// have changed).
	KindNotify Kind = Kind(azp.KindNotify)
)

// String returns the kind's name.
func (k Kind) String() string {
	switch k {
	case KindDatabase:
		return "database"
	case KindTable:
		return "table"
	case KindRow:
		return "row"
	case KindFiles:
		return "files"
	case KindNotify:
		return "notify"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Ops selects operations; 0 means all.
type Ops uint8

// Operation masks.
const (
	OpsInsert Ops = 1
	OpsUpdate Ops = 2
	OpsDelete Ops = 4
	OpsAll        = OpsInsert | OpsUpdate | OpsDelete
)

// Change is one row change. Values are nil, int64, float64, string or
// []byte. Key holds the primary key values; New the row after an insert
// or update, Old the row before an update or delete (both positional,
// matching Columns). Snapshot rows are inserts with Seq 0. Columns and PK
// are shared between changes: do not modify them.
type Change struct {
	Seq     uint64
	Table   string
	Op      Op
	Key     []any
	New     []any
	Old     []any
	Columns []string
	PK      []string
	tv      int64
}

// Update is what Subscription.Next delivers: a consistent step. Apply
// Changes in order, after dropping your copy of the subscription's rows
// when Reset is set; you are then at Checkpoint (store Epoch and
// Checkpoint to resume in a later process with Spec.Since).
type Update struct {
	// Reset: drop every row of this subscription before applying Changes
	// (a new snapshot follows a lost resume point).
	Reset bool
	// Snapshot: Changes are the subscription's current rows (inserts).
	Snapshot bool
	Changes  []Change
	// Checkpoint and Epoch identify the consistent point reached.
	Checkpoint uint64
	Epoch      int64
	// Tables lists changed tables (notify subscriptions); "*" means any
	// table may have changed (missed signals, for example after a
	// reconnect).
	Tables []string
}

// RowHash hashes one row the way the server's checksums do: SHA-256 over
// the lowercased table name, the key values and the row values; the first
// 8 bytes of the digest, little-endian. A copy's checksum is the sum of
// RowHash over its rows, mod 2^64 (see Subscription.Verify).
func RowHash(table string, key, row []any) uint64 { return azp.RowHash(table, key, row) }
