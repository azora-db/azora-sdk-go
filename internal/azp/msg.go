package azp

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/azora-db/azora-go/internal/codec"
)

// ProtocolVersion is the message-layer version sent in Hello.
const ProtocolVersion = 1

// MsgType identifies a message. The first byte of every application
// message carried by a Conn is its type.
type MsgType uint8

// Message types (see docs/protocol.md).
const (
	TypeHello       MsgType = 1  // client -> server, first message
	TypeWelcome     MsgType = 2  // server -> client
	TypeError       MsgType = 3  // server -> client
	TypePing        MsgType = 4  // both
	TypePong        MsgType = 5  // both
	TypeAuth        MsgType = 6  // client -> server: replace the credential
	TypeSubscribe   MsgType = 7  // client -> server
	TypeSubscribed  MsgType = 8  // server -> client
	TypeUnsubscribe MsgType = 9  // client -> server
	TypeBatch       MsgType = 10 // server -> client
	TypeSubEnd      MsgType = 11 // server -> client: a subscription ended
	TypeAck         MsgType = 12 // client -> server: checkpoint applied
	TypeQuery       MsgType = 13 // client -> server
	TypeExec        MsgType = 14 // client -> server
	TypeTx          MsgType = 15 // client -> server
	TypeResult      MsgType = 16 // server -> client
	TypeVerify      MsgType = 17 // client -> server
	TypeVerified    MsgType = 18 // server -> client
	TypeSign        MsgType = 19 // client -> server
	TypeSigned      MsgType = 20 // server -> client
	TypeGoAway      MsgType = 21 // server -> client
	TypeShape       MsgType = 22 // server -> client: column layout of a table version
	TypeOK          MsgType = 23 // server -> client: request done, no payload

	TypeCall            MsgType = 24 // client -> server: run a unit
	TypeReturned        MsgType = 25 // server -> client: a unit's result
	TypeUnitAdmin       MsgType = 26 // client -> server: manage units
	TypeUnitAdminResult MsgType = 27 // server -> client
)

// TypeLast is the highest message type.
const TypeLast = TypeUnitAdminResult

// Msg is a protocol message.
type Msg interface {
	Type() MsgType
	encode(e *encoder)
	decode(d *decoder)
}

// Limits applied while decoding, so a hostile peer cannot make the decoder
// allocate more than the message it sent.
const (
	maxFilterDepth = 16
	maxFilterNodes = 256
)

// ErrMalformed means a message failed to decode.
var ErrMalformed = errors.New("azp: malformed message")

// Encode serializes m.
func Encode(m Msg) ([]byte, error) {
	e := &encoder{b: make([]byte, 0, 64)}
	e.b = append(e.b, byte(m.Type()))
	m.encode(e)
	if e.err != nil {
		return nil, e.err
	}
	return e.b, nil
}

// Decode parses one message.
func Decode(b []byte) (Msg, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("%w: empty", ErrMalformed)
	}
	m := newMsg(MsgType(b[0]))
	if m == nil {
		return nil, fmt.Errorf("%w: unknown type %d", ErrMalformed, b[0])
	}
	d := &decoder{d: codec.NewDecoder(b[1:])}
	m.decode(d)
	if d.err == nil {
		d.err = d.d.Err()
	}
	if d.err == nil && d.d.Remaining() != 0 {
		d.err = fmt.Errorf("%d trailing bytes", d.d.Remaining())
	}
	if d.err != nil {
		return nil, fmt.Errorf("%w: type %d: %v", ErrMalformed, b[0], d.err)
	}
	return m, nil
}

// newMsg returns an empty message of type t, or nil for an unknown type.
func newMsg(t MsgType) Msg {
	switch t {
	case TypeHello:
		return &Hello{}
	case TypeWelcome:
		return &Welcome{}
	case TypeError:
		return &Error{}
	case TypePing:
		return &Ping{}
	case TypePong:
		return &Pong{}
	case TypeAuth:
		return &Auth{}
	case TypeSubscribe:
		return &Subscribe{}
	case TypeSubscribed:
		return &Subscribed{}
	case TypeUnsubscribe:
		return &Unsubscribe{}
	case TypeBatch:
		return &Batch{}
	case TypeSubEnd:
		return &SubEnd{}
	case TypeAck:
		return &Ack{}
	case TypeQuery:
		return &Query{}
	case TypeExec:
		return &Exec{}
	case TypeTx:
		return &Tx{}
	case TypeResult:
		return &Result{}
	case TypeVerify:
		return &Verify{}
	case TypeVerified:
		return &Verified{}
	case TypeSign:
		return &Sign{}
	case TypeSigned:
		return &Signed{}
	case TypeGoAway:
		return &GoAway{}
	case TypeShape:
		return &Shape{}
	case TypeOK:
		return &OK{}
	case TypeCall:
		return &Call{}
	case TypeReturned:
		return &Returned{}
	case TypeUnitAdmin:
		return &UnitAdmin{}
	case TypeUnitAdminResult:
		return &UnitAdminResult{}
	}
	return nil
}

// RowHash hashes one row for checksums: SHA-256 over the lowercased table
// name, then the key values and the row values (each list prefixed with
// its length as a uvarint, values in internal/codec encoding); the first 8
// bytes of the digest, little-endian. A replica's checksum is the sum of
// RowHash over its rows, mod 2^64.
func RowHash(table string, key, row []any) uint64 {
	b := codec.AppendString(nil, strings.ToLower(table))
	b = binary.AppendUvarint(b, uint64(len(key)))
	for _, v := range key {
		b, _ = codec.AppendValue(b, v)
	}
	b = binary.AppendUvarint(b, uint64(len(row)))
	for _, v := range row {
		b, _ = codec.AppendValue(b, v)
	}
	sum := sha256.Sum256(b)
	return binary.LittleEndian.Uint64(sum[:8])
}
