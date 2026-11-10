package azora

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrFilterSyntax is wrapped by the errors ParseFilter returns.
var ErrFilterSyntax = errors.New("azora: invalid filter")

// ParseFilter reads the text form of a filter, the same grammar the
// server uses for `azora realtime tail --where` and for the filters pinned
// in client tokens:
//
//	status = 'paid' AND (total >= 100 OR vip IS NOT NULL) AND id IN (1, 2, 3)
//	NOT (path ^= 'tmp/')     -- ^= is "starts with"
//
// Operators are = (or ==), != (or <>), <, <=, >, >=, ^=, [NOT] IN (...),
// IS [NOT] NULL, AND, OR, NOT and parentheses; AND binds tighter than OR.
// Values are integers, floats, 'text' (quotes doubled inside), x'hex'
// blobs, TRUE/FALSE (1/0) or NULL (only in IS NULL). Column names may be
// "quoted". Keywords are case-insensitive.
//
// An empty string gives a nil filter (no filter). Errors wrap
// ErrFilterSyntax and name the byte position of the problem.
func ParseFilter(s string) (*Filter, error) {
	p := &fparser{src: s}
	p.next()
	if p.err != nil {
		return nil, p.err
	}
	if p.tok.kind == tEOF {
		return nil, nil
	}
	f, err := p.or(0)
	if err != nil {
		return nil, err
	}
	if p.err != nil {
		return nil, p.err
	}
	if p.tok.kind != tEOF {
		return nil, p.errf("unexpected %q", p.tok.text)
	}
	return f, nil
}

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tString
	tNumber
	tBlob
	tOp
	tLParen
	tRParen
	tComma
)

type ftok struct {
	kind tokKind
	text string
	val  any
	pos  int
}

type fparser struct {
	src string
	i   int
	tok ftok
	err error
}

func (p *fparser) errf(format string, args ...any) error {
	return fmt.Errorf("%w at %d: %s", ErrFilterSyntax, p.tok.pos, fmt.Sprintf(format, args...))
}

func (p *fparser) next() {
	for p.i < len(p.src) && strings.ContainsRune(" \t\r\n", rune(p.src[p.i])) {
		p.i++
	}
	start := p.i
	if p.i >= len(p.src) {
		p.tok = ftok{kind: tEOF, pos: start}
		return
	}
	c := p.src[p.i]
	switch {
	case c == '(':
		p.i++
		p.tok = ftok{kind: tLParen, text: "(", pos: start}
	case c == ')':
		p.i++
		p.tok = ftok{kind: tRParen, text: ")", pos: start}
	case c == ',':
		p.i++
		p.tok = ftok{kind: tComma, text: ",", pos: start}
	case c == '\'':
		var b strings.Builder
		p.i++
		for {
			if p.i >= len(p.src) {
				p.tok = ftok{kind: tEOF, pos: start}
				p.err = fmt.Errorf("%w at %d: unterminated string", ErrFilterSyntax, start)
				return
			}
			if p.src[p.i] == '\'' {
				if p.i+1 < len(p.src) && p.src[p.i+1] == '\'' {
					b.WriteByte('\'')
					p.i += 2
					continue
				}
				p.i++
				break
			}
			b.WriteByte(p.src[p.i])
			p.i++
		}
		p.tok = ftok{kind: tString, text: b.String(), val: b.String(), pos: start}
	case c == '"':
		j := p.i + 1
		var b strings.Builder
		for j < len(p.src) {
			if p.src[j] == '"' {
				if j+1 < len(p.src) && p.src[j+1] == '"' {
					b.WriteByte('"')
					j += 2
					continue
				}
				break
			}
			b.WriteByte(p.src[j])
			j++
		}
		if j >= len(p.src) {
			p.err = fmt.Errorf("%w at %d: unterminated identifier", ErrFilterSyntax, start)
			p.tok = ftok{kind: tEOF, pos: start}
			return
		}
		p.i = j + 1
		p.tok = ftok{kind: tIdent, text: b.String(), pos: start}
	case (c == 'x' || c == 'X') && p.i+1 < len(p.src) && p.src[p.i+1] == '\'':
		end := strings.IndexByte(p.src[p.i+2:], '\'')
		if end < 0 {
			p.err = fmt.Errorf("%w at %d: unterminated blob", ErrFilterSyntax, start)
			p.tok = ftok{kind: tEOF, pos: start}
			return
		}
		h := p.src[p.i+2 : p.i+2+end]
		b, err := hex.DecodeString(h)
		if err != nil {
			p.err = fmt.Errorf("%w at %d: bad blob", ErrFilterSyntax, start)
			p.tok = ftok{kind: tEOF, pos: start}
			return
		}
		if b == nil {
			b = []byte{}
		}
		p.i += 3 + end
		p.tok = ftok{kind: tBlob, text: h, val: b, pos: start}
	case c == '-' || c == '+' || c == '.' || (c >= '0' && c <= '9'):
		j := p.i + 1
		for j < len(p.src) && (strings.ContainsRune("0123456789.eE", rune(p.src[j])) ||
			((p.src[j] == '-' || p.src[j] == '+') && (p.src[j-1] == 'e' || p.src[j-1] == 'E'))) {
			j++
		}
		text := p.src[p.i:j]
		p.i = j
		v, err := decodeNumber(strings.TrimPrefix(text, "+"))
		if err != nil {
			p.err = fmt.Errorf("%w at %d: bad number %q", ErrFilterSyntax, start, text)
			p.tok = ftok{kind: tEOF, pos: start}
			return
		}
		p.tok = ftok{kind: tNumber, text: text, val: v, pos: start}
	case strings.ContainsRune("=!<>^", rune(c)):
		j := p.i + 1
		if j < len(p.src) && strings.ContainsRune("=>", rune(p.src[j])) {
			j++
		}
		p.tok = ftok{kind: tOp, text: p.src[p.i:j], pos: start}
		p.i = j
	default:
		j := p.i
		for j < len(p.src) && (p.src[j] == '_' || p.src[j] >= 0x80 || (p.src[j] >= 'a' && p.src[j] <= 'z') ||
			(p.src[j] >= 'A' && p.src[j] <= 'Z') || (p.src[j] >= '0' && p.src[j] <= '9')) {
			j++
		}
		if j == p.i {
			p.err = fmt.Errorf("%w at %d: unexpected %q", ErrFilterSyntax, start, string(c))
			p.tok = ftok{kind: tEOF, pos: start}
			return
		}
		p.tok = ftok{kind: tIdent, text: p.src[p.i:j], pos: start}
		p.i = j
	}
}

func (p *fparser) kw(s string) bool { return p.tok.kind == tIdent && strings.EqualFold(p.tok.text, s) }

func (p *fparser) or(d int) (*Filter, error) {
	if d > maxFilterDepth {
		return nil, p.errf("nested too deeply")
	}
	l, err := p.and(d)
	if err != nil {
		return nil, err
	}
	args := []Filter{*l}
	for p.kw("or") {
		p.next()
		r, err := p.and(d)
		if err != nil {
			return nil, err
		}
		args = append(args, *r)
	}
	if len(args) == 1 {
		return l, nil
	}
	return &Filter{Op: "or", Args: args}, nil
}

func (p *fparser) and(d int) (*Filter, error) {
	l, err := p.unary(d)
	if err != nil {
		return nil, err
	}
	args := []Filter{*l}
	for p.kw("and") {
		p.next()
		r, err := p.unary(d)
		if err != nil {
			return nil, err
		}
		args = append(args, *r)
	}
	if len(args) == 1 {
		return l, nil
	}
	return &Filter{Op: "and", Args: args}, nil
}

func (p *fparser) unary(d int) (*Filter, error) {
	if p.err != nil {
		return nil, p.err
	}
	if p.kw("not") {
		p.next()
		x, err := p.unary(d + 1)
		if err != nil {
			return nil, err
		}
		return &Filter{Op: "not", Args: []Filter{*x}}, nil
	}
	if p.tok.kind == tLParen {
		p.next()
		x, err := p.or(d + 1)
		if err != nil {
			return nil, err
		}
		if p.tok.kind != tRParen {
			return nil, p.errf("expected )")
		}
		p.next()
		return x, nil
	}
	return p.leaf()
}

func (p *fparser) value() (any, error) {
	if p.err != nil {
		return nil, p.err
	}
	switch {
	case p.tok.kind == tString || p.tok.kind == tNumber || p.tok.kind == tBlob:
		v := p.tok.val
		p.next()
		return v, nil
	case p.kw("true"):
		p.next()
		return int64(1), nil
	case p.kw("false"):
		p.next()
		return int64(0), nil
	}
	return nil, p.errf("expected a value, got %q", p.tok.text)
}

func (p *fparser) leaf() (*Filter, error) {
	if p.err != nil {
		return nil, p.err
	}
	if p.tok.kind != tIdent {
		return nil, p.errf("expected a column name, got %q", p.tok.text)
	}
	col := p.tok.text
	p.next()
	if p.err != nil {
		return nil, p.err
	}
	switch {
	case p.kw("is"):
		p.next()
		not := false
		if p.kw("not") {
			not = true
			p.next()
		}
		if !p.kw("null") {
			return nil, p.errf("expected NULL")
		}
		p.next()
		if not {
			return &Filter{Op: "not_null", Col: col}, nil
		}
		return &Filter{Op: "is_null", Col: col}, nil
	case p.kw("in") || p.kw("not"):
		op := "in"
		if p.kw("not") {
			p.next()
			if !p.kw("in") {
				return nil, p.errf("expected IN")
			}
			op = "not_in"
		}
		p.next()
		if p.tok.kind != tLParen {
			return nil, p.errf("expected (")
		}
		p.next()
		var vals []any
		for {
			v, err := p.value()
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
			if len(vals) > maxFilterValues {
				return nil, p.errf("more than %d values", maxFilterValues)
			}
			if p.tok.kind == tComma {
				p.next()
				continue
			}
			break
		}
		if p.tok.kind != tRParen {
			return nil, p.errf("expected )")
		}
		p.next()
		return &Filter{Op: op, Col: col, Vals: vals}, nil
	case p.tok.kind == tOp:
		op := p.tok.text
		switch op {
		case "==":
			op = "="
		case "<>":
			op = "!="
		case "^=":
			op = "prefix"
		case "=", "!=", "<", "<=", ">", ">=":
		default:
			return nil, p.errf("unknown operator %q", op)
		}
		p.next()
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		return &Filter{Op: op, Col: col, Val: v}, nil
	}
	return nil, p.errf("expected an operator after %q", col)
}

// decodeNumber parses an integer, or a float when the text has a
// fraction or exponent or does not fit in an int64.
func decodeNumber(s string) (any, error) {
	if !strings.ContainsAny(s, ".eE") {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, nil
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		var ne *strconv.NumError
		if errors.As(err, &ne) && errors.Is(ne.Err, strconv.ErrRange) {
			return f, nil // ±Inf (SQLite writes 9.0e+999) or a denormal rounded to 0
		}
		return nil, err
	}
	return f, nil
}

// MustParseFilter is ParseFilter for filters written in the program's
// source: it panics on a syntax error.
func MustParseFilter(s string) *Filter {
	f, err := ParseFilter(s)
	if err != nil {
		panic(err)
	}
	return f
}
