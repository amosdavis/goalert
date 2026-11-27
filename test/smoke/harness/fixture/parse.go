package fixture

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type tokenKind int

const (
	tokEnd tokenKind = iota
	tokIdent
	tokString
	tokNumber
	tokOp
)

type token struct {
	kind tokenKind
	text string // identifiers are in lower case, unless they were quoted
	pos  int
}

func (t token) String() string {
	switch t.kind {
	case tokEnd:
		return "the end"
	case tokString:
		return "a string"
	}
	return strconv.Quote(t.text)
}

// tokens splits SQL into tokens, without comments.
func tokens(sql string) ([]token, error) {
	var toks []token
	for i := 0; i < len(sql); {
		c := sql[i]
		start := i
		switch {
		case strings.ContainsRune(" \t\r\n", rune(c)):
			i++
		case strings.HasPrefix(sql[i:], "--"):
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sql[i:], "/*"):
			end := strings.Index(sql[i:], "*/")
			if end < 0 {
				return nil, fmt.Errorf("a comment has no end")
			}
			i += end + 2
		case c == '\'' || c == '"':
			var b strings.Builder
			for i++; ; i++ {
				if i >= len(sql) {
					return nil, fmt.Errorf("%c has no end: %s", c, line(sql, start))
				}
				if sql[i] != c {
					b.WriteByte(sql[i])
					continue
				}
				if i++; i >= len(sql) || sql[i] != c {
					break
				}
				b.WriteByte(c) // a doubled quote
			}
			kind := tokString
			if c == '"' {
				kind = tokIdent
			}
			toks = append(toks, token{kind, b.String(), start})
		case c >= '0' && c <= '9':
			for i < len(sql) && (sql[i] >= '0' && sql[i] <= '9' || sql[i] == '.') {
				i++
			}
			toks = append(toks, token{tokNumber, sql[start:i], start})
		case c == '_' || unicode.IsLetter(rune(c)):
			for i < len(sql) && (sql[i] == '_' || unicode.IsLetter(rune(sql[i])) || unicode.IsDigit(rune(sql[i]))) {
				i++
			}
			toks = append(toks, token{tokIdent, strings.ToLower(sql[start:i]), start})
		case strings.HasPrefix(sql[i:], "::"):
			i += 2
			toks = append(toks, token{tokOp, "::", start})
		case strings.ContainsRune("(),;=+-.", rune(c)):
			i++
			toks = append(toks, token{tokOp, string(c), start})
		default:
			return nil, fmt.Errorf("unexpected %q: %s", c, line(sql, start))
		}
	}
	return append(toks, token{kind: tokEnd, pos: len(sql)}), nil
}

// line is the line of sql at pos, for messages.
func line(sql string, pos int) string {
	start := strings.LastIndexByte(sql[:pos], '\n') + 1
	end := strings.IndexByte(sql[pos:], '\n')
	if end < 0 {
		end = len(sql) - pos
	}
	return strings.TrimSpace(sql[start : pos+end])
}

type (
	statement interface{ run(x *execution) error }

	insertStmt struct {
		table string
		cols  []string
		rows  [][]expr
	}
	updateStmt struct {
		table      string
		set, where []columnValue
	}
	deleteStmt struct {
		table string
		where []columnValue
	}
	setZoneStmt struct{ zone string }

	columnValue struct {
		col string
		val expr
	}
)

type parser struct {
	sql  string
	toks []token
}

// parse parses the statements fixtures use.
func parse(sql string) (stmts []statement, err error) {
	toks, err := tokens(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{sql: sql, toks: toks}
	for {
		for p.is(";") {
		}
		if p.peek().kind == tokEnd {
			return stmts, nil
		}
		st, err := p.statement()
		if err != nil {
			return nil, err
		}
		if t := p.peek(); t.kind != tokEnd && t.text != ";" {
			return nil, p.unexpected()
		}
		stmts = append(stmts, st)
	}
}

func (p *parser) peek() token { return p.toks[0] }

func (p *parser) next() token {
	t := p.toks[0]
	if t.kind != tokEnd {
		p.toks = p.toks[1:]
	}
	return t
}

// is skips the next token if it is an operator or a keyword.
func (p *parser) is(text string) bool {
	if t := p.peek(); t.text != text || t.kind == tokString || t.kind == tokEnd {
		return false
	}
	p.next()
	return true
}

func (p *parser) unexpected() error {
	t := p.peek()
	return fmt.Errorf("unexpected %s: %s", t, line(p.sql, t.pos))
}

func (p *parser) expect(texts ...string) error {
	for _, text := range texts {
		if !p.is(text) {
			return fmt.Errorf("want %q: %w", text, p.unexpected())
		}
	}
	return nil
}

func (p *parser) ident() (string, error) {
	if p.peek().kind != tokIdent {
		return "", fmt.Errorf("want a name: %w", p.unexpected())
	}
	return p.next().text, nil
}

func (p *parser) statement() (statement, error) {
	switch {
	case p.is("insert"):
		return p.insert()
	case p.is("update"):
		return p.update()
	case p.is("delete"):
		if err := p.expect("from"); err != nil {
			return nil, err
		}
		table, err := p.ident()
		if err != nil {
			return nil, err
		}
		where, err := p.where()
		return deleteStmt{table, where}, err
	case p.is("set"):
		if err := p.expect("timezone", "="); err != nil {
			return nil, err
		}
		if p.peek().kind != tokString {
			return nil, p.unexpected()
		}
		return setZoneStmt{p.next().text}, nil
	}
	return nil, p.unexpected()
}

func (p *parser) insert() (statement, error) {
	if err := p.expect("into"); err != nil {
		return nil, err
	}
	table, err := p.ident()
	if err != nil {
		return nil, err
	}
	st := insertStmt{table: table}
	if err := p.expect("("); err != nil {
		return nil, err
	}
	for {
		col, err := p.ident()
		if err != nil {
			return nil, err
		}
		st.cols = append(st.cols, col)
		if !p.is(",") {
			break
		}
	}
	if err := p.expect(")", "values"); err != nil {
		return nil, err
	}
	for {
		if err := p.expect("("); err != nil {
			return nil, err
		}
		var row []expr
		for {
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			row = append(row, e)
			if !p.is(",") {
				break
			}
		}
		if len(row) != len(st.cols) {
			return nil, fmt.Errorf("%d columns, but %d values: %s", len(st.cols), len(row), line(p.sql, p.peek().pos))
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
		st.rows = append(st.rows, row)
		if !p.is(",") {
			return st, nil
		}
	}
}

func (p *parser) update() (statement, error) {
	table, err := p.ident()
	if err != nil {
		return nil, err
	}
	if err := p.expect("set"); err != nil {
		return nil, err
	}
	set, err := p.assignments(",")
	if err != nil {
		return nil, err
	}
	where, err := p.where()
	return updateStmt{table, set, where}, err
}

func (p *parser) where() ([]columnValue, error) {
	if !p.is("where") {
		return nil, nil
	}
	return p.assignments("and")
}

func (p *parser) assignments(sep string) (list []columnValue, err error) {
	for {
		col, err := p.ident()
		if err != nil {
			return nil, err
		}
		if err := p.expect("="); err != nil {
			return nil, err
		}
		e, err := p.expr()
		if err != nil {
			return nil, err
		}
		list = append(list, columnValue{col, e})
		if !p.is(sep) {
			return list, nil
		}
	}
}

func (p *parser) expr() (expr, error) {
	e, err := p.term()
	for err == nil {
		op := p.peek()
		if op.kind != tokOp || op.text != "+" && op.text != "-" {
			return e, nil
		}
		p.next()
		var right expr
		if right, err = p.term(); err == nil {
			e = sum{e, right, op.text == "-"}
		}
	}
	return nil, err
}

func (p *parser) term() (expr, error) {
	if p.is("-") {
		if p.peek().kind != tokNumber {
			return nil, p.unexpected()
		}
		n, err := strconv.ParseInt(p.next().text, 10, 64)
		return constant{-n}, err
	}
	e, err := p.primary()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.is("::"):
			typ, err := p.typeName()
			if err != nil {
				return nil, err
			}
			e = cast{e, typ}
		case p.is("at"):
			if err := p.expect("time", "zone"); err != nil {
				return nil, err
			}
			if p.peek().kind != tokString {
				return nil, p.unexpected()
			}
			e = atZone{e, p.next().text}
		default:
			return e, nil
		}
	}
}

func (p *parser) primary() (expr, error) {
	switch t := p.peek(); t.kind {
	case tokString:
		return constant{literal(p.next().text)}, nil
	case tokNumber:
		n, err := strconv.ParseInt(p.next().text, 10, 64)
		return constant{n}, err
	case tokEnd:
		return nil, p.unexpected()
	}
	switch {
	case p.is("("):
		e, err := p.expr()
		if err != nil {
			return nil, err
		}
		return e, p.expect(")")
	case p.is("null"):
		return constant{nil}, nil
	case p.is("true"):
		return constant{true}, nil
	case p.is("false"):
		return constant{false}, nil
	case p.is("now"):
		return now{}, p.expect("(", ")")
	case p.is("interval"):
		if p.peek().kind != tokString {
			return nil, p.unexpected()
		}
		return cast{constant{literal(p.next().text)}, "interval"}, nil
	case p.is("cast"):
		if err := p.expect("("); err != nil {
			return nil, err
		}
		e, err := p.expr()
		if err != nil {
			return nil, err
		}
		if err := p.expect("as"); err != nil {
			return nil, err
		}
		typ, err := p.typeName()
		if err != nil {
			return nil, err
		}
		return cast{e, typ}, p.expect(")")
	}
	return nil, p.unexpected()
}

func (p *parser) typeName() (string, error) {
	typ, err := p.ident()
	if err != nil || typ != "time" && typ != "timestamp" {
		return typ, err
	}
	switch {
	case p.is("with"):
		return typ + "tz", p.expect("time", "zone")
	case p.is("without"):
		return typ, p.expect("time", "zone")
	}
	return typ, nil
}
