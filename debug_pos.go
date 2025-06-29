//go:build debugpos
// +build debugpos

package sql

import "fmt"

type Pos struct {
	Offset int // offset, starting at 0 (byte offset)
	Line   int // line number, starting at 1
	Column int // column number, starting at 1 (byte count)
}

// String returns a string representation of the position.
func (p Pos) String() string {
	if !(p.Line > 0 && p.Column > 0 && p.Offset >= 0) {
		return "-"
	}
	return fmt.Sprintf("%d:%d", p.Line, p.Column)
}

func NewValidPos() Pos {
	return Pos{Offset: 0, Line: 1, Column: 1}
}

func (p Pos) Increase(ch byte) Pos {
	p.Offset += 1
	if ch == '\n' {
		p.Line++
		p.Column = 1
	} else {
		p.Column += 1
	}

	return p
}

func (p Pos) GetOffset() int {
	return p.Offset
}
