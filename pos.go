//go:build !debugpos
// +build !debugpos

package sql

import (
	"strconv"
)

// Pos represents a position in the source code.
type Pos int

// String returns a string representation of the position.
func (p Pos) String() string {
	return strconv.Itoa(int(p))
}

func NewValidPos() Pos {
	return 0
}

func (p Pos) Increase(_ byte) Pos {
	return p + 1
}

func (p Pos) GetOffset() int {
	return int(p)
}
