package sql

import (
	"strings"
	"unsafe"
)

func stob(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

type Scanner struct {
	s    string
	prev Pos // index of previous byte
	pos  Pos
}

// NewScanner creates a new Scanner for the given string.
func NewScanner(s string) Scanner {
	return Scanner{
		s:    s,
		prev: NewValidPos(),
		pos:  NewValidPos(),
	}
}

// Scan returns the next token from the input string.
func (s *Scanner) Scan() (pos Pos, token Token, lit string) {
	for {
		if ch := s.peek(); ch == 0 && s.isEOF() {
			return s.pos, EOF, ""
		} else if isSpace(ch) {
			s.read()
			continue
		} else if isDigit(ch) || ch == '.' {
			return s.scanNumber()
		} else if ch == 'x' || ch == 'X' {
			return s.scanBlob()
		} else if isAlpha(ch) || ch == '_' {
			return s.scanUnquotedIdent()
		} else if ch == '"' || ch == '`' || ch == '[' {
			return s.scanQuotedIdent()
		} else if ch == '\'' {
			return s.scanString()
		} else if ch == '?' || ch == ':' || ch == '@' || ch == '$' {
			return s.scanBind()
		}

		switch ch, pos := s.read(); ch {
		case ';':
			return pos, SEMI, ";"
		case '(':
			return pos, LP, "("
		case ')':
			return pos, RP, ")"
		case ',':
			return pos, COMMA, ","
		case '!':
			if s.peek() == '=' {
				s.read()
				return pos, NE, "!="
			}
			return pos, ILLEGAL, "!"
		case '=':
			if s.peek() == '=' {
				s.read()
				return pos, EQ, "=="
			}
			return pos, EQ, "="
		case '<':
			if s.peek() == '=' {
				s.read()
				return pos, LE, "<="
			} else if s.peek() == '<' {
				s.read()
				return pos, LSHIFT, "<<"
			} else if s.peek() == '>' {
				s.read()
				return pos, NE, "<>"
			}
			return pos, LT, "<"
		case '>':
			if s.peek() == '=' {
				s.read()
				return pos, GE, ">="
			} else if s.peek() == '>' {
				s.read()
				return pos, RSHIFT, ">>"
			}
			return pos, GT, ">"
		case '&':
			return pos, BITAND, "&"
		case '|':
			if s.peek() == '|' {
				s.read()
				return pos, CONCAT, "||"
			}
			return pos, BITOR, "|"
		case '+':
			return pos, PLUS, "+"
		case '-':
			if s.peek() == '>' {
				s.read()
				if s.peek() == '>' {
					s.read()
					return pos, JSON_EXTRACT_SQL, "->>"
				}
				return pos, JSON_EXTRACT_JSON, "->"
			} else if s.peek() == '-' {
				s.read()
				return pos, COMMENT, s.scanSingleLineComment()
			}
			return pos, MINUS, "-"
		case '*':
			return pos, STAR, "*"
		case '/':
			if s.peek() == '*' {
				s.read()
				return pos, COMMENT, s.scanMultiLineComment()
			}
			return pos, SLASH, "/"
		case '%':
			return pos, REM, "%"
		case '~':
			return pos, BITNOT, "~"
		default:
			return pos, ILLEGAL, string(ch)
		}
	}
}

func (s *Scanner) scanUnquotedIdent() (Pos, Token, string) {
	assert(isUnquotedIdent(s.peek()))

	pos := s.pos
	for isUnquotedIdent(s.peek()) {
		s.read()
	}
	end := s.pos.GetOffset()

	lit := s.s[pos.GetOffset():end]
	tok := keywordOrIdent(lit)
	return pos, tok, lit
}

func (s *Scanner) scanQuotedIdent() (Pos, Token, string) {
	ch, pos := s.read()
	var expectedEnd byte = '"'
	allowDuplicate := true
	findDuplicate := false
	switch ch {
	case '`':
		expectedEnd = '`'
	case '[':
		expectedEnd = ']'
		allowDuplicate = false
	case '"':
		expectedEnd = '"'
	default:
		panic("unexpected character for quoted identifier: " + string(ch))
	}

	start := s.pos.GetOffset()
	for {
		ch, _ := s.read()
		if ch == 0 && s.isEOF() {
			return pos, ILLEGAL, `"` + s.s[start:s.pos.GetOffset()]
		} else if ch == expectedEnd {
			if s.peek() == expectedEnd && allowDuplicate { // escaped quote
				s.read()
				findDuplicate = true
				continue
			}

			if findDuplicate { // we found a duplicate quote, so we need to skip it
				return pos, QIDENT, strings.ReplaceAll(s.s[start:s.pos.GetOffset()-1], string(expectedEnd)+string(expectedEnd), string(expectedEnd))
			}

			return pos, QIDENT, s.s[start : s.pos.GetOffset()-1]
		}
	}
}

func (s *Scanner) scanString() (Pos, Token, string) {
	ch, pos := s.read()
	assert(ch == '\'')

	findDuplicate := false
	start := s.pos.GetOffset()
	for {
		ch, _ := s.read()
		if ch == 0 && s.isEOF() {
			return pos, ILLEGAL, s.s[start-1 : s.pos.GetOffset()]
		} else if ch == '\'' {
			if s.peek() == '\'' { // escaped quote
				s.read()
				findDuplicate = true
				continue
			}

			if findDuplicate { // we found a duplicate quote, so we need to skip it
				return pos, STRING, strings.ReplaceAll(s.s[start:s.pos.GetOffset()-1], "''", "'")
			}

			return pos, STRING, s.s[start : s.pos.GetOffset()-1]
		}
	}
}

func (s *Scanner) scanSingleLineComment() string {
	start := s.pos.GetOffset()
	for {
		ch, _ := s.read()
		switch ch {
		case 0:
			if s.isEOF() {
				return s.s[start-2 : s.pos.GetOffset()]
			}

			continue
		case '\n':
			return s.s[start-2 : s.pos.GetOffset()-1]
		}
	}
}

func (s *Scanner) scanMultiLineComment() string {
	start := s.pos.GetOffset()
	for {
		ch, _ := s.read()
		if ch == 0 && s.isEOF() {
			return s.s[start-2 : s.pos.GetOffset()] // EOF before closing comment
		} else if ch == '*' && s.peek() == '/' {
			s.read()
			return s.s[start-2 : s.pos.GetOffset()] // closing comment found
		}
	}
}

func (s *Scanner) scanBind() (Pos, Token, string) {
	start, pos := s.read()
	startIdx := pos.GetOffset()

	// Question mark starts a numeric bind.
	if start == '?' {
		for isDigit(s.peek()) {
			s.read()
		}
		return pos, BIND, s.s[startIdx:s.pos.GetOffset()]
	}

	// All other characters start an alphanumeric bind.
	assert(start == ':' || start == '@' || start == '$')
	for isUnquotedIdent(s.peek()) {
		s.read()
	}
	return pos, BIND, s.s[startIdx:s.pos.GetOffset()]
}

func (s *Scanner) scanBlob() (Pos, Token, string) {
	start, pos := s.read()
	assert(start == 'x' || start == 'X')

	// If the next character is not a quote, it's an IDENT.
	if isUnquotedIdent(s.peek()) {
		s.unread() // unread 'x' or 'X'
		return s.scanUnquotedIdent()
	} else if s.peek() != '\'' {
		return pos, IDENT, string(start)
	}
	ch, _ := s.read()
	assert(ch == '\'')

	startIdx := s.pos.GetOffset()
	for i := 0; ; i++ {
		ch, _ := s.read()
		if ch == '\'' {
			return pos, BLOB, s.s[startIdx : s.pos.GetOffset()-1]
		} else if ch == 0 && s.isEOF() {
			return pos, ILLEGAL, s.s[startIdx-2 : s.pos.GetOffset()]
		} else if !isHex(ch) {
			return pos, ILLEGAL, s.s[startIdx-2 : s.pos.GetOffset()]
		}
	}
}

func (s *Scanner) scanNumber() (Pos, Token, string) {
	assert(isDigit(s.peek()) || s.peek() == '.')
	pos := s.pos
	tok := INTEGER

	if s.peek() == '0' {
		s.read()
		if s.peek() == 'x' || s.peek() == 'X' {
			s.read()
			for isHex(s.peek()) {
				s.read()
			}

			// TODO: error handling:
			// if len(s.buf.String()) < 2 => invalid
			// reason: means we scanned '0x'
			// if len(s.buf.String()) - 2 > 16 => invalid
			// reason: according to spec maximum of 16 significant digits)
			return pos, tok, s.s[pos.GetOffset():s.pos.GetOffset()]
		}
	}

	// Read whole number if starting with a digit.
	if isDigit(s.peek()) {
		for isDigit(s.peek()) {
			s.read()
		}
	}

	// Read decimal and successive digits.
	if s.peek() == '.' {
		tok = FLOAT

		s.read()

		for isDigit(s.peek()) {
			s.read()
		}
	}

	// If we just have a dot in the buffer with no digits by this point,
	// this can't be a number, so we can stop and return DOT
	if s.s[pos.GetOffset():s.pos.GetOffset()] == "." {
		return pos, DOT, "."
	}

	// Read exponent with optional +/- sign.
	if ch := s.peek(); ch == 'e' || ch == 'E' {
		tok = FLOAT

		s.read()

		if s.peek() == '+' || s.peek() == '-' {
			s.read()
			if !isDigit(s.peek()) {
				return pos, ILLEGAL, s.s[pos.GetOffset():s.pos.GetOffset()]
			}
			for isDigit(s.peek()) {
				s.read()
			}
		} else if isDigit(s.peek()) {
			for isDigit(s.peek()) {
				s.read()
			}
		} else {
			return pos, ILLEGAL, s.s[pos.GetOffset():s.pos.GetOffset()]
		}
	}

	return pos, tok, s.s[pos.GetOffset():s.pos.GetOffset()]
}

func (s *Scanner) read() (byte, Pos) {
	if s.isEOF() {
		return 0, s.pos
	}

	pos := s.pos
	ch := s.peek()
	s.prev = pos
	s.pos = pos.Increase(ch)
	return ch, pos
}

func (s *Scanner) peek() byte {
	if s.isEOF() {
		return 0 // EOF
	}

	return s.s[s.pos.GetOffset()]
}

func (s *Scanner) unread() {
	assert(s.pos.GetOffset() > s.prev.GetOffset())
	s.pos = s.prev
}

func (s *Scanner) isEOF() bool {
	return s.pos.GetOffset() >= len(s.s)
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isAlpha(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isHex(ch byte) bool {
	return isDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func isUnquotedIdent(ch byte) bool {
	return isAlpha(ch) || isDigit(ch) || ch == '_'
}

// IsInteger returns true if s only contains digits.
func IsInteger(s string) bool {
	for _, ch := range stob(s) {
		if !isDigit(ch) {
			return false
		}
	}
	return s != ""
}

func isSpace(b byte) bool {
	switch b {
	case '\t', '\n', '\x0C', '\r', ' ':
		return true
	}
	return false
}
