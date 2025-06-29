//line "/input.txt":1
package sql

// Returns "fake" terminating null if cursor has reached limit.
func peekIsTypeName(str string, cur int) byte {
	if cur >= len(str) {
		return 0 // fake null
	} else {
		return str[cur]
	}
}

// run script: re2c $INPUT --lang go -o $OUTPUT --bit-vectors --nested-ifs
func isTypeName(str string) bool {
	var cur, marker int

//line "/output.txt":20
	{
		var yych byte
		yych = peekIsTypeName(str, cur)
		switch yych {
		case 'B':
			fallthrough
		case 'b':
			goto yy3
		case 'C':
			fallthrough
		case 'c':
			goto yy4
		case 'D':
			fallthrough
		case 'd':
			goto yy5
		case 'F':
			fallthrough
		case 'f':
			goto yy6
		case 'I':
			fallthrough
		case 'i':
			goto yy7
		case 'M':
			fallthrough
		case 'm':
			goto yy8
		case 'N':
			fallthrough
		case 'n':
			goto yy9
		case 'R':
			fallthrough
		case 'r':
			goto yy10
		case 'S':
			fallthrough
		case 's':
			goto yy11
		case 'T':
			fallthrough
		case 't':
			goto yy12
		case 'V':
			fallthrough
		case 'v':
			goto yy13
		default:
			goto yy1
		}
	yy1:
		cur += 1
	yy2:
//line "/input.txt":28
		{
			return false
		}
//line "/output.txt":77
	yy3:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych <= 'O' {
			if yych <= 'K' {
				if yych == 'I' {
					goto yy14
				}
				goto yy2
			} else {
				if yych <= 'L' {
					goto yy16
				}
				if yych <= 'N' {
					goto yy2
				}
				goto yy17
			}
		} else {
			if yych <= 'k' {
				if yych == 'i' {
					goto yy14
				}
				goto yy2
			} else {
				if yych <= 'l' {
					goto yy16
				}
				if yych == 'o' {
					goto yy17
				}
				goto yy2
			}
		}
	yy4:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych <= 'L' {
			if yych == 'H' {
				goto yy18
			}
			if yych <= 'K' {
				goto yy2
			}
			goto yy16
		} else {
			if yych <= 'h' {
				if yych <= 'g' {
					goto yy2
				}
				goto yy18
			} else {
				if yych == 'l' {
					goto yy16
				}
				goto yy2
			}
		}
	yy5:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych <= 'O' {
			if yych <= 'D' {
				if yych == 'A' {
					goto yy19
				}
				goto yy2
			} else {
				if yych <= 'E' {
					goto yy20
				}
				if yych <= 'N' {
					goto yy2
				}
				goto yy21
			}
		} else {
			if yych <= 'd' {
				if yych == 'a' {
					goto yy19
				}
				goto yy2
			} else {
				if yych <= 'e' {
					goto yy20
				}
				if yych == 'o' {
					goto yy21
				}
				goto yy2
			}
		}
	yy6:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych == 'L' {
			goto yy22
		}
		if yych == 'l' {
			goto yy22
		}
		goto yy2
	yy7:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych == 'N' {
			goto yy23
		}
		if yych == 'n' {
			goto yy23
		}
		goto yy2
	yy8:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych == 'E' {
			goto yy24
		}
		if yych == 'e' {
			goto yy24
		}
		goto yy2
	yy9:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych <= 'V' {
			if yych <= 'C' {
				if yych <= 'B' {
					goto yy2
				}
				goto yy25
			} else {
				if yych <= 'T' {
					goto yy2
				}
				if yych <= 'U' {
					goto yy26
				}
				goto yy27
			}
		} else {
			if yych <= 't' {
				if yych == 'c' {
					goto yy25
				}
				goto yy2
			} else {
				if yych <= 'u' {
					goto yy26
				}
				if yych <= 'v' {
					goto yy27
				}
				goto yy2
			}
		}
	yy10:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych == 'E' {
			goto yy28
		}
		if yych == 'e' {
			goto yy28
		}
		goto yy2
	yy11:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych == 'M' {
			goto yy29
		}
		if yych == 'm' {
			goto yy29
		}
		goto yy2
	yy12:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych <= 'I' {
			if yych == 'E' {
				goto yy30
			}
			if yych <= 'H' {
				goto yy2
			}
			goto yy31
		} else {
			if yych <= 'e' {
				if yych <= 'd' {
					goto yy2
				}
				goto yy30
			} else {
				if yych == 'i' {
					goto yy31
				}
				goto yy2
			}
		}
	yy13:
		cur += 1
		marker = cur
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy32
		}
		if yych == 'a' {
			goto yy32
		}
		goto yy2
	yy14:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'G' {
			goto yy33
		}
		if yych == 'g' {
			goto yy33
		}
	yy15:
		cur = marker
		goto yy2
	yy16:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'O' {
			goto yy34
		}
		if yych == 'o' {
			goto yy34
		}
		goto yy15
	yy17:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'O' {
			goto yy35
		}
		if yych == 'o' {
			goto yy35
		}
		goto yy15
	yy18:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy36
		}
		if yych == 'a' {
			goto yy36
		}
		goto yy15
	yy19:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'T' {
			goto yy37
		}
		if yych == 't' {
			goto yy37
		}
		goto yy15
	yy20:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'C' {
			goto yy38
		}
		if yych == 'c' {
			goto yy38
		}
		goto yy15
	yy21:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'U' {
			goto yy39
		}
		if yych == 'u' {
			goto yy39
		}
		goto yy15
	yy22:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'O' {
			goto yy40
		}
		if yych == 'o' {
			goto yy40
		}
		goto yy15
	yy23:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'T' {
			goto yy41
		}
		if yych == 't' {
			goto yy41
		}
		goto yy15
	yy24:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'D' {
			goto yy42
		}
		if yych == 'd' {
			goto yy42
		}
		goto yy15
	yy25:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'H' {
			goto yy43
		}
		if yych == 'h' {
			goto yy43
		}
		goto yy15
	yy26:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'M' {
			goto yy44
		}
		if yych == 'm' {
			goto yy44
		}
		goto yy15
	yy27:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy32
		}
		if yych == 'a' {
			goto yy32
		}
		goto yy15
	yy28:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy45
		}
		if yych == 'a' {
			goto yy45
		}
		goto yy15
	yy29:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy46
		}
		if yych == 'a' {
			goto yy46
		}
		goto yy15
	yy30:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'X' {
			goto yy47
		}
		if yych == 'x' {
			goto yy47
		}
		goto yy15
	yy31:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'N' {
			goto yy48
		}
		if yych == 'n' {
			goto yy48
		}
		goto yy15
	yy32:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'R' {
			goto yy49
		}
		if yych == 'r' {
			goto yy49
		}
		goto yy15
	yy33:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'I' {
			goto yy50
		}
		if yych == 'i' {
			goto yy50
		}
		goto yy15
	yy34:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'B' {
			goto yy51
		}
		if yych == 'b' {
			goto yy51
		}
		goto yy15
	yy35:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'L' {
			goto yy52
		}
		if yych == 'l' {
			goto yy52
		}
		goto yy15
	yy36:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'R' {
			goto yy53
		}
		if yych == 'r' {
			goto yy53
		}
		goto yy15
	yy37:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'E' {
			goto yy54
		}
		if yych == 'e' {
			goto yy54
		}
		goto yy15
	yy38:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'I' {
			goto yy55
		}
		if yych == 'i' {
			goto yy55
		}
		goto yy15
	yy39:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'B' {
			goto yy56
		}
		if yych == 'b' {
			goto yy56
		}
		goto yy15
	yy40:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy47
		}
		if yych == 'a' {
			goto yy47
		}
		goto yy15
	yy41:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych <= 'E' {
			if yych <= 0x00 {
				goto yy57
			}
			if yych <= 'D' {
				goto yy15
			}
			goto yy58
		} else {
			if yych == 'e' {
				goto yy58
			}
			goto yy15
		}
	yy42:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'I' {
			goto yy59
		}
		if yych == 'i' {
			goto yy59
		}
		goto yy15
	yy43:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy60
		}
		if yych == 'a' {
			goto yy60
		}
		goto yy15
	yy44:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'E' {
			goto yy61
		}
		if yych == 'e' {
			goto yy61
		}
		goto yy15
	yy45:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'L' {
			goto yy51
		}
		if yych == 'l' {
			goto yy51
		}
		goto yy15
	yy46:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'L' {
			goto yy62
		}
		if yych == 'l' {
			goto yy62
		}
		goto yy15
	yy47:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'T' {
			goto yy51
		}
		if yych == 't' {
			goto yy51
		}
		goto yy15
	yy48:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'Y' {
			goto yy33
		}
		if yych == 'y' {
			goto yy33
		}
		goto yy15
	yy49:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'C' {
			goto yy25
		}
		if yych == 'c' {
			goto yy25
		}
		goto yy15
	yy50:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'N' {
			goto yy47
		}
		if yych == 'n' {
			goto yy47
		}
		goto yy15
	yy51:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych <= 0x00 {
			goto yy57
		}
		goto yy15
	yy52:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'E' {
			goto yy63
		}
		if yych == 'e' {
			goto yy63
		}
		goto yy15
	yy53:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy64
		}
		if yych == 'a' {
			goto yy64
		}
		goto yy15
	yy54:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych <= 'T' {
			if yych <= 0x00 {
				goto yy57
			}
			if yych <= 'S' {
				goto yy15
			}
			goto yy65
		} else {
			if yych == 't' {
				goto yy65
			}
			goto yy15
		}
	yy55:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'M' {
			goto yy28
		}
		if yych == 'm' {
			goto yy28
		}
		goto yy15
	yy56:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'L' {
			goto yy66
		}
		if yych == 'l' {
			goto yy66
		}
		goto yy15
	yy57:
		cur += 1
//line "/input.txt":27
		{
			return true
		}
//line "/output.txt":736
	yy58:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'G' {
			goto yy67
		}
		if yych == 'g' {
			goto yy67
		}
		goto yy15
	yy59:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'U' {
			goto yy68
		}
		if yych == 'u' {
			goto yy68
		}
		goto yy15
	yy60:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'R' {
			goto yy51
		}
		if yych == 'r' {
			goto yy51
		}
		goto yy15
	yy61:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'R' {
			goto yy69
		}
		if yych == 'r' {
			goto yy69
		}
		goto yy15
	yy62:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'L' {
			goto yy33
		}
		if yych == 'l' {
			goto yy33
		}
		goto yy15
	yy63:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'A' {
			goto yy70
		}
		if yych == 'a' {
			goto yy70
		}
		goto yy15
	yy64:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'C' {
			goto yy71
		}
		if yych == 'c' {
			goto yy71
		}
		goto yy15
	yy65:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'I' {
			goto yy72
		}
		if yych == 'i' {
			goto yy72
		}
		goto yy15
	yy66:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'E' {
			goto yy51
		}
		if yych == 'e' {
			goto yy51
		}
		goto yy15
	yy67:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'E' {
			goto yy60
		}
		if yych == 'e' {
			goto yy60
		}
		goto yy15
	yy68:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'M' {
			goto yy33
		}
		if yych == 'm' {
			goto yy33
		}
		goto yy15
	yy69:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'I' {
			goto yy73
		}
		if yych == 'i' {
			goto yy73
		}
		goto yy15
	yy70:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'N' {
			goto yy51
		}
		if yych == 'n' {
			goto yy51
		}
		goto yy15
	yy71:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'T' {
			goto yy67
		}
		if yych == 't' {
			goto yy67
		}
		goto yy15
	yy72:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'M' {
			goto yy66
		}
		if yych == 'm' {
			goto yy66
		}
		goto yy15
	yy73:
		cur += 1
		yych = peekIsTypeName(str, cur)
		if yych == 'C' {
			goto yy51
		}
		if yych == 'c' {
			goto yy51
		}
		goto yy15
	}
//line "/input.txt":29
}
