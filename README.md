sql
===

This repository holds a pure Go SQL parser based on the [SQLite](https://sqlite.org/) SQL definition. It implements nearly all features of the language except some other minor features.

This parser was originally created by [Ben Johnson](https://github.com/benbjohnson).

## Example Usage

Review the unit tests in `parser_test.go` or `examples` folder for an extensive set of parsing examples.

## Diff

- supports more features of sqlite like `ATTACH`, `DETACH`, `VACUUM`,...
- refactor ast, scanner, parser
- increase perf
- add more tests, see `testdata` folder

### Benchmark

```sh
goos: darwin
goarch: arm64
pkg: github.com/TcMits/sql
cpu: Apple M4 Pro
Benchmark_NewScanner-14                 14077094                83.88 ns/op            0 B/op          0 allocs/op
Benchmark_NewScanner-14                 15354103                76.60 ns/op            0 B/op          0 allocs/op
Benchmark_NewScanner-14                 15823201                74.77 ns/op            0 B/op          0 allocs/op
Benchmark_NewParser_Simple-14            5014988               233.1 ns/op           472 B/op          8 allocs/op
Benchmark_NewParser_Simple-14            5234992               228.8 ns/op           472 B/op          8 allocs/op
Benchmark_NewParser_Simple-14            5220668               230.6 ns/op           472 B/op          8 allocs/op
Benchmark_NewParser_Hard-14              1000000              1141 ns/op            1800 B/op         42 allocs/op
Benchmark_NewParser_Hard-14              1000000              1142 ns/op            1800 B/op         42 allocs/op
Benchmark_NewParser_Hard-14              1000000              1132 ns/op            1800 B/op         42 allocs/op
Benchmark_Walk-14                        6091918               193.5 ns/op             0 B/op          0 allocs/op
Benchmark_Walk-14                        6305332               191.0 ns/op             0 B/op          0 allocs/op
Benchmark_Walk-14                        6295114               190.0 ns/op             0 B/op          0 allocs/op
```
