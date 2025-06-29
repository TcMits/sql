package sql_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/TcMits/sql"
)

func Test_ParseTestData(t *testing.T) {
	yield := func(_ sql.Statement) error { return nil }

	filepath.Walk("./testdata/", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		t.Run(path, func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Error(err)
			}
			defer f.Close()

			b, err := io.ReadAll(f)
			if err != nil {
				t.Error(err)
			}

			if err := sql.ParseMultiStmtString(string(b), yield); err != nil {
				t.Errorf("error parsing %s: %v", path, err)
			}
		})

		return nil
	})
}
