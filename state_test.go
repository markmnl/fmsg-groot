package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSaveLastID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "last")

	id, err := LoadLastID(path)
	if err != nil || id != 0 {
		t.Fatalf("missing file: id=%d err=%v", id, err)
	}

	if err := SaveLastID(path, 42); err != nil {
		t.Fatal(err)
	}
	id, err = LoadLastID(path)
	if err != nil || id != 42 {
		t.Fatalf("after save: id=%d err=%v", id, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("state file should not be group/world accessible: %o", info.Mode().Perm())
	}
}
