package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// LoadLastID reads the last processed local message ID from path.
// Missing file yields 0 (process entire current inbox catch-up path).
func LoadLastID(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		return 0, nil
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse state file %s: %w", path, err)
	}
	if id < 0 {
		return 0, fmt.Errorf("negative last id in %s", path)
	}
	return id, nil
}

// SaveLastID atomically writes lastID to path (mode 0600).
func SaveLastID(path string, lastID int64) error {
	if lastID < 0 {
		return fmt.Errorf("last id must be non-negative")
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmpDir := dir
	if tmpDir == "" || tmpDir == "." {
		tmpDir = "."
	}
	tmp, err := os.CreateTemp(tmpDir, "fmsg-groot-state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := fmt.Fprintf(tmp, "%d\n", lastID); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
