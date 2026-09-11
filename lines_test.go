package main

import (
	"slices"
	"strings"
	"testing"
)

func TestGrootLinesNonEmptyAndContainGroot(t *testing.T) {
	if len(GrootLines) < 10 {
		t.Fatalf("expected at least 10 lines, got %d", len(GrootLines))
	}
	for i, line := range GrootLines {
		if strings.TrimSpace(line) == "" {
			t.Errorf("line %d is empty", i)
		}
		if !strings.Contains(strings.ToLower(line), "groot") {
			t.Errorf("line %d %q does not contain groot", i, line)
		}
	}
}

func TestPickLineFromTable(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		line := PickLine()
		seen[line] = true
		ok := slices.Contains(GrootLines, line)
		if !ok {
			t.Fatalf("PickLine returned unknown line %q", line)
		}
	}
	if len(seen) < 2 {
		t.Fatalf("expected variety from PickLine, got %d unique", len(seen))
	}
}
