package gqm

import (
	"regexp"
	"testing"
	"time"
)

var uuidV7Regex = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
)

func TestNewUUID_Format(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := NewUUID()
		if !uuidV7Regex.MatchString(id) {
			t.Errorf("UUID %q does not match v7 format", id)
		}
	}
}

func TestNewUUID_Unique(t *testing.T) {
	seen := make(map[string]bool, 10000)
	for i := 0; i < 10000; i++ {
		id := NewUUID()
		if seen[id] {
			t.Fatalf("duplicate UUID: %s", id)
		}
		seen[id] = true
	}
}

func TestNewUUID_TimeSortable(t *testing.T) {
	id1 := newUUIDAt(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	// Reset seq for different timestamp
	uuidMu.Lock()
	lastMS = 0
	uuidSeq = 0
	uuidMu.Unlock()

	id2 := newUUIDAt(time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC))

	if id1 >= id2 {
		t.Errorf("UUIDs not time-sortable: %s >= %s", id1, id2)
	}
}

func TestNewUUID_SubMillisecondOrdering(t *testing.T) {
	// Reset
	uuidMu.Lock()
	lastMS = 0
	uuidSeq = 0
	uuidMu.Unlock()

	now := time.Now()
	ids := make([]string, 10)
	for i := range ids {
		ids[i] = newUUIDAt(now)
	}

	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("sub-ms ordering broken at index %d: %s <= %s", i, ids[i], ids[i-1])
		}
	}
}
