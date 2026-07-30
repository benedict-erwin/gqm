package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// /api/v1/dag/roots is read-only, so a viewer can reach it. It used to SCAN the
// whole keyspace with no bound, no deadline, and pagination applied only after
// the scan finished — one request could turn into millions of Redis commands
// against the same instance the workers depend on.
//
// The bounds are package variables so these tests can shrink them; the real
// values would need a hundred thousand seeded keys to exercise.
//
// Because they shrink the bounds, these tests verify the mechanism, not the
// shipped values — they stay green even if someone raises a cap to infinity.
// TestSecurity_DAGRoots_ShippedBoundsAreFinite covers the values, and
// scripts/verify-security-fixes.sh exercises the endpoint at them end to end.

func withDAGScanBounds(t *testing.T, count int64, maxCalls, maxRoots int, timeout time.Duration) {
	t.Helper()
	oc, omc, omr, ot := dagScanCount, dagScanMaxCalls, dagScanMaxRoots, dagScanTimeout
	dagScanCount, dagScanMaxCalls, dagScanMaxRoots, dagScanTimeout = count, maxCalls, maxRoots, timeout
	t.Cleanup(func() {
		dagScanCount, dagScanMaxCalls, dagScanMaxRoots, dagScanTimeout = oc, omc, omr, ot
	})
}

// seedDAGRoots creates n job hashes that each have a :dependents set, which is
// what makes them look like DAG roots to the endpoint.
func seedDAGRoots(t *testing.T, m *Monitor, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("job%04d", i)
		if err := m.rdb.HSet(ctx, m.key("job", id),
			"id", id, "type", "test.job", "status", "completed").Err(); err != nil {
			t.Fatalf("seeding job %s: %v", id, err)
		}
		if err := m.rdb.SAdd(ctx, m.key("job", id, "dependents"), "child-"+id).Err(); err != nil {
			t.Fatalf("seeding dependents for %s: %v", id, err)
		}
	}
}

func dagRootsMeta(t *testing.T, m *Monitor, path string) (meta, int) {
	t.Helper()
	w := doRequest(m, http.MethodGet, path, "")
	var resp struct {
		Data []map[string]any `json:"data"`
		Meta meta             `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response (HTTP %d): %v — body: %s", w.Code, err, w.Body.String())
	}
	if w.Code != http.StatusOK {
		t.Fatalf("got HTTP %d, want 200 — body: %s", w.Code, w.Body.String())
	}
	return resp.Meta, len(resp.Data)
}

// The shipped bounds must actually bound something. Every other test here
// overrides them, so without this one the caps could be raised to effectively
// infinite and the suite would stay green — which is precisely the regression
// this finding was.
func TestSecurity_DAGRoots_ShippedBoundsAreFinite(t *testing.T) {
	if dagScanMaxCalls <= 0 || dagScanMaxCalls > 1000 {
		t.Errorf("dagScanMaxCalls is %d, want a small positive bound", dagScanMaxCalls)
	}
	if dagScanMaxRoots <= 0 || dagScanMaxRoots > 100_000 {
		t.Errorf("dagScanMaxRoots is %d, want a bounded result set", dagScanMaxRoots)
	}
	if dagScanTimeout <= 0 || dagScanTimeout > 30*time.Second {
		t.Errorf("dagScanTimeout is %v, want a deadline a caller would actually wait out", dagScanTimeout)
	}
	if dagScanCount <= 0 {
		t.Errorf("dagScanCount is %d, want a positive COUNT hint", dagScanCount)
	}
}

// The scan must stop once it has made its allowed number of round trips, and
// must say so. Round trips are the thing to bound: Redis applies MATCH after
// iterating, so a pattern that matches nothing still walks the entire keyspace.
func TestSecurity_DAGRoots_ScanStopsAtCallCap(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	seedDAGRoots(t, m, 40)

	// One key per call, two calls allowed: the scan cannot possibly reach all 40.
	withDAGScanBounds(t, 1, 2, 5000, 5*time.Second)

	meta, _ := dagRootsMeta(t, m, "/api/v1/dag/roots")
	if !meta.Truncated {
		t.Error("truncated flag not set after the call cap was hit")
	}
	if meta.Total >= 40 {
		t.Errorf("total is %d, want well under 40 — the scan did not stop at the cap", meta.Total)
	}
}

// The in-memory result set is capped independently, so a keyspace where
// everything matches cannot be used to grow the response without bound.
func TestSecurity_DAGRoots_StopsAtRootCap(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	seedDAGRoots(t, m, 30)

	withDAGScanBounds(t, 1000, 100, 5, 5*time.Second)

	meta, _ := dagRootsMeta(t, m, "/api/v1/dag/roots")
	if meta.Total > 5 {
		t.Errorf("collected %d roots, want at most the cap of 5", meta.Total)
	}
	if !meta.Truncated {
		t.Error("truncated flag not set after the root cap was hit")
	}
}

// A deadline must actually be attached to the request. With an impossible
// timeout the handler has to degrade to a truncated 200 rather than a 500 or a
// hang: the page assembled so far is still correct as far as it goes.
func TestSecurity_DAGRoots_DeadlineIsEnforced(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	seedDAGRoots(t, m, 20)

	withDAGScanBounds(t, 1, 100, 5000, time.Nanosecond)

	w := doRequest(m, http.MethodGet, "/api/v1/dag/roots", "")
	if w.Code != http.StatusOK {
		t.Fatalf("got HTTP %d, want 200 — an expired deadline should truncate, not fail", w.Code)
	}
	var resp struct {
		Meta meta `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if !resp.Meta.Truncated {
		t.Error("truncated flag not set after the deadline expired")
	}
}

// The bounds must not change the answer for a normal-sized dataset, which is
// what catches an over-tight cap.
func TestSecurity_DAGRoots_SmallDatasetIsCompleteAndNotTruncated(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	seedDAGRoots(t, m, 3)

	meta, got := dagRootsMeta(t, m, "/api/v1/dag/roots")
	if meta.Truncated {
		t.Error("truncated flag set for a dataset far below every cap")
	}
	if meta.Total != 3 {
		t.Errorf("total is %d, want 3", meta.Total)
	}
	if got != 3 {
		t.Errorf("returned %d roots, want 3", got)
	}
}

// truncated must be absent from the JSON when nothing was truncated, so a
// client cannot read a complete result as a partial one.
func TestSecurity_DAGRoots_TruncatedOmittedWhenComplete(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	seedDAGRoots(t, m, 2)

	w := doRequest(m, http.MethodGet, "/api/v1/dag/roots", "")
	if body := w.Body.String(); strings.Contains(body, "truncated") {
		t.Errorf("response advertises truncated for a complete result: %s", body)
	}
}
