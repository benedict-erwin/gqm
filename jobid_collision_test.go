package gqm

import (
	"errors"
	"strings"
	"testing"
)

// A job owns a bare Redis key and suffixed ones: the hash at "<prefix>job:<id>"
// alongside "<prefix>job:<id>:deps", ":pending_deps" and ":dependents". Because
// GQM joins key segments with a colon, a job id containing one can be spelled
// to land on another job's DAG metadata. The two hold different Redis types, so
// the victim's enqueue fails with WRONGTYPE — a targeted denial of service
// against one job id, and via ":dependents" against every child of a parent.
//
// The old comment in helpers.go called this theoretical. It was reproduced
// through Client.Enqueue, which is why these tests exist.

func TestValidateJobInputs_ColonInJobIDRejected(t *testing.T) {
	// The first two are the reproduced attacks; the rest close the family.
	for _, id := range []string{
		"order-42:deps",
		"order-42:dependents",
		"order-42:pending_deps",
		"tenant-a:order-42",
		":leading",
		"trailing:",
		"a:b:c",
	} {
		t.Run(id, func(t *testing.T) {
			err := validateJobInputs(&Job{ID: id, Type: "test.job"})
			if !errors.Is(err, ErrInvalidJobID) {
				t.Errorf("id %q accepted (err = %v), want ErrInvalidJobID", id, err)
			}
		})
	}
}

// The rejection has to explain itself. An id that looks perfectly ordinary to
// the caller is being refused, so "invalid characters" alone would send them
// hunting.
func TestValidateJobInputs_ColonRejectionExplainsWhy(t *testing.T) {
	err := validateJobInputs(&Job{ID: "order-42:deps", Type: "test.job"})
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	for _, want := range []string{"colon", "separator"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message does not mention %q: %s", want, msg)
		}
	}
}

// A dependency id names another job, so it is held to the same rule: it could
// only ever refer to a job that cannot be enqueued.
func TestValidateJobInputs_ColonInDependsOnRejected(t *testing.T) {
	err := validateJobInputs(&Job{
		ID:        "child-1",
		Type:      "test.job",
		DependsOn: []string{"parent-1", "parent-2:dependents"},
	})
	if !errors.Is(err, ErrInvalidJobID) {
		t.Errorf("DependsOn with a colon accepted (err = %v), want ErrInvalidJobID", err)
	}
}

// Queue names and job types must keep their colons. The "namespace:action"
// convention is load-bearing — implicit pools derive a queue name from the job
// type — and neither owns a bare key that a suffixed one could collide with.
func TestValidateJobInputs_ColonStillAllowedInQueueAndType(t *testing.T) {
	for _, tc := range []struct {
		name string
		job  Job
	}{
		{"job type with colon", Job{ID: "job-1", Type: "email:send"}},
		{"queue with colon", Job{ID: "job-2", Type: "test.job", Queue: "email:send"}},
		{"both", Job{ID: "job-3", Type: "report:generate", Queue: "report:generate"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateJobInputs(&tc.job); err != nil {
				t.Errorf("rejected %+v: %v", tc.job, err)
			}
		})
	}
}

// Ordinary ids must keep working, which is what catches an over-tight pattern.
func TestValidateJobInputs_OrdinaryJobIDsAccepted(t *testing.T) {
	for _, id := range []string{
		"order-42",
		"01912d3f-8b7a-7c3e-9f21-2a4b6c8d0e1f", // uuid v7, the generated form
		"txn_00123",
		"batch.2026.07.30",
		"UPPER-and-lower-123",
	} {
		t.Run(id, func(t *testing.T) {
			if err := validateJobInputs(&Job{ID: id, Type: "test.job"}); err != nil {
				t.Errorf("rejected ordinary id %q: %v", id, err)
			}
		})
	}
}

// The generated ids must satisfy the rule they are validated against, or every
// enqueue that does not set an id explicitly would fail.
func TestValidateJobInputs_GeneratedIDsPassTheRule(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := NewUUID()
		if !safeJobIDRe.MatchString(id) {
			t.Fatalf("generated id %q does not match the job id rule", id)
		}
	}
}
