package gqm

import (
	"strings"
	"testing"
)

// The queues block declares which queues exist; pools then order a subset of
// them to express priority. Nothing tied the two together, so a pool could name
// a queue that was never declared and nobody ever wrote to.
//
// That failure is quiet in the worst way. The pool starts, holds its workers,
// and polls a queue that stays empty forever. Nothing errors, nothing logs, and
// the dashboard shows a healthy pool doing nothing.

func TestValidate_PoolQueueMustBeDeclared(t *testing.T) {
	const cfg = `
redis:
  addr: "localhost:6379"
queues:
  - name: "q1"
  - name: "q2"
pools:
  - name: "p1"
    job_types: ["a.b"]
    queues: ["q5", "q1"]
`
	_, err := LoadConfig([]byte(cfg))
	if err == nil {
		t.Fatal("a pool listening on an undeclared queue was accepted; it would poll an empty queue forever")
	}
	for _, want := range []string{"p1", "q5"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name %q, so it does not say what to fix: %v", want, err)
		}
	}
}

func TestValidate_DeclaredPoolQueuesAreAccepted(t *testing.T) {
	const cfg = `
redis:
  addr: "localhost:6379"
queues:
  - name: "critical"
  - name: "low"
pools:
  - name: "p1"
    job_types: ["a.b"]
    queues: ["critical", "low"]
`
	if _, err := LoadConfig([]byte(cfg)); err != nil {
		t.Fatalf("a pool naming only declared queues was rejected: %v", err)
	}
}

// "default" is where a job with no Queue() lands and what a pool falls back to
// when it declares no queues. Requiring it to be spelled out would make every
// minimal config carry a line that says nothing.
func TestValidate_DefaultQueueNeedsNoDeclaration(t *testing.T) {
	t.Run("named explicitly by a pool", func(t *testing.T) {
		const cfg = `
redis:
  addr: "localhost:6379"
queues:
  - name: "critical"
pools:
  - name: "p1"
    job_types: ["a.b"]
    queues: ["critical", "default"]
`
		if _, err := LoadConfig([]byte(cfg)); err != nil {
			t.Fatalf("default had to be declared: %v", err)
		}
	})

	t.Run("no queues block at all", func(t *testing.T) {
		const cfg = `
redis:
  addr: "localhost:6379"
pools:
  - name: "p1"
    job_types: ["a.b"]
`
		if _, err := LoadConfig([]byte(cfg)); err != nil {
			t.Fatalf("a pool with no queues and no queues block was rejected: %v", err)
		}
	})
}

// queues.priority never did anything: it was parsed, never read, and never
// affected dispatch. Priority comes from the order of pools.queues. Leaving a
// field that looks like it works is worse than not having it, so it is gone —
// and because unknown keys are now rejected, a config still carrying it says so
// plainly instead of being ignored twice over.
func TestValidate_QueuePriorityFieldIsGone(t *testing.T) {
	const cfg = `
redis:
  addr: "localhost:6379"
queues:
  - name: "q1"
    priority: 10
`
	_, err := LoadConfig([]byte(cfg))
	if err == nil {
		t.Fatal("queues[].priority was accepted; it should no longer be part of the schema")
	}
	if !strings.Contains(err.Error(), "priority") {
		t.Errorf("error does not name the removed field: %v", err)
	}
}

// An unset dequeue_strategy resolves to weighted. The distinction only matters
// for a pool reading more than one queue, and there it matters a great deal:
// under strict, a queue whose predecessor never runs dry is never read at all.
// Measured with two full queues and one worker, the second queue's first job
// came 301st under strict and 1st under weighted.
//
// Someone who does not set a strategy has not asked for starvation; they have
// not thought about it. The default should be the one that keeps every queue
// moving.
func TestConfig_DefaultDequeueStrategyIsWeighted(t *testing.T) {
	const cfg = `
redis:
  addr: "localhost:6379"
queues:
  - name: "critical"
  - name: "low"
pools:
  - name: "p1"
    job_types: ["a.b"]
    queues: ["critical", "low"]
`
	c, err := LoadConfig([]byte(cfg))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.Pools[0].DequeueStrategy != "" {
		t.Fatalf("fixture should leave the strategy unset, got %q", c.Pools[0].DequeueStrategy)
	}

	internal := PoolConfig{
		Name:     c.Pools[0].Name,
		JobTypes: c.Pools[0].JobTypes,
		Queues:   c.Pools[0].Queues,
	}.toInternal(0)

	if internal.dequeueStrategy != StrategyWeighted {
		t.Errorf("unset strategy resolved to %q, want %q", internal.dequeueStrategy, StrategyWeighted)
	}
}
