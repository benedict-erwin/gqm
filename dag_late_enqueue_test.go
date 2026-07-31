package gqm

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// A dependent is normally enqueued while its parent is still pending, and the
// parent's completion is what promotes it. But nothing stops an application
// from enqueuing the child after the parent has already finished — and that is
// not an exotic case: "enqueue the parent, then enqueue what depends on it" is
// the ordinary way to build a chain, and the window widens the faster the
// system runs.
//
// Before this was fixed the child was orphaned. The worker read the parent's
// :dependents set at completion time, found it empty because the child had not
// been written yet, and deleted it. The child then recreated a set nobody would
// ever read again, and sat in "deferred" forever: no error, no log, no
// dead-letter entry. In a soak run 73 jobs vanished this way.
//
// The three terminal states are tested separately because they take different
// paths — success promotes, failure cascades a cancellation — and a fix that
// only handles the happy path leaves two holes open.

type lateEnqueueFixture struct {
	server *Server
	client *Client
	rc     *RedisClient
	prefix string
	cancel context.CancelFunc
}

func newLateEnqueueFixture(t *testing.T, handlers map[string]Handler) *lateEnqueueFixture {
	t.Helper()
	skipWithoutRedis(t)

	prefix := fmt.Sprintf("gqm:daglate:%s:%d:", t.Name(), time.Now().UnixNano())
	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithLogLevel("error"),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	for jt, h := range handlers {
		if err := server.Handle(jt, h, Workers(2)); err != nil {
			t.Fatalf("Handle(%s): %v", jt, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	go func() { _ = server.Start(ctx) }()
	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		cancel()
		t.Fatalf("NewClient: %v", err)
	}
	rc, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		cancel()
		t.Fatalf("NewRedisClient: %v", err)
	}

	f := &lateEnqueueFixture{server: server, client: client, rc: rc, prefix: prefix, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		client.Close()
		rc.Close()
		stressCleanup(t, prefix)
	})
	return f
}

func (f *lateEnqueueFixture) status(t *testing.T, jobID string) string {
	t.Helper()
	s, err := f.rc.rdb.HGet(context.Background(), f.rc.Key("job", jobID), "status").Result()
	if err != nil {
		t.Fatalf("reading status of %s: %v", jobID, err)
	}
	return s
}

// The common case: the parent succeeded before the child was enqueued. The
// child must still run.
func TestDAGLateEnqueue_ParentAlreadyCompleted(t *testing.T) {
	parentDone := make(chan struct{}, 1)
	childRan := make(chan struct{}, 1)

	f := newLateEnqueueFixture(t, map[string]Handler{
		"late.parent": func(ctx context.Context, job *Job) error { parentDone <- struct{}{}; return nil },
		"late.child":  func(ctx context.Context, job *Job) error { childRan <- struct{}{}; return nil },
	})

	parent, err := f.client.Enqueue(context.Background(), "late.parent", nil, Queue("late.parent"))
	if err != nil {
		t.Fatalf("enqueue parent: %v", err)
	}
	select {
	case <-parentDone:
	case <-time.After(15 * time.Second):
		t.Fatal("parent never ran")
	}
	// Let the completion path finish, including the deletion of :dependents.
	time.Sleep(1200 * time.Millisecond)
	if got := f.status(t, parent.ID); got != "completed" {
		t.Fatalf("parent status = %q, want completed before enqueuing the child", got)
	}

	child, err := f.client.Enqueue(context.Background(), "late.child", nil,
		Queue("late.child"), DependsOn(parent.ID))
	if err != nil {
		t.Fatalf("enqueue child: %v", err)
	}

	select {
	case <-childRan:
	case <-time.After(15 * time.Second):
		t.Fatalf("child %s never ran; status = %q — a dependency on a finished parent was orphaned",
			child.ID, f.status(t, child.ID))
	}
}

// The parent was dead-lettered before the child was enqueued. The child must
// be canceled, exactly as it would be had it been waiting at the time.
func TestDAGLateEnqueue_ParentAlreadyDeadLettered(t *testing.T) {
	parentTried := make(chan struct{}, 4)
	childRan := make(chan struct{}, 1)

	f := newLateEnqueueFixture(t, map[string]Handler{
		"late.parent": func(ctx context.Context, job *Job) error {
			parentTried <- struct{}{}
			return fmt.Errorf("permanent failure: %w", ErrSkipRetry)
		},
		"late.child": func(ctx context.Context, job *Job) error { childRan <- struct{}{}; return nil },
	})

	parent, err := f.client.Enqueue(context.Background(), "late.parent", nil,
		Queue("late.parent"), MaxRetry(0))
	if err != nil {
		t.Fatalf("enqueue parent: %v", err)
	}
	select {
	case <-parentTried:
	case <-time.After(15 * time.Second):
		t.Fatal("parent never ran")
	}
	time.Sleep(1500 * time.Millisecond)
	if got := f.status(t, parent.ID); got != "dead_letter" {
		t.Fatalf("parent status = %q, want dead_letter", got)
	}

	child, err := f.client.Enqueue(context.Background(), "late.child", nil,
		Queue("late.child"), DependsOn(parent.ID))
	if err != nil {
		t.Fatalf("enqueue child: %v", err)
	}

	select {
	case <-childRan:
		t.Error("child ran even though its parent had been dead-lettered")
	case <-time.After(4 * time.Second):
	}

	// Cancelled is the correct resting state. Left as "deferred" it is a leak:
	// nothing will ever resolve it, and no operator will ever see it.
	if got := f.status(t, child.ID); got != "canceled" {
		t.Errorf("child status = %q, want canceled — a dependent of a dead-lettered parent must not sit deferred forever", got)
	}
}

// AllowFailure inverts the rule: the child is meant to run whatever became of
// the parent, so a parent that had already failed must still release it.
func TestDAGLateEnqueue_ParentAlreadyFailedWithAllowFailure(t *testing.T) {
	parentTried := make(chan struct{}, 4)
	childRan := make(chan struct{}, 1)

	f := newLateEnqueueFixture(t, map[string]Handler{
		"late.parent": func(ctx context.Context, job *Job) error {
			parentTried <- struct{}{}
			return fmt.Errorf("permanent failure: %w", ErrSkipRetry)
		},
		"late.child": func(ctx context.Context, job *Job) error { childRan <- struct{}{}; return nil },
	})

	parent, err := f.client.Enqueue(context.Background(), "late.parent", nil,
		Queue("late.parent"), MaxRetry(0))
	if err != nil {
		t.Fatalf("enqueue parent: %v", err)
	}
	select {
	case <-parentTried:
	case <-time.After(15 * time.Second):
		t.Fatal("parent never ran")
	}
	time.Sleep(1500 * time.Millisecond)

	child, err := f.client.Enqueue(context.Background(), "late.child", nil,
		Queue("late.child"), DependsOn(parent.ID), AllowFailure(true))
	if err != nil {
		t.Fatalf("enqueue child: %v", err)
	}

	select {
	case <-childRan:
	case <-time.After(15 * time.Second):
		t.Fatalf("child %s never ran; status = %q — AllowFailure should release it despite the parent failing",
			child.ID, f.status(t, child.ID))
	}
}

// The ordinary path must keep working: a child enqueued while its parent is
// still running is promoted by the parent's completion, and must not be
// promoted early or run twice by the new enqueue-time check.
func TestDAGLateEnqueue_ParentStillRunningIsUnaffected(t *testing.T) {
	release := make(chan struct{})
	childRuns := make(chan string, 4)

	f := newLateEnqueueFixture(t, map[string]Handler{
		"late.parent": func(ctx context.Context, job *Job) error { <-release; return nil },
		"late.child":  func(ctx context.Context, job *Job) error { childRuns <- job.ID; return nil },
	})

	parent, err := f.client.Enqueue(context.Background(), "late.parent", nil, Queue("late.parent"))
	if err != nil {
		t.Fatalf("enqueue parent: %v", err)
	}
	time.Sleep(800 * time.Millisecond) // parent is now occupied in its handler

	child, err := f.client.Enqueue(context.Background(), "late.child", nil,
		Queue("late.child"), DependsOn(parent.ID))
	if err != nil {
		t.Fatalf("enqueue child: %v", err)
	}

	// It must not have been promoted while the parent is still running.
	time.Sleep(1200 * time.Millisecond)
	select {
	case id := <-childRuns:
		t.Fatalf("child %s ran before its parent finished", id)
	default:
	}
	if got := f.status(t, child.ID); got != "deferred" {
		t.Errorf("child status = %q while parent is still running, want deferred", got)
	}

	close(release)
	select {
	case <-childRuns:
	case <-time.After(15 * time.Second):
		t.Fatal("child never ran after the parent completed")
	}

	// Exactly once: the enqueue-time check must not queue a second copy.
	select {
	case id := <-childRuns:
		t.Errorf("child ran a second time (%s) — resolution is not idempotent", id)
	case <-time.After(2 * time.Second):
	}
}
