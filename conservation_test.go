package gqm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// A silent break is defined by the absence of a signal: a job that stops
// existing without an error, a log line, or a dead-letter entry. Counting
// successes cannot find one, because the missing job was never counted. The
// only thing that finds it is accounting for every job that was enqueued.
//
// This is not hypothetical here. A dependent enqueued after its parent had
// already finished used to sit in "deferred" forever — no error, no log, no
// DLQ — and the window widened as the system got faster. Seventy-three jobs
// vanished that way in a soak run. The stress suite did not catch it, and could
// not have: DependsOn appears zero times in stress_test.go, and so does every
// scheduler API. The two features least covered under load are the two that
// fail most quietly.
//
// So this raises load in steps and, at every step, insists that every job
// reached a terminal state that matches what was asked of it. The output that
// matters is not throughput; it is "the accounts balanced up to level N".
//
//	go test -run TestStress_EscalatingConservation          # fast, every push
//	GQM_STRESS_TEST=1 go test -run TestStress_EscalatingConservation  # deep

type consKind string

const (
	consFlat    consKind = "flat"
	consChain   consKind = "dag-chain"
	consDelayed consKind = "delayed"
	consFailing consKind = "failing"
)

// What a single enqueued job is required to end up as.
type consExpect struct {
	terminal string
	kind     consKind
	detail   string // e.g. "stage 2 of 3", to make a failure locatable
}

// One rung of the ladder. A chain contributes three jobs, not one.
type consMix struct {
	flat    int
	chains  int
	delayed int
	failing int
}

func (m consMix) scaled(f int) consMix {
	return consMix{m.flat * f, m.chains * f, m.delayed * f, m.failing * f}
}

func (m consMix) jobs() int { return m.flat + m.chains*consChainLen + m.delayed + m.failing }

const consChainLen = 3

func TestStress_EscalatingConservation(t *testing.T) {
	skipWithoutRedis(t)

	base := consMix{flat: 200, chains: 40, delayed: 40, failing: 40}
	// Fast mode stays around fifteen seconds so it can run on every push; a
	// conservation check that only runs weekly lets a silent break sit in the
	// tree for six days. Deep mode climbs until each level is an order of
	// magnitude past anything the rest of the suite exercises.
	factors := []int{1, 2, 4}
	if os.Getenv("GQM_STRESS_TEST") == "1" {
		factors = []int{1, 2, 4, 8, 16, 32, 64, 128}
	}

	// Measured once, before any server exists, so later levels are compared
	// against a stable floor rather than against each other.
	runtime.GC()
	baseline := runtime.NumGoroutine()

	for _, f := range factors {
		mix := base.scaled(f)
		name := fmt.Sprintf("level_%dx_%d_jobs", f, mix.jobs())
		ok := t.Run(name, func(t *testing.T) {
			runConservationLevel(t, mix, baseline)
		})
		// Escalating past a level that already lost jobs only produces more
		// noise about the same defect, and the first rung that breaks is the
		// interesting one.
		if !ok {
			t.Fatalf("accounts stopped balancing at %s; not escalating further", name)
		}
	}
}

func runConservationLevel(t *testing.T, mix consMix, goroutineBaseline int) {
	prefix := fmt.Sprintf("gqm:cons:%d:", time.Now().UnixNano())
	t.Cleanup(func() { stressCleanup(t, prefix) })

	// Retention must not fire during the run. A completed job whose record has
	// already expired is indistinguishable from one that was never processed,
	// and that would turn this test into a source of false losses.
	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithLogLevel("error"),
		WithResultTTL(30*time.Minute),
		WithFailureTTL(30*time.Minute),
		WithSchedulerPollInterval(300*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// runs[jobID] counts executions. Anything above one is a duplicate, which
	// is a silent break in the other direction: the job is accounted for, but
	// its side effects happened twice.
	var runs sync.Map
	record := func(job *Job) {
		v, _ := runs.LoadOrStore(job.ID, new(atomic.Int64))
		v.(*atomic.Int64).Add(1)
	}

	var done atomic.Int64
	okHandler := func(ctx context.Context, job *Job) error {
		record(job)
		done.Add(1)
		return nil
	}
	for _, jt := range []string{"cons.flat", "cons.chain", "cons.delayed"} {
		if err := server.Handle(jt, okHandler, Workers(8)); err != nil {
			t.Fatalf("Handle(%s): %v", jt, err)
		}
	}
	var failed atomic.Int64
	if err := server.Handle("cons.fail", func(ctx context.Context, job *Job) error {
		record(job)
		failed.Add(1)
		return fmt.Errorf("deliberate terminal failure: %w", ErrSkipRetry)
	}, Workers(4)); err != nil {
		t.Fatalf("Handle(cons.fail): %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Start(ctx) }()
	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	expect := make(map[string]consExpect, mix.jobs())
	bg := context.Background()

	for i := 0; i < mix.flat; i++ {
		j, err := client.Enqueue(bg, "cons.flat", Payload{"i": i}, Queue("cons.flat"))
		if err != nil {
			t.Fatalf("enqueue flat %d: %v", i, err)
		}
		expect[j.ID] = consExpect{terminal: StatusCompleted, kind: consFlat}
	}

	// Chains are built a stage at a time — every stage-one job, then every
	// stage-two job naming its parent, and so on. This is how an application
	// batches work, and it is also the only ordering that opens the window the
	// orphan bug lived in: by the time a child is enqueued, its parent has
	// usually already finished.
	//
	// Building each chain end-to-end in one pass instead would keep every
	// parent still in flight, and the bug would be invisible. That is not a
	// hypothetical — this test was written that way first, stayed green with
	// the fix deliberately removed, and proved nothing at all.
	prevStage := make([]string, mix.chains)
	for stage := 0; stage < consChainLen; stage++ {
		before := done.Load()
		for i := 0; i < mix.chains; i++ {
			opts := []EnqueueOption{Queue("cons.chain")}
			if stage > 0 {
				opts = append(opts, DependsOn(prevStage[i]))
			}
			j, err := client.Enqueue(bg, "cons.chain", Payload{"chain": i, "stage": stage}, opts...)
			if err != nil {
				t.Fatalf("enqueue chain %d stage %d: %v", i, stage, err)
			}
			expect[j.ID] = consExpect{
				terminal: StatusCompleted,
				kind:     consChain,
				detail:   fmt.Sprintf("chain %d, stage %d of %d", i, stage+1, consChainLen),
			}
			prevStage[i] = j.ID
		}
		// Let this stage drain before naming it as a dependency, so the next
		// stage really is enqueued against finished parents.
		if stage < consChainLen-1 {
			consWaitForStage(&done, before+int64(mix.chains), 5*time.Second)
		}
	}

	for i := 0; i < mix.delayed; i++ {
		j, err := client.EnqueueIn(bg, 700*time.Millisecond, "cons.delayed", Payload{"i": i}, Queue("cons.delayed"))
		if err != nil {
			t.Fatalf("enqueue delayed %d: %v", i, err)
		}
		expect[j.ID] = consExpect{terminal: StatusCompleted, kind: consDelayed}
	}

	for i := 0; i < mix.failing; i++ {
		j, err := client.Enqueue(bg, "cons.fail", Payload{"i": i}, Queue("cons.fail"), MaxRetry(0))
		if err != nil {
			t.Fatalf("enqueue failing %d: %v", i, err)
		}
		expect[j.ID] = consExpect{terminal: StatusDeadLetter, kind: consFailing}
	}

	wantDone := int64(mix.flat + mix.chains*consChainLen + mix.delayed)
	wantFailed := int64(mix.failing)

	// A timeout here is not the verdict — the accounting below is. Reaching the
	// deadline only means the accounts are settled as far as they will settle,
	// and the point is to say precisely what is missing rather than "timed out".
	deadline := time.Now().Add(time.Duration(60+mix.jobs()/50) * time.Second)
	for time.Now().Before(deadline) {
		if done.Load() >= wantDone && failed.Load() >= wantFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Let terminal writes and dependency resolution land before reading.
	time.Sleep(1500 * time.Millisecond)

	rc, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewRedisClient: %v", err)
	}
	defer rc.Close()

	got := consReadStatuses(t, rc, expect)
	assertConservation(t, expect, got, &runs)
	assertNoOrphanDependencyKeys(t, rc)

	cancel()
	time.Sleep(700 * time.Millisecond)
	runtime.GC()
	if delta := runtime.NumGoroutine() - goroutineBaseline; delta > 40 {
		t.Errorf("goroutines did not return to baseline after the level drained: %d above the %d measured before any server started",
			delta, goroutineBaseline)
	}
}

// consWaitForStage waits until the completion counter reaches target, giving up
// after the deadline. Giving up is not an error here: if a stage does not drain,
// the next stage is enqueued against parents that are still running, which is
// the benign ordering. The accounting at the end is what decides the verdict.
func consWaitForStage(done *atomic.Int64, target int64, limit time.Duration) {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if done.Load() >= target {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// consReadStatuses reads the recorded status of every job that was enqueued.
// A job whose record is gone entirely is reported as "<missing>" rather than
// skipped: a vanished record is the loudest possible form of a silent loss, and
// silently dropping it here would reproduce the very bug being hunted.
func consReadStatuses(t *testing.T, rc *RedisClient, expect map[string]consExpect) map[string]string {
	t.Helper()

	ids := make([]string, 0, len(expect))
	for id := range expect {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	ctx := context.Background()
	got := make(map[string]string, len(ids))
	const chunk = 500
	for start := 0; start < len(ids); start += chunk {
		end := start + chunk
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		pipe := rc.rdb.Pipeline()
		cmds := make([]*redis.StringCmd, len(batch))
		for i, id := range batch {
			cmds[i] = pipe.HGet(ctx, rc.Key("job", id), "status")
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			t.Fatalf("reading job statuses: %v", err)
		}
		for i, id := range batch {
			s, err := cmds[i].Result()
			if err != nil {
				s = "<missing>"
			}
			got[id] = s
		}
	}
	return got
}

func assertConservation(t *testing.T, expect map[string]consExpect, got map[string]string, runs *sync.Map) {
	t.Helper()

	byKind := map[consKind]int{}
	var unresolved, wrongTerminal, duplicates []string

	for id, want := range expect {
		status := got[id]
		switch {
		case status == want.terminal:
			byKind[want.kind]++
		case isTerminalStatus(status):
			if len(wrongTerminal) < 10 {
				wrongTerminal = append(wrongTerminal, fmt.Sprintf("%s (%s%s): %s, want %s",
					id, want.kind, detailSuffix(want.detail), status, want.terminal))
			}
		default:
			// Not terminal after the queue drained: nothing will ever move it
			// again, and nothing anywhere will say so.
			if len(unresolved) < 10 {
				unresolved = append(unresolved, fmt.Sprintf("%s (%s%s): stuck in %q",
					id, want.kind, detailSuffix(want.detail), status))
			}
		}

		if v, loaded := runs.Load(id); loaded {
			if n := v.(*atomic.Int64).Load(); n > 1 && len(duplicates) < 10 {
				duplicates = append(duplicates, fmt.Sprintf("%s (%s) ran %d times", id, want.kind, n))
			}
		}
	}

	accounted := 0
	for _, n := range byKind {
		accounted += n
	}
	t.Logf("accounted %d/%d — flat %d, chains %d, delayed %d, failing %d",
		accounted, len(expect), byKind[consFlat], byKind[consChain], byKind[consDelayed], byKind[consFailing])

	if n := len(expect) - accounted; n > 0 {
		t.Errorf("%d of %d jobs did not reach the state they were promised", n, len(expect))
	}
	for _, line := range unresolved {
		t.Errorf("never resolved: %s", line)
	}
	for _, line := range wrongTerminal {
		t.Errorf("wrong terminal state: %s", line)
	}
	for _, line := range duplicates {
		t.Errorf("executed more than once: %s", line)
	}
}

func isTerminalStatus(s string) bool {
	switch s {
	case StatusCompleted, StatusDeadLetter, StatusCanceled, StatusStopped:
		return true
	}
	return false
}

func detailSuffix(d string) string {
	if d == "" {
		return ""
	}
	return ", " + d
}

// Dependency bookkeeping is created per job and deleted when the parent
// resolves it. Sets left behind after everything has drained are the residue of
// the orphan bug: a dependent wrote a set that no parent would ever read again.
// They leak memory quietly and, unlike a stuck job, do not even show up as a
// pending count.
func assertNoOrphanDependencyKeys(t *testing.T, rc *RedisClient) {
	t.Helper()

	ctx := context.Background()
	var leftovers []string
	for _, suffix := range []string{":dependents", ":pending_deps"} {
		var cursor uint64
		for {
			keys, next, err := rc.rdb.Scan(ctx, cursor, rc.Key("job", "*")+suffix, 500).Result()
			if err != nil {
				t.Fatalf("scanning for %s keys: %v", suffix, err)
			}
			leftovers = append(leftovers, keys...)
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	if len(leftovers) == 0 {
		return
	}
	sort.Strings(leftovers)
	shown := leftovers
	if len(shown) > 5 {
		shown = shown[:5]
	}
	t.Errorf("%d dependency key(s) survived the drain, e.g. %v — nothing will ever read these again",
		len(leftovers), shown)
}
