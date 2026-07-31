package gqm

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// An end-to-end simulation of a payment gateway, run as both a stress test and
// a benchmark.
//
// The nine tests in stress_test.go are synthetic: mw.fast, mw.slow, mw.fail,
// mw.panic. They prove the engine survives load, which is worth proving, but
// they cannot say anything about what happens when an application combines DAG
// chains, retries, dead-lettering, cron, delayed jobs, priorities and
// idempotency the way a real one does — all at once, against each other.
//
// What makes this more than a load generator is the invariants. A payment
// system has a property no synthetic workload has: the money has to add up.
// If a job is lost, run twice, or a DAG child runs before its parent, the
// ledger stops balancing and the test fails with a number rather than a vague
// sense that something is slow.
//
// Failure injection is deterministic by payment index rather than random. That
// is deliberate: it makes the expected ledger exactly computable, so the
// assertions are equalities instead of tolerances. A tolerance is where a real
// off-by-one goes to hide.
//
// Two modes:
//
//	GQM_STRESS_TEST=1   fast, ~10 seconds, run this regularly
//	GQM_SOAK=1          long, ~20 seconds at 4000 payments, finds what only
//	                    volume reveals
//
// Soak mode is what found gqm-6shx: dependents enqueued after their parent had
// already finished were orphaned forever. It stalled at roughly 3840 of 3920
// captures, and only at volume — the window widens the faster the system runs,
// so the fast mode never reached it.

// --- Domain -----------------------------------------------------------------

// Amounts are integer minor units (cents). Never float: the ledger invariant is
// a sum of thousands of values, and float rounding would produce a mismatch
// that looks like a bug in the queue and is really a bug in the test — the kind
// of false failure people learn to ignore.
type paymentOutcome int

const (
	outcomeSuccess      paymentOutcome = iota // authorises first try
	outcomeRetrySucceed                       // fails transiently, succeeds on retry
	outcomeDeclined                           // permanently declined: ErrSkipRetry -> DLQ
	outcomeTimeout                            // handler exceeds its job timeout
	outcomePanic                              // handler panics
)

// outcomeFor maps a payment index to an outcome. The proportions mirror a real
// gateway: most succeed, a minority fail transiently, a few are declined
// outright, and timeouts and panics are rare but not zero.
//
// Index-based rather than random so that a failing run can be reproduced and
// the expected totals can be computed in advance.
func outcomeFor(i int) paymentOutcome {
	switch {
	case i%200 == 7: // 0.5%
		return outcomePanic
	case i%100 == 13: // 1%
		return outcomeTimeout
	case i%50 == 21: // 2%
		return outcomeDeclined
	case i%25 < 2: // 8%
		return outcomeRetrySucceed
	default:
		return outcomeSuccess
	}
}

// paymentPayload builds a realistically shaped payload: nested objects, the
// metadata a gateway actually carries, roughly 1-2 KB serialised. Payload size
// dominates the memory cost of a retained job, so measuring with nil payloads
// would understate it by an order of magnitude.
func paymentPayload(i int, amountCents int64) Payload {
	return Payload{
		"payment_reference": fmt.Sprintf("pay_%012d", i),
		// Job.CreatedAt is stored in whole seconds, which is fine for the queue
		// but useless for measuring sub-second latency: it would carry up to a
		// second of systematic error, comfortably larger than the numbers being
		// measured. A real integration carries its own correlation timestamp,
		// so this does the same.
		"enqueued_at_ns":  time.Now().UnixNano(),
		"idempotency_key": fmt.Sprintf("idem_%012d", i),
		"amount": map[string]any{
			"value":    amountCents,
			"currency": "IDR",
			"exponent": 2,
		},
		"merchant": map[string]any{
			"id":            fmt.Sprintf("mch_%06d", i%500),
			"name":          "Toko Sejahtera Abadi Nusantara",
			"category_code": "5411",
			"country":       "ID",
			"settlement": map[string]any{
				"bank_code":      "014",
				"account_masked": "****4821",
				"schedule":       "T+2",
			},
		},
		"customer": map[string]any{
			"id":    fmt.Sprintf("cus_%08d", i%20000),
			"email": fmt.Sprintf("customer%08d@example.invalid", i%20000),
			"phone": "+62811" + fmt.Sprintf("%07d", i%10000000),
			"device": map[string]any{
				"ip":          fmt.Sprintf("10.%d.%d.%d", i%256, (i/256)%256, (i/65536)%256),
				"user_agent":  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15",
				"fingerprint": fmt.Sprintf("fp_%032x", i),
			},
		},
		"instrument": map[string]any{
			"type":        "card",
			"brand":       []string{"visa", "mastercard", "jcb"}[i%3],
			"last4":       fmt.Sprintf("%04d", i%10000),
			"exp_month":   (i % 12) + 1,
			"exp_year":    2028 + (i % 4),
			"issuer_bank": "Bank Central Asia",
			"country":     "ID",
			"three_d_secure": map[string]any{
				"enrolled": true,
				"version":  "2.2.0",
				"eci":      "05",
			},
		},
		"order": map[string]any{
			"id": fmt.Sprintf("ord_%012d", i),
			"items": []map[string]any{
				{"sku": "SKU-00123", "name": "Kopi Arabika Gayo 250g", "qty": 2, "unit_price": 8500000},
				{"sku": "SKU-00456", "name": "Gula Aren Cair 500ml", "qty": 1, "unit_price": 4500000},
			},
			"shipping": map[string]any{
				"method":   "regular",
				"address":  "Jl. Merdeka No. 45, RT 03 RW 07, Kelurahan Menteng",
				"city":     "Jakarta Pusat",
				"postcode": "10310",
			},
		},
	}
}

// --- Ledger -----------------------------------------------------------------

// ledger records what each stage actually did. Every assertion at the end reads
// from here rather than from queue statistics, because the point is to check
// the application's view of the world, not the queue's own bookkeeping.
type ledger struct {
	mu sync.Mutex

	authorized int64 // cents
	captured   int64
	settled    int64
	voided     int64 // declined, never captured

	authorizeAttempts map[string]int   // reference -> handler invocations
	captureAt         map[string]int64 // reference -> unix nano when captured
	authorizeAt       map[string]int64
	invoiced          map[string]bool
	notified          map[string]bool
	declined          map[string]bool
}

func newLedger() *ledger {
	return &ledger{
		authorizeAttempts: make(map[string]int),
		captureAt:         make(map[string]int64),
		authorizeAt:       make(map[string]int64),
		invoiced:          make(map[string]bool),
		notified:          make(map[string]bool),
		declined:          make(map[string]bool),
	}
}

func (l *ledger) authorize(ref string, cents int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.authorized += cents
	l.authorizeAt[ref] = time.Now().UnixNano()
}

func (l *ledger) attempt(ref string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.authorizeAttempts[ref]++
	return l.authorizeAttempts[ref]
}

func (l *ledger) capture(ref string, cents int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.captured += cents
	l.captureAt[ref] = time.Now().UnixNano()
}

func (l *ledger) settle(cents int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.settled += cents
}

func (l *ledger) void(ref string, cents int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.voided += cents
	l.declined[ref] = true
}

func (l *ledger) invoice(ref string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.invoiced[ref] = true
}

func (l *ledger) notify(ref string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.notified[ref] = true
}

// --- Latency ----------------------------------------------------------------

type latencyRecorder struct {
	mu sync.Mutex
	d  map[string][]time.Duration
}

func newLatencyRecorder() *latencyRecorder {
	return &latencyRecorder{d: make(map[string][]time.Duration)}
}

func (r *latencyRecorder) record(stage string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.d[stage] = append(r.d[stage], d)
}

func (r *latencyRecorder) report(t testingLogger) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stages := make([]string, 0, len(r.d))
	for s := range r.d {
		stages = append(stages, s)
	}
	sort.Strings(stages)

	t.Logf("%-28s %7s %10s %10s %10s %10s", "stage", "n", "p50", "p95", "p99", "max")
	for _, s := range stages {
		v := append([]time.Duration(nil), r.d[s]...)
		if len(v) == 0 {
			continue
		}
		sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
		t.Logf("%-28s %7d %10s %10s %10s %10s", s, len(v),
			e2ePct(v, 50).Round(time.Millisecond),
			e2ePct(v, 95).Round(time.Millisecond),
			e2ePct(v, 99).Round(time.Millisecond),
			v[len(v)-1].Round(time.Millisecond))
	}
}

// pct returns the p-th percentile using nearest-rank, which for these sample
// sizes is honest about being a sample rather than pretending to interpolate.
func e2ePct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p*len(sorted) + 99) / 100
	if idx < 1 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

type testingLogger interface{ Logf(string, ...any) }

// --- The simulation ---------------------------------------------------------

type e2eScale struct {
	name        string
	payments    int
	concurrency int
	deadline    time.Duration
}

func e2eScaleFor(t *testing.T) e2eScale {
	t.Helper()
	if os.Getenv("GQM_SOAK") == "1" {
		skipWithoutRedis(t)
		return e2eScale{name: "soak", payments: 4000, concurrency: 32, deadline: 18 * time.Minute}
	}
	skipWithoutStressFlag(t)
	return e2eScale{name: "fast", payments: 300, concurrency: 16, deadline: 3 * time.Minute}
}

func TestE2E_PaymentGateway(t *testing.T) {
	scale := e2eScaleFor(t)
	prefix := fmt.Sprintf("gqm:e2e:pay:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	l := newLedger()
	lat := newLatencyRecorder()
	var enqueued atomic.Int64
	var notifyFailures atomic.Int64
	var reconRuns atomic.Int64

	// Retention long enough that nothing expires while the assertions run. A
	// soak run produces tens of thousands of terminal jobs, and the default
	// 7-day window is fine, but being explicit here documents that the test
	// must not race the reaper.
	// The pool must be at least as large as the total worker concurrency:
	// every worker blocked on a dequeue holds a connection for the duration.
	// go-redis defaults to 10*GOMAXPROCS, which on a 4-CPU box is 40 — fewer
	// than the 52 workers configured below, so the default would violate the
	// project's own sizing rule. Sized explicitly here for that reason alone:
	// it was tested as a cause of the soak stall and ruled out.
	rdb := redis.NewClient(&redis.Options{
		Addr:     testRedisAddr(),
		PoolSize: 128,
	})
	t.Cleanup(func() { rdb.Close() })

	server, err := NewServer(
		WithServerRedisOpts(WithRedisClient(rdb), WithPrefix(prefix)),
		WithGlobalTimeout(20*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(30*time.Second),
		WithResultTTL(TTLPermanent),
		WithFailureTTL(TTLPermanent),
		WithLogLevel("error"),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Pools mirror how a real deployment separates concerns: money-critical
	// work gets its own isolated pool so an analytics backlog cannot starve a
	// refund, and the queues are ordered by priority within each pool.
	mustPool(t, server, PoolConfig{
		Name:            "payments",
		JobTypes:        []string{"payment.authorize", "payment.capture", "payment.settle"},
		Queues:          []string{"payments.critical", "payments.default"},
		Concurrency:     scale.concurrency,
		DequeueStrategy: StrategyStrict,
		JobTimeout:      8 * time.Second,
	})
	mustPool(t, server, PoolConfig{
		Name:        "billing",
		JobTypes:    []string{"invoice.generate", "refund.process"},
		Queues:      []string{"payments.critical", "billing.default"},
		Concurrency: 8,
	})
	mustPool(t, server, PoolConfig{
		Name:        "notifications",
		JobTypes:    []string{"notification.email", "notification.webhook"},
		Queues:      []string{"notifications.default"},
		Concurrency: 8,
	})
	mustPool(t, server, PoolConfig{
		Name:        "background",
		JobTypes:    []string{"*"},
		Queues:      []string{"background.default"},
		Concurrency: 4,
	})

	// --- Handlers ---

	// Authorization talks to a flaky issuer. Transient failures return a plain
	// error so the retry policy applies; a decline returns ErrSkipRetry, which
	// must send it straight to the dead-letter queue without burning retries.
	mustHandle(t, server, "payment.authorize", func(ctx context.Context, job *Job) error {
		ref, cents, idx := paymentFields(job)
		attempt := l.attempt(ref)

		switch outcomeFor(idx) {
		case outcomeDeclined:
			l.void(ref, cents)
			return fmt.Errorf("issuer declined card: %w", ErrSkipRetry)
		case outcomeTimeout:
			// Exceeds the pool's 8s job timeout on the first attempt only, so
			// the retry path is exercised rather than the job being stuck.
			if attempt == 1 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(12 * time.Second):
				}
			}
		case outcomePanic:
			if attempt == 1 {
				panic("simulated issuer client panic")
			}
		case outcomeRetrySucceed:
			if attempt == 1 {
				return fmt.Errorf("issuer temporarily unavailable")
			}
		}

		lat.record("payment.authorize", sinceEnqueue(job))
		l.authorize(ref, cents)
		return nil
	})

	mustHandle(t, server, "payment.capture", func(ctx context.Context, job *Job) error {
		ref, cents, _ := paymentFields(job)
		lat.record("payment.capture", sinceEnqueue(job))
		l.capture(ref, cents)
		return nil
	})

	mustHandle(t, server, "payment.settle", func(ctx context.Context, job *Job) error {
		_, cents, _ := paymentFields(job)
		lat.record("payment.settle", sinceEnqueue(job))
		l.settle(cents)
		return nil
	})

	mustHandle(t, server, "invoice.generate", func(ctx context.Context, job *Job) error {
		ref, _, _ := paymentFields(job)
		// The last money-path stage, so its age is the whole chain's latency:
		// authorize -> capture -> settle -> invoice. This is the number an
		// operator is actually asked about.
		lat.record("CHAIN authorize..invoice", sinceEnqueue(job))
		l.invoice(ref)
		return nil
	})

	// Notifications fail often and must never hold up the money path. They are
	// enqueued with AllowFailure(true), so a failure here must not cancel
	// anything upstream — that is one of the invariants checked at the end.
	mustHandle(t, server, "notification.email", func(ctx context.Context, job *Job) error {
		ref, _, idx := paymentFields(job)
		if idx%20 < 3 { // 15%
			notifyFailures.Add(1)
			return fmt.Errorf("smtp upstream refused: %w", ErrSkipRetry)
		}
		lat.record("notification.email", sinceEnqueue(job))
		l.notify(ref)
		return nil
	})

	mustHandle(t, server, "refund.process", func(ctx context.Context, job *Job) error {
		return nil
	})

	mustHandle(t, server, "reconciliation.daily", func(ctx context.Context, job *Job) error {
		// Deliberately not timed. A cron job is enqueued by the scheduler at
		// fire time and carries no correlation timestamp, so measuring it the
		// way the payment chain is measured would report 0s — which reads as
		// "no lag" when it means "not measured". That the entry fires at all
		// is asserted below.
		reconRuns.Add(1)
		return nil
	})

	mustHandle(t, server, "analytics.aggregate", func(ctx context.Context, job *Job) error {
		return nil
	})

	// A cron entry that fires every second, so a run of this length actually
	// exercises the scheduler rather than merely configuring it.
	if err := server.Schedule(CronEntry{
		ID:       "recon-daily",
		Name:     "Settlement reconciliation",
		CronExpr: "* * * * * *",
		JobType:  "reconciliation.daily",
		Queue:    "background.default",
		Enabled:  true,
	}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), scale.deadline)
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		if err := server.Start(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("server.Start: %v", err)
		}
	}()
	time.Sleep(700 * time.Millisecond) // let pools come up

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	// --- Drive the workload ---

	t.Logf("mode=%s payments=%d concurrency=%d", scale.name, scale.payments, scale.concurrency)
	start := time.Now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, scale.concurrency)
	for i := 0; i < scale.payments; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := enqueuePaymentChain(context.Background(), client, i, &enqueued); err != nil {
				t.Errorf("payment %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	enqueueDone := time.Since(start)

	// --- Wait for the system to quiesce ---

	expectSettled := 0
	for i := 0; i < scale.payments; i++ {
		if outcomeFor(i) != outcomeDeclined {
			expectSettled++
		}
	}

	deadline := time.Now().Add(scale.deadline - time.Since(start) - 20*time.Second)
	var lastCaptured int
	stable := 0
	for time.Now().Before(deadline) {
		l.mu.Lock()
		captured := len(l.captureAt)
		l.mu.Unlock()

		if captured >= expectSettled {
			break
		}
		if captured == lastCaptured {
			stable++
			// Nothing has moved for 30 consecutive checks. Waiting longer will
			// not help, and failing here with real numbers beats timing out
			// with none.
			if stable > 120 {
				t.Logf("progress stalled at %d/%d captured", captured, expectSettled)
				break
			}
		} else {
			stable = 0
			lastCaptured = captured
		}
		time.Sleep(500 * time.Millisecond)
	}
	elapsed := time.Since(start)

	cancel()
	serverWg.Wait()

	// --- Invariants ---

	l.mu.Lock()
	authorized, captured, settled, voided := l.authorized, l.captured, l.settled, l.voided
	nCaptured, nInvoiced, nNotified, nDeclined := len(l.captureAt), len(l.invoiced), len(l.notified), len(l.declined)
	authAt := make(map[string]int64, len(l.authorizeAt))
	for k, v := range l.authorizeAt {
		authAt[k] = v
	}
	capAt := make(map[string]int64, len(l.captureAt))
	for k, v := range l.captureAt {
		capAt[k] = v
	}
	attempts := make(map[string]int, len(l.authorizeAttempts))
	for k, v := range l.authorizeAttempts {
		attempts[k] = v
	}
	l.mu.Unlock()

	var expectAuthorized, expectVoided int64
	for i := 0; i < scale.payments; i++ {
		cents := amountFor(i)
		if outcomeFor(i) == outcomeDeclined {
			expectVoided += cents
		} else {
			expectAuthorized += cents
		}
	}

	t.Logf("enqueue of %d chains took %s; workload settled in %s", scale.payments, enqueueDone.Round(time.Millisecond), elapsed.Round(time.Millisecond))
	t.Logf("ledger: authorized=%d captured=%d settled=%d voided=%d (cents)", authorized, captured, settled, voided)
	t.Logf("counts: captured=%d invoiced=%d notified=%d declined=%d | notify failures=%d | recon runs=%d",
		nCaptured, nInvoiced, nNotified, nDeclined, notifyFailures.Load(), reconRuns.Load())
	t.Logf("throughput: %.0f payments/sec end-to-end", float64(nCaptured)/elapsed.Seconds())
	lat.report(t)

	// 1. The money adds up. A lost job, a duplicate, or a child that ran before
	//    its parent all show up here as a mismatch.
	if authorized != expectAuthorized {
		t.Errorf("authorized = %d cents, want %d — a job was lost or ran twice", authorized, expectAuthorized)
	}
	if captured != authorized {
		t.Errorf("captured = %d cents but authorized = %d — capture and authorize disagree", captured, authorized)
	}
	if settled != captured {
		t.Errorf("settled = %d cents but captured = %d", settled, captured)
	}
	if voided != expectVoided {
		t.Errorf("voided = %d cents, want %d", voided, expectVoided)
	}

	// 2. Every payment reached exactly one terminal outcome: captured or voided.
	if got := nCaptured + nDeclined; got != scale.payments {
		t.Errorf("%d payments captured + %d declined = %d, want %d — some payment reached neither outcome",
			nCaptured, nDeclined, got, scale.payments)
	}

	// 3. A DAG child never ran before its parent. Checked per payment against
	//    recorded wall-clock, not inferred from completion counts.
	violations := 0
	for ref, capturedAt := range capAt {
		authorizedAt, ok := authAt[ref]
		if !ok {
			t.Errorf("%s was captured but never authorized — DAG ordering broken", ref)
			violations++
			continue
		}
		if capturedAt < authorizedAt {
			violations++
			if violations <= 5 {
				t.Errorf("%s captured %dns before it was authorized", ref, authorizedAt-capturedAt)
			}
		}
	}
	if violations > 5 {
		t.Errorf("... and %d more DAG ordering violations", violations-5)
	}

	// 4. Retries stayed inside their budget. maxRetryAuthorize+1 is the most
	//    times a handler may be invoked for one reference.
	for ref, n := range attempts {
		if n > maxRetryAuthorize+1 {
			t.Errorf("%s: authorize ran %d times, budget is %d", ref, n, maxRetryAuthorize+1)
		}
	}

	// 5. AllowFailure held: notifications failed often, and the money path
	//    completed anyway. Without it, a failing notification would cascade a
	//    cancellation back up the chain.
	if notifyFailures.Load() == 0 {
		t.Error("no notification failures were injected; the AllowFailure path went untested")
	}
	if nInvoiced != nCaptured {
		t.Errorf("invoiced=%d but captured=%d — a failing notification took the chain with it", nInvoiced, nCaptured)
	}

	// 6. The scheduler actually ran while all this was going on.
	if reconRuns.Load() == 0 {
		t.Error("cron entry never fired during the run")
	}
}

// Idempotency is the property a payment system cannot do without: a retried
// webhook must not charge the customer twice. Unique() is what provides it, and
// this drives it the way it actually breaks — the same reference arriving many
// times at once, not one after another.
func TestE2E_PaymentIdempotencyUnderContention(t *testing.T) {
	skipWithoutStressFlag(t)

	prefix := fmt.Sprintf("gqm:e2e:idem:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	const (
		references = 50
		duplicates = 20 // concurrent deliveries of each reference
	)

	var executions sync.Map // reference -> *atomic.Int64
	var totalRuns atomic.Int64

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithLogLevel("error"),
		WithResultTTL(TTLPermanent),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	mustHandle(t, server, "webhook.payment", func(ctx context.Context, job *Job) error {
		ref, _ := job.Payload["payment_reference"].(string)
		v, _ := executions.LoadOrStore(ref, &atomic.Int64{})
		v.(*atomic.Int64).Add(1)
		totalRuns.Add(1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() { defer serverWg.Done(); _ = server.Start(ctx) }()
	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	var accepted, rejected atomic.Int64
	for i := 0; i < references; i++ {
		ref := fmt.Sprintf("pay_dup_%06d", i)
		for d := 0; d < duplicates; d++ {
			wg.Add(1)
			go func(ref string) {
				defer wg.Done()
				_, err := client.Enqueue(context.Background(), "webhook.payment",
					Payload{"payment_reference": ref, "amount": 125000},
					JobID(ref), Unique())
				if err != nil {
					rejected.Add(1) // duplicate rejection is the expected path
					return
				}
				accepted.Add(1)
			}(ref)
		}
	}
	wg.Wait()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && totalRuns.Load() < int64(references) {
		time.Sleep(200 * time.Millisecond)
	}
	cancel()
	serverWg.Wait()

	t.Logf("%d references x %d concurrent deliveries: accepted=%d rejected=%d executed=%d",
		references, duplicates, accepted.Load(), rejected.Load(), totalRuns.Load())

	if accepted.Load() != references {
		t.Errorf("accepted %d enqueues, want exactly %d — Unique() let a duplicate through",
			accepted.Load(), references)
	}
	over := 0
	for i := 0; i < references; i++ {
		ref := fmt.Sprintf("pay_dup_%06d", i)
		v, ok := executions.Load(ref)
		if !ok {
			t.Errorf("%s never executed", ref)
			continue
		}
		if n := v.(*atomic.Int64).Load(); n != 1 {
			over++
			if over <= 5 {
				t.Errorf("%s executed %d times, want exactly 1 — the customer was charged twice", ref, n)
			}
		}
	}
	if over > 5 {
		t.Errorf("... and %d more references executed more than once", over-5)
	}
}

// --- Benchmark --------------------------------------------------------------

// BenchmarkE2E_PaymentChain measures a whole authorize -> capture -> settle ->
// invoice chain, not a single enqueue. ns/op here is the cost of moving one
// payment all the way through, which is the number an operator actually cares
// about; BenchmarkEndToEnd's figure covers one hop.
func BenchmarkE2E_PaymentChain(b *testing.B) {
	if os.Getenv("GQM_TEST_REDIS_ADDR") == "" && testRedisAddr() == "" {
		b.Skip("requires Redis")
	}
	prefix := fmt.Sprintf("gqm:e2e:bench:%d:", time.Now().UnixNano())

	var completed atomic.Int64
	done := make(chan struct{}, b.N+16)

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithLogLevel("error"),
		WithResultTTL(60*time.Second),
	)
	if err != nil {
		b.Fatalf("NewServer: %v", err)
	}
	// Explicit pools, matching the queues enqueuePaymentChain writes to. Using
	// Handle() alone would create implicit pools that derive their own queue
	// names from the job type, so the jobs would land in queues nothing reads
	// and the chain would never complete.
	for _, cfg := range []PoolConfig{
		{Name: "payments", JobTypes: []string{"payment.authorize", "payment.capture", "payment.settle"},
			Queues: []string{"payments.critical", "payments.default"}, Concurrency: 8},
		{Name: "billing", JobTypes: []string{"invoice.generate"},
			Queues: []string{"billing.default"}, Concurrency: 8},
		{Name: "notifications", JobTypes: []string{"notification.email"},
			Queues: []string{"notifications.default"}, Concurrency: 4},
	} {
		if err := server.Pool(cfg); err != nil {
			b.Fatalf("Pool(%s): %v", cfg.Name, err)
		}
	}
	for _, jt := range []string{"payment.authorize", "payment.capture", "payment.settle", "notification.email"} {
		if err := server.Handle(jt, func(ctx context.Context, job *Job) error { return nil }); err != nil {
			b.Fatalf("Handle %s: %v", jt, err)
		}
	}
	if err := server.Handle("invoice.generate", func(ctx context.Context, job *Job) error {
		completed.Add(1)
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		b.Fatalf("Handle invoice: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = server.Start(ctx) }()
	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := enqueuePaymentChain(context.Background(), client, i, nil); err != nil {
			b.Fatalf("chain %d: %v", i, err)
		}
	}
	for completed.Load() < int64(b.N) {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			b.Fatalf("only %d/%d chains completed", completed.Load(), b.N)
		}
	}
	b.StopTimer()

	cancel()
	client.Close()
	stressCleanupB(b, prefix)
}

// --- Helpers ----------------------------------------------------------------

const maxRetryAuthorize = 3

// amountFor keeps amounts deterministic so the expected ledger can be computed
// without replaying the run.
func amountFor(i int) int64 {
	return int64(1_000_00 + (i%900)*1_37) // Rp 1.000,00 .. ~Rp 13.333,00 in cents
}

func paymentFields(job *Job) (ref string, cents int64, idx int) {
	ref, _ = job.Payload["payment_reference"].(string)
	if amt, ok := job.Payload["amount"].(map[string]any); ok {
		switch v := amt["value"].(type) {
		case float64:
			cents = int64(v)
		case int64:
			cents = v
		case int:
			cents = int64(v)
		}
	}
	// The reference encodes the index, which is how a handler recovers the
	// deterministic outcome without the test threading extra state through.
	_, _ = fmt.Sscanf(ref, "pay_%d", &idx)
	return ref, cents, idx
}

// enqueuePaymentChain builds the full DAG for one payment. The chain is
// enqueued up front, children blocked on their parents, which is how a real
// integration expresses "capture only after authorization succeeds".
func enqueuePaymentChain(ctx context.Context, c *Client, i int, counter *atomic.Int64) error {
	cents := amountFor(i)
	payload := paymentPayload(i, cents)

	queue := "payments.default"
	if i%10 == 0 {
		queue = "payments.critical" // 10% expedited
	}

	auth, err := c.Enqueue(ctx, "payment.authorize", payload,
		Queue(queue),
		MaxRetry(maxRetryAuthorize),
		RetryIntervals(1, 1, 2),
		Timeout(8*time.Second),
	)
	if err != nil {
		return fmt.Errorf("authorize: %w", err)
	}

	capture, err := c.Enqueue(ctx, "payment.capture", payload,
		Queue(queue), DependsOn(auth.ID), MaxRetry(2))
	if err != nil {
		return fmt.Errorf("capture: %w", err)
	}

	settle, err := c.Enqueue(ctx, "payment.settle", payload,
		Queue(queue), DependsOn(capture.ID), MaxRetry(2))
	if err != nil {
		return fmt.Errorf("settle: %w", err)
	}

	invoice, err := c.Enqueue(ctx, "invoice.generate", payload,
		Queue("billing.default"), DependsOn(settle.ID), MaxRetry(2))
	if err != nil {
		return fmt.Errorf("invoice: %w", err)
	}

	// AllowFailure: a bounced email must not unwind a settled payment.
	if _, err := c.Enqueue(ctx, "notification.email", payload,
		Queue("notifications.default"), DependsOn(invoice.ID),
		AllowFailure(true), MaxRetry(1)); err != nil {
		return fmt.Errorf("notification: %w", err)
	}

	if counter != nil {
		counter.Add(5)
	}
	return nil
}

// sinceEnqueue is how long after the chain was enqueued this handler started:
// the wait a caller actually experiences, queue time and DAG blocking
// included. Measuring inside the handler would only report handler speed,
// which nobody is asking about.
//
// Every job in a chain is enqueued at the same instant with its children
// blocked, so this reads as cumulative progress through the DAG rather than
// per-hop cost.
func sinceEnqueue(job *Job) time.Duration {
	var ns int64
	switch v := job.Payload["enqueued_at_ns"].(type) {
	case float64:
		ns = int64(v)
	case int64:
		ns = v
	case int:
		ns = int64(v)
	}
	if ns == 0 {
		return 0
	}
	d := time.Since(time.Unix(0, ns))
	if d < 0 {
		return 0
	}
	return d
}

func mustPool(t *testing.T, s *Server, cfg PoolConfig) {
	t.Helper()
	if err := s.Pool(cfg); err != nil {
		t.Fatalf("Pool(%s): %v", cfg.Name, err)
	}
}

func mustHandle(t *testing.T, s *Server, jobType string, h Handler) {
	t.Helper()
	if err := s.Handle(jobType, h); err != nil {
		t.Fatalf("Handle(%s): %v", jobType, err)
	}
}

func stressCleanupB(b *testing.B, prefix string) {
	b.Helper()
	rc, err := NewRedisClient(WithRedisAddr(testRedisAddr()))
	if err != nil {
		return
	}
	defer rc.Close()
	ctx := context.Background()
	iter := rc.rdb.Scan(ctx, 0, prefix+"*", 1000).Iterator()
	var batch []string
	for iter.Next(ctx) {
		batch = append(batch, iter.Val())
		if len(batch) >= 500 {
			rc.rdb.Del(ctx, batch...)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		rc.rdb.Del(ctx, batch...)
	}
}
