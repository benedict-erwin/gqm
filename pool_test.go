package gqm

import (
	"log/slog"
	"runtime"
	"testing"
	"time"
)

func TestNewDefaultPoolConfig(t *testing.T) {
	t.Run("with queues", func(t *testing.T) {
		cfg := newDefaultPoolConfig("mypool", "q1", "q2")
		if cfg.name != "mypool" {
			t.Errorf("name = %q, want %q", cfg.name, "mypool")
		}
		if len(cfg.queues) != 2 || cfg.queues[0] != "q1" || cfg.queues[1] != "q2" {
			t.Errorf("queues = %v, want [q1, q2]", cfg.queues)
		}
		if cfg.concurrency != runtime.NumCPU() {
			t.Errorf("concurrency = %d, want %d", cfg.concurrency, runtime.NumCPU())
		}
		if cfg.dequeueStrategy != StrategyWeighted {
			t.Errorf("dequeueStrategy = %q, want %q", cfg.dequeueStrategy, StrategyWeighted)
		}
		if cfg.retryPolicy != nil {
			t.Error("retryPolicy should be nil by default")
		}
	})

	t.Run("no queues defaults to default", func(t *testing.T) {
		cfg := newDefaultPoolConfig("mypool")
		if len(cfg.queues) != 1 || cfg.queues[0] != "default" {
			t.Errorf("queues = %v, want [default]", cfg.queues)
		}
	})
}

func TestPoolConfig_ToInternal(t *testing.T) {
	serverGrace := 15 * time.Second

	t.Run("defaults applied", func(t *testing.T) {
		pc := PoolConfig{Name: "test-pool"}
		cfg := pc.toInternal(serverGrace)

		if cfg.name != "test-pool" {
			t.Errorf("name = %q, want %q", cfg.name, "test-pool")
		}
		if len(cfg.queues) != 1 || cfg.queues[0] != "default" {
			t.Errorf("queues = %v, want [default]", cfg.queues)
		}
		if cfg.concurrency != runtime.NumCPU() {
			t.Errorf("concurrency = %d, want %d", cfg.concurrency, runtime.NumCPU())
		}
		if cfg.gracePeriod != serverGrace {
			t.Errorf("gracePeriod = %v, want %v", cfg.gracePeriod, serverGrace)
		}
		if cfg.dequeueStrategy != StrategyStrict {
			t.Errorf("dequeueStrategy = %q, want %q", cfg.dequeueStrategy, StrategyStrict)
		}
	})

	t.Run("custom values preserved", func(t *testing.T) {
		rp := &RetryPolicy{MaxRetry: 5, Backoff: BackoffExponential}
		pc := PoolConfig{
			Name:            "custom",
			Queues:          []string{"email", "push"},
			Concurrency:     10,
			GracePeriod:     20 * time.Second,
			DequeueStrategy: StrategyRoundRobin,
			RetryPolicy:     rp,
		}
		cfg := pc.toInternal(serverGrace)

		if len(cfg.queues) != 2 || cfg.queues[0] != "email" {
			t.Errorf("queues = %v, want [email, push]", cfg.queues)
		}
		if cfg.concurrency != 10 {
			t.Errorf("concurrency = %d, want 10", cfg.concurrency)
		}
		if cfg.gracePeriod != 20*time.Second {
			t.Errorf("gracePeriod = %v, want 20s", cfg.gracePeriod)
		}
		if cfg.dequeueStrategy != StrategyRoundRobin {
			t.Errorf("dequeueStrategy = %q, want %q", cfg.dequeueStrategy, StrategyRoundRobin)
		}
		if cfg.retryPolicy != rp {
			t.Error("retryPolicy not preserved")
		}
	})
}

// newTestPool creates a minimal pool for unit testing without Redis.
func newTestPool(cfg *poolConfig) *pool {
	return &pool{
		cfg:        cfg,
		logger:     slog.Default(),
		activeJobs: make(map[int]string),
	}
}

func TestDequeueOrder_SingleQueue(t *testing.T) {
	p := newTestPool(&poolConfig{queues: []string{"only"}})
	order := p.dequeueOrder()
	if len(order) != 1 || order[0] != 0 {
		t.Errorf("order = %v, want [0]", order)
	}
}

func TestDequeueOrder_Strict(t *testing.T) {
	p := newTestPool(&poolConfig{
		queues:          []string{"high", "medium", "low"},
		dequeueStrategy: StrategyStrict,
	})

	// Strict should always return [0, 1, 2]
	for i := 0; i < 10; i++ {
		order := p.dequeueOrder()
		if len(order) != 3 || order[0] != 0 || order[1] != 1 || order[2] != 2 {
			t.Fatalf("iteration %d: order = %v, want [0, 1, 2]", i, order)
		}
	}
}

func TestDequeueOrder_RoundRobin(t *testing.T) {
	p := newTestPool(&poolConfig{
		queues:          []string{"a", "b", "c"},
		dequeueStrategy: StrategyRoundRobin,
	})

	// Round robin should rotate the starting index each call.
	// Call 1: start=0 → [0, 1, 2]
	// Call 2: start=1 → [1, 2, 0]
	// Call 3: start=2 → [2, 0, 1]
	// Call 4: start=0 → [0, 1, 2] (wraps)
	expected := [][]int{
		{0, 1, 2},
		{1, 2, 0},
		{2, 0, 1},
		{0, 1, 2},
	}

	for i, want := range expected {
		got := p.dequeueOrder()
		if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
			t.Errorf("call %d: got %v, want %v", i+1, got, want)
		}
	}
}

func TestDequeueOrder_Weighted_Distribution(t *testing.T) {
	p := newTestPool(&poolConfig{
		queues:          []string{"high", "medium", "low"},
		dequeueStrategy: StrategyWeighted,
	})

	// Run many iterations and verify that higher-priority queues are
	// selected as first choice more often. Weights: [3, 2, 1] → total 6.
	// Expected distribution: high ≈ 50%, medium ≈ 33%, low ≈ 17%.
	counts := make(map[int]int)
	iterations := 6000
	for i := 0; i < iterations; i++ {
		order := p.dequeueOrder()
		counts[order[0]]++

		// All queues must appear in every order
		if len(order) != 3 {
			t.Fatalf("iteration %d: order length = %d, want 3", i, len(order))
		}
	}

	// Verify bias: high (idx 0) should be picked more than low (idx 2).
	// Use generous thresholds to avoid flaky tests.
	highPct := float64(counts[0]) / float64(iterations) * 100
	lowPct := float64(counts[2]) / float64(iterations) * 100

	if highPct < 35 { // expect ~50%, threshold 35%
		t.Errorf("high queue selected %.1f%% (expected ~50%%, min 35%%)", highPct)
	}
	if lowPct > 30 { // expect ~17%, threshold 30%
		t.Errorf("low queue selected %.1f%% (expected ~17%%, max 30%%)", lowPct)
	}
	if counts[0] <= counts[2] {
		t.Errorf("high queue (%d) should be selected more than low queue (%d)", counts[0], counts[2])
	}
}

func TestResolveMaxRetry(t *testing.T) {
	tests := []struct {
		name       string
		jobMax     int
		poolPolicy *RetryPolicy
		want       int
	}{
		{
			name:   "job-level takes priority",
			jobMax: 5,
			poolPolicy: &RetryPolicy{MaxRetry: 3},
			want:   5,
		},
		{
			name:       "pool-level when job is 0",
			jobMax:     0,
			poolPolicy: &RetryPolicy{MaxRetry: 3},
			want:       3,
		},
		{
			name:       "default when both 0",
			jobMax:     0,
			poolPolicy: nil,
			want:       0,
		},
		{
			name:       "pool policy with 0 max retry",
			jobMax:     0,
			poolPolicy: &RetryPolicy{MaxRetry: 0},
			want:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestPool(&poolConfig{retryPolicy: tt.poolPolicy})
			job := &Job{MaxRetry: tt.jobMax}
			got := p.resolveMaxRetry(job)
			if got != tt.want {
				t.Errorf("resolveMaxRetry() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRetryDelay(t *testing.T) {
	tests := []struct {
		name       string
		job        *Job
		poolPolicy *RetryPolicy
		retryCount int
		want       time.Duration
	}{
		{
			name:       "job intervals take priority",
			job:        &Job{RetryIntervals: []int{10, 30, 60}},
			poolPolicy: &RetryPolicy{Backoff: BackoffFixed, BackoffBase: 5 * time.Second},
			retryCount: 2,
			want:       30 * time.Second,
		},
		{
			name:       "job intervals clamp to last",
			job:        &Job{RetryIntervals: []int{10, 30}},
			poolPolicy: nil,
			retryCount: 5,
			want:       30 * time.Second,
		},
		{
			name:       "pool fixed backoff",
			job:        &Job{},
			poolPolicy: &RetryPolicy{Backoff: BackoffFixed, BackoffBase: 15 * time.Second},
			retryCount: 3,
			want:       15 * time.Second,
		},
		{
			name:       "pool exponential backoff",
			job:        &Job{},
			poolPolicy: &RetryPolicy{Backoff: BackoffExponential, BackoffBase: 10 * time.Second, BackoffMax: 5 * time.Minute},
			retryCount: 1,
			want:       10 * time.Second, // 10 * 2^0
		},
		{
			name:       "pool exponential backoff attempt 3",
			job:        &Job{},
			poolPolicy: &RetryPolicy{Backoff: BackoffExponential, BackoffBase: 10 * time.Second, BackoffMax: 5 * time.Minute},
			retryCount: 3,
			want:       40 * time.Second, // 10 * 2^2
		},
		{
			name:       "pool exponential backoff capped",
			job:        &Job{},
			poolPolicy: &RetryPolicy{Backoff: BackoffExponential, BackoffBase: 10 * time.Second, BackoffMax: 30 * time.Second},
			retryCount: 5,
			want:       30 * time.Second, // capped at max
		},
		{
			name:       "pool custom intervals",
			job:        &Job{},
			poolPolicy: &RetryPolicy{Backoff: BackoffCustom, Intervals: []int{5, 15, 45}},
			retryCount: 2,
			want:       15 * time.Second,
		},
		{
			name:       "pool custom intervals clamp to last",
			job:        &Job{},
			poolPolicy: &RetryPolicy{Backoff: BackoffCustom, Intervals: []int{5, 15}},
			retryCount: 10,
			want:       15 * time.Second,
		},
		{
			name:       "default when no job or pool config",
			job:        &Job{},
			poolPolicy: nil,
			retryCount: 1,
			want:       defaultRetryDelay,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestPool(&poolConfig{retryPolicy: tt.poolPolicy})
			got := p.retryDelay(tt.job, tt.retryCount)
			if got != tt.want {
				t.Errorf("retryDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPoolRetryDelay_Exponential(t *testing.T) {
	rp := &RetryPolicy{
		Backoff:     BackoffExponential,
		BackoffBase: 2 * time.Second,
		BackoffMax:  30 * time.Second,
	}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	tests := []struct {
		retryCount int
		want       time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 30 * time.Second}, // capped at max
		{10, 30 * time.Second},
	}
	for _, tt := range tests {
		got := p.poolRetryDelay(rp, tt.retryCount)
		if got != tt.want {
			t.Errorf("exponential(%d) = %v, want %v", tt.retryCount, got, tt.want)
		}
	}
}

func TestPoolRetryDelay_ExponentialDefaultBase(t *testing.T) {
	rp := &RetryPolicy{Backoff: BackoffExponential}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	got := p.poolRetryDelay(rp, 1)
	if got != defaultRetryDelay {
		t.Errorf("got %v, want default %v", got, defaultRetryDelay)
	}
}

func TestPoolRetryDelay_Custom(t *testing.T) {
	rp := &RetryPolicy{
		Backoff:   BackoffCustom,
		Intervals: []int{5, 15, 60},
	}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	tests := []struct {
		retryCount int
		want       time.Duration
	}{
		{1, 5 * time.Second},
		{2, 15 * time.Second},
		{3, 60 * time.Second},
		{4, 60 * time.Second}, // clamps to last
	}
	for _, tt := range tests {
		got := p.poolRetryDelay(rp, tt.retryCount)
		if got != tt.want {
			t.Errorf("custom(%d) = %v, want %v", tt.retryCount, got, tt.want)
		}
	}
}

func TestPoolRetryDelay_CustomEmptyIntervals(t *testing.T) {
	rp := &RetryPolicy{
		Backoff:     BackoffCustom,
		Intervals:   nil,
		BackoffBase: 10 * time.Second,
	}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	got := p.poolRetryDelay(rp, 1)
	if got != 10*time.Second {
		t.Errorf("got %v, want 10s", got)
	}
}

func TestPoolRetryDelay_CustomNoIntervalsNoBase(t *testing.T) {
	rp := &RetryPolicy{Backoff: BackoffCustom}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	got := p.poolRetryDelay(rp, 1)
	if got != defaultRetryDelay {
		t.Errorf("got %v, want default %v", got, defaultRetryDelay)
	}
}

func TestPoolRetryDelay_Fixed(t *testing.T) {
	rp := &RetryPolicy{
		Backoff:     BackoffFixed,
		BackoffBase: 7 * time.Second,
	}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	got := p.poolRetryDelay(rp, 3)
	if got != 7*time.Second {
		t.Errorf("got %v, want 7s", got)
	}
}

func TestPoolRetryDelay_FixedNoBase(t *testing.T) {
	rp := &RetryPolicy{Backoff: BackoffFixed}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	got := p.poolRetryDelay(rp, 1)
	if got != defaultRetryDelay {
		t.Errorf("got %v, want default %v", got, defaultRetryDelay)
	}
}

func TestPoolRetryDelay_ExponentialOverflow(t *testing.T) {
	// No BackoffMax set — overflow must be caught and capped.
	rp := &RetryPolicy{
		Backoff:     BackoffExponential,
		BackoffBase: 1 * time.Second,
	}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	// retryCount=100 would cause 2^99 overflow without guard.
	got := p.poolRetryDelay(rp, 100)
	if got <= 0 {
		t.Fatalf("overflow not caught: got %v", got)
	}
	if got > maxBackoffDelay {
		t.Errorf("delay %v exceeds maxBackoffDelay %v", got, maxBackoffDelay)
	}
}

func TestPoolRetryDelay_ExponentialOverflowWithMax(t *testing.T) {
	// BackoffMax set — overflow should return BackoffMax.
	rp := &RetryPolicy{
		Backoff:     BackoffExponential,
		BackoffBase: 1 * time.Second,
		BackoffMax:  1 * time.Hour,
	}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	got := p.poolRetryDelay(rp, 100)
	if got != 1*time.Hour {
		t.Errorf("got %v, want BackoffMax 1h", got)
	}
}

func TestPoolRetryDelay_ExponentialHardCap(t *testing.T) {
	// No BackoffMax, moderate retry count that exceeds 24h without overflow.
	rp := &RetryPolicy{
		Backoff:     BackoffExponential,
		BackoffBase: 1 * time.Minute,
	}
	p := newTestPool(&poolConfig{retryPolicy: rp})

	// 2^20 minutes = ~1048576 min >> 24h; should be capped at maxBackoffDelay.
	got := p.poolRetryDelay(rp, 21)
	if got != maxBackoffDelay {
		t.Errorf("got %v, want maxBackoffDelay %v", got, maxBackoffDelay)
	}
}
