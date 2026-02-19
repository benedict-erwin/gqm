package monitor

import (
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	defaultAPIRateLimit = 100 // requests per second per IP
	rateLimitBurstMult  = 2   // burst = rate * multiplier
)

// rateLimiter implements a per-IP token bucket rate limiter for API endpoints.
type rateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*tokenBucket
	rate     float64 // tokens replenished per second
	burst    int     // maximum burst size
	stop     chan struct{}
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

// newRateLimiter creates a per-IP rate limiter. A background goroutine
// periodically removes stale entries to prevent memory growth.
func newRateLimiter(rate int) *rateLimiter {
	if rate <= 0 {
		rate = defaultAPIRateLimit
	}
	rl := &rateLimiter{
		visitors: make(map[string]*tokenBucket),
		rate:     float64(rate),
		burst:    rate * rateLimitBurstMult,
		stop:     make(chan struct{}),
	}
	go rl.cleanup()
	return rl
}

// allow checks whether a request from the given IP is permitted.
func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, exists := rl.visitors[ip]
	if !exists {
		rl.visitors[ip] = &tokenBucket{tokens: float64(rl.burst) - 1, last: now}
		return true
	}

	// Refill tokens based on elapsed time
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// cleanup periodically removes stale visitor entries (no activity for 5+ minutes).
func (rl *rateLimiter) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-rl.stop:
			return
		case <-ticker.C:
			rl.mu.Lock()
			cutoff := time.Now().Add(-5 * time.Minute)
			for ip, b := range rl.visitors {
				if b.last.Before(cutoff) {
					delete(rl.visitors, ip)
				}
			}
			rl.mu.Unlock()
		}
	}
}

// close stops the background cleanup goroutine.
func (rl *rateLimiter) close() {
	close(rl.stop)
}

// middleware returns an HTTP middleware that rate-limits requests by IP.
// The /health endpoint is exempt from rate limiting.
func (rl *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Exempt health checks
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		ip := extractIP(r)
		if !rl.allow(ip) {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded", "RATE_LIMITED")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// extractIP extracts the IP address from an HTTP request's RemoteAddr.
func extractIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
