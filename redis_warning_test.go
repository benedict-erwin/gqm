package gqm

import (
	"bytes"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// The unprotected-Redis banner has to stay unmissable while not drowning out
// everything around it. Those two goals pull against each other, so both
// directions are pinned here: it must still appear, and it must not repeat.

func newTestRedisClient(addr string) *RedisClient {
	return &RedisClient{
		rdb:    redis.NewClient(&redis.Options{Addr: addr}),
		prefix: defaultPrefix,
	}
}

// warnIfUnprotected deliberately does not consult the acknowledgement flag —
// the caller does — so these tests are unaffected by whatever another test may
// have set process-wide.
func TestWarnIfUnprotected_PrintsForAnUnseenAddress(t *testing.T) {
	var buf bytes.Buffer
	newTestRedisClient("warn-test-1:6379").warnIfUnprotected(&buf, true)

	out := buf.String()
	if !strings.Contains(out, "REDIS HAS NO PASSWORD") {
		t.Fatalf("no banner for an unprotected Redis: %q", out)
	}
	// The consequence has to be spelled out, or the banner is just noise.
	if !strings.Contains(out, "session token") && !strings.Contains(out, "session tokens") {
		t.Errorf("banner does not explain the session token consequence: %s", out)
	}
}

// Once per address, not once per Server.Start. A test binary that starts dozens
// of servers used to print dozens of copies: 37 in one run of the suite.
func TestWarnIfUnprotected_RepeatsAreSuppressedPerAddress(t *testing.T) {
	const addr = "warn-test-dedup:6379"

	var first, second bytes.Buffer
	newTestRedisClient(addr).warnIfUnprotected(&first, true)
	newTestRedisClient(addr).warnIfUnprotected(&second, true)

	if !strings.Contains(first.String(), "REDIS HAS NO PASSWORD") {
		t.Fatal("first call did not warn")
	}
	if second.Len() != 0 {
		t.Errorf("second call for the same address warned again: %q", second.String())
	}
}

// Keyed by address rather than a plain sync.Once, so a process talking to two
// unprotected instances still hears about the second one.
func TestWarnIfUnprotected_DifferentAddressStillWarns(t *testing.T) {
	var a, b bytes.Buffer
	newTestRedisClient("warn-test-addr-a:6379").warnIfUnprotected(&a, true)
	newTestRedisClient("warn-test-addr-b:6379").warnIfUnprotected(&b, true)

	if !strings.Contains(a.String(), "REDIS HAS NO PASSWORD") {
		t.Error("no banner for the first address")
	}
	if !strings.Contains(b.String(), "REDIS HAS NO PASSWORD") {
		t.Error("dedup swallowed a different address; it must be keyed per address")
	}
}

// A password means there is nothing to warn about.
func TestWarnIfUnprotected_SilentWhenPasswordIsSet(t *testing.T) {
	rc := &RedisClient{
		rdb:    redis.NewClient(&redis.Options{Addr: "warn-test-pw:6379", Password: "s3cret"}),
		prefix: defaultPrefix,
	}
	var buf bytes.Buffer
	rc.warnIfUnprotected(&buf, true)
	if buf.Len() != 0 {
		t.Errorf("warned about a password-protected Redis: %q", buf.String())
	}
}

// The acknowledgement is what Server.Start consults. It is one-way by design,
// so this restores the previous value rather than assuming it was unset — no
// root test runs in parallel, which is what makes that safe.
func TestAcknowledgeUnprotectedRedis_SetsTheFlag(t *testing.T) {
	prev := unprotectedRedisAcknowledged.Load()
	t.Cleanup(func() { unprotectedRedisAcknowledged.Store(prev) })

	unprotectedRedisAcknowledged.Store(false)
	if unprotectedRedisAcknowledged.Load() {
		t.Fatal("flag did not start false")
	}

	AcknowledgeUnprotectedRedis()
	if !unprotectedRedisAcknowledged.Load() {
		t.Error("AcknowledgeUnprotectedRedis did not set the flag")
	}

	// Calling twice must stay safe and stay true.
	AcknowledgeUnprotectedRedis()
	if !unprotectedRedisAcknowledged.Load() {
		t.Error("a second call cleared the flag")
	}
}

// End-to-end proof that acknowledging silences Server.Start, and that not
// acknowledging does not, lives in scripts/verify-security-fixes.sh, where each
// case runs as its own process. That is the only way to test process-global
// state honestly: a same-process test would have to mutate the flag and could
// only ever prove the mechanism, not the wiring.
