package monitor

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// --- L-03: body-parsing endpoints must insist on a JSON content type ---
//
// A cross-site HTML form can only send text/plain, urlencoded or multipart, and
// json.Decoder stops after the first JSON value — so a form field named to open
// a JSON object and valued to close it produces a body the decoder accepts.
// Login takes no cookie, so SameSite does not apply, and the Set-Cookie on the
// response is stored regardless: SameSite governs sending, not setting.

// The exact shape of the cross-site attack: a text/plain form body that is
// nonetheless valid JSON.
func TestSecurity_LoginRejectsCrossSiteFormBody(t *testing.T) {
	cfg := Config{
		AuthEnabled:    true,
		AuthSessionTTL: 86400,
		AuthUsers:      []AuthUser{{Username: "alice", PasswordHash: "$2a$10$x", Role: "viewer"}},
	}
	m, _ := testMonitorWithAdmin(t, cfg, &mockAdmin{})

	body := `{"username":"attacker","password":"attacker-pw"}`
	for _, ct := range []string{
		"text/plain",
		"text/plain;charset=UTF-8",
		"application/x-www-form-urlencoded",
		"multipart/form-data; boundary=x",
		"", // no header at all
	} {
		t.Run("ct="+ct, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(body))
			if ct != "" {
				req.Header.Set("Content-Type", ct)
			}
			w := httptest.NewRecorder()
			m.mux.ServeHTTP(w, req)

			if w.Code != http.StatusUnsupportedMediaType {
				t.Errorf("content type %q got %d, want 415", ct, w.Code)
			}
			// The important half: no session may be handed out.
			if len(w.Result().Cookies()) > 0 {
				t.Error("a cookie was set for a request that should have been refused")
			}
		})
	}
}

// The legitimate content types must keep working, including with a charset
// parameter, or the dashboard login breaks.
func TestSecurity_LoginAcceptsJSONContentTypes(t *testing.T) {
	cfg := Config{
		AuthEnabled:    true,
		AuthSessionTTL: 86400,
		AuthUsers:      []AuthUser{{Username: "alice", PasswordHash: "$2a$10$x", Role: "viewer"}},
	}
	m, _ := testMonitorWithAdmin(t, cfg, &mockAdmin{})

	for _, ct := range []string{"application/json", "application/json; charset=utf-8"} {
		t.Run(ct, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/auth/login",
				strings.NewReader(`{"username":"alice","password":"wrong"}`))
			req.Header.Set("Content-Type", ct)
			w := httptest.NewRecorder()
			m.mux.ServeHTTP(w, req)

			// Wrong password, so 401 — but it must have got past the media type
			// gate to reach the credential check.
			if w.Code == http.StatusUnsupportedMediaType {
				t.Errorf("content type %q was rejected as unsupported", ct)
			}
		})
	}
}

// The batch endpoints parse a body too, so they need the same gate.
func TestSecurity_BatchEndpointsRequireJSON(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})

	for _, path := range []string{"/api/v1/jobs/batch/retry", "/api/v1/jobs/batch/delete"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path,
				strings.NewReader(`{"job_ids":["a"]}`))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("X-GQM-CSRF", "1")
			w := httptest.NewRecorder()
			m.mux.ServeHTTP(w, req)

			if w.Code != http.StatusUnsupportedMediaType {
				t.Errorf("got %d, want 415", w.Code)
			}
		})
	}
}

// --- L-04: the one caller-supplied string that reached a Redis key name ---

func TestSecurity_StatsQueueFilterIsValidated(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})

	for _, q := range []string{
		"has space",
		"crlf\r\ninjected",
		"star*glob",
		strings.Repeat("a", 300), // unbounded length is the real amplifier
	} {
		t.Run(q, func(t *testing.T) {
			w := doRequest(m, http.MethodGet, "/api/v1/stats/daily?queue="+url.QueryEscape(q), "")
			if w.Code != http.StatusBadRequest {
				t.Errorf("queue filter %q got %d, want 400", q, w.Code)
			}
		})
	}
}

// Legitimate queue names must survive, colons included — a queue name owns no
// bare key, so unlike a job ID it may contain them.
func TestSecurity_StatsQueueFilterAcceptsValidNames(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})

	for _, q := range []string{"default", "email:send", "app.reports", "queue-1"} {
		t.Run(q, func(t *testing.T) {
			w := doRequest(m, http.MethodGet, "/api/v1/stats/daily?queue="+url.QueryEscape(q), "")
			if w.Code != http.StatusOK {
				t.Errorf("queue filter %q got %d, want 200", q, w.Code)
			}
		})
	}
	// Absent filter is still valid.
	if w := doRequest(m, http.MethodGet, "/api/v1/stats/daily", ""); w.Code != http.StatusOK {
		t.Errorf("no queue filter got %d, want 200", w.Code)
	}
}

// --- L-05: response headers ---

func TestSecurity_ResponseHeaders(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	// Through the full chain, not m.mux: securityHeaders wraps the mux, so a
	// test that drives the mux directly would assert on headers that the real
	// server adds and see none of them.
	w := doFullChainRequest(m, http.MethodGet, "/api/v1/queues")
	h := w.Header()

	for _, tc := range []struct{ key, want string }{
		{"X-Content-Type-Options", "nosniff"},
		{"X-Frame-Options", "DENY"},
		{"Referrer-Policy", "no-referrer"},
		{"Cross-Origin-Opener-Policy", "same-origin"},
	} {
		if got := h.Get(tc.key); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.key, got, tc.want)
		}
	}
	if h.Get("Permissions-Policy") == "" {
		t.Error("Permissions-Policy is missing")
	}

	// default-src is not a fallback for any of these, which is exactly why
	// their absence mattered.
	csp := h.Get("Content-Security-Policy")
	for _, directive := range []string{
		"base-uri 'none'",
		"form-action 'self'",
		"frame-ancestors 'none'",
		"object-src 'none'",
	} {
		if !strings.Contains(csp, directive) {
			t.Errorf("CSP is missing %q: %s", directive, csp)
		}
	}
}

// Authenticated JSON carries job payloads and session identity, so it must not
// be cached where it can outlive a logout. Static assets stay cacheable.
func TestSecurity_NoStoreOnAPIAndAuthOnly(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{DashEnabled: true, DashPathPrefix: "/dashboard"}, &mockAdmin{})

	for _, path := range []string{"/api/v1/queues", "/auth/me"} {
		w := doFullChainRequest(m, http.MethodGet, path)
		if got := w.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s Cache-Control = %q, want no-store", path, got)
		}
	}

	// A dashboard asset must not be marked no-store: that would be a pure
	// performance loss with nothing to protect.
	w := doFullChainRequest(m, http.MethodGet, "/dashboard/")
	if got := w.Header().Get("Cache-Control"); got == "no-store" {
		t.Error("dashboard asset marked no-store; only /api/ and /auth/ should be")
	}
}

// doFullChainRequest drives the handler the real server uses, middleware
// included. doRequest goes straight to the mux, which is right for handler
// behaviour and wrong for anything a middleware adds.
func doFullChainRequest(m *Monitor, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("X-GQM-CSRF", "1")
	w := httptest.NewRecorder()
	m.server.Handler.ServeHTTP(w, req)
	return w
}
