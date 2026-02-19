package monitor

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	sessionCookieName = "gqm_session"
	apiKeyHeader      = "X-API-Key"

	// loginRateLimit is the maximum number of failed login attempts per username
	// within the loginRateWindow before further attempts are blocked.
	loginRateLimit  = 5
	loginRateWindow = 5 * time.Minute
)

// dummyHash is a pre-computed bcrypt hash used when a login attempt targets a
// non-existent username. Comparing against this dummy prevents timing-based
// username enumeration (bcrypt is intentionally slow and would otherwise be
// skipped for unknown users, creating a measurable timing difference).
var dummyHash = func() []byte {
	h, _ := bcrypt.GenerateFromPassword([]byte("gqm-dummy-timing-pad"), bcrypt.DefaultCost)
	return h
}()

// validSessionToken matches a 64-character hex string (32 bytes hex-encoded).
var validSessionToken = regexp.MustCompile(`^[0-9a-f]{64}$`)

// requireAuth is middleware that checks for a valid session cookie or API key.
// If auth is disabled in config, all requests are allowed through.
// Sets X-GQM-User and X-GQM-Role internal headers on authenticated requests.
func (m *Monitor) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Strip any client-supplied internal headers to prevent spoofing.
		r.Header.Del("X-GQM-User")
		r.Header.Del("X-GQM-Role")

		// If auth is disabled, skip authentication (admin role by default)
		if !m.cfg.AuthEnabled {
			r.Header.Set("X-GQM-Role", "admin")
			next(w, r)
			return
		}

		// Check session cookie
		if cookie, err := r.Cookie(sessionCookieName); err == nil {
			username, err := m.validateSession(r.Context(), cookie.Value)
			if err == nil {
				r.Header.Set("X-GQM-User", username)
				r.Header.Set("X-GQM-Role", m.userRole(username))
				next(w, r)
				return
			}
		}

		// Check API key header (constant-time comparison to prevent timing attacks)
		if key := r.Header.Get(apiKeyHeader); key != "" {
			if name, role := m.matchAPIKey(key); name != "" {
				r.Header.Set("X-GQM-User", "apikey:"+name)
				r.Header.Set("X-GQM-Role", role)
				next(w, r)
				return
			}
		}

		writeError(w, http.StatusUnauthorized, "unauthorized", "UNAUTHORIZED")
	}
}

// requireAdmin is middleware that checks the authenticated user has the "admin" role.
// Must be used after requireAuth.
// For session cookie auth, also requires the X-GQM-CSRF header as CSRF protection.
// API key auth is exempt since API keys are not sent automatically by browsers.
func (m *Monitor) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		role := r.Header.Get("X-GQM-Role")
		if role != "admin" {
			writeError(w, http.StatusForbidden, "admin role required", "FORBIDDEN")
			return
		}

		// CSRF protection: require X-GQM-CSRF header for session-cookie auth.
		// Custom headers cannot be set cross-origin without CORS preflight
		// (which we don't enable), so this prevents CSRF even if SameSite
		// is somehow bypassed. A static value is sufficient because the
		// protection comes from the header's presence, not its value.
		// API key auth (X-GQM-User starts with "apikey:") is exempt.
		user := r.Header.Get("X-GQM-User")
		isCookieAuth := user != "" && !strings.HasPrefix(user, "apikey:")
		if isCookieAuth && r.Header.Get("X-GQM-CSRF") != "1" {
			writeError(w, http.StatusForbidden, "missing CSRF header", "CSRF_REQUIRED")
			return
		}

		next(w, r)
	}
}

// userRole returns the effective role for a username. Defaults to "admin" if not set.
func (m *Monitor) userRole(username string) string {
	for _, u := range m.cfg.AuthUsers {
		if u.Username == username {
			return effectiveRole(u.Role)
		}
	}
	return "admin"
}

// effectiveRole returns the role, defaulting to "admin" if empty (backward compat).
func effectiveRole(role string) string {
	if role == "" {
		return "admin"
	}
	return role
}

// isSecure reports whether the request was made over HTTPS.
func isSecure(r *http.Request) bool {
	return r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
}

// handleLogin authenticates a user and creates a session.
func (m *Monitor) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !m.cfg.AuthEnabled {
		writeError(w, http.StatusBadRequest, "authentication is not enabled", "AUTH_DISABLED")
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
		return
	}

	if req.Username == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password required", "BAD_REQUEST")
		return
	}
	if len(req.Username) > 256 || len(req.Password) > 256 {
		writeError(w, http.StatusBadRequest, "username or password too long", "BAD_REQUEST")
		return
	}

	ctx := r.Context()

	// Rate limit: check failed login attempts for this username.
	// Combined with the per-IP rate limiter (middleware), this provides
	// both per-username and per-IP brute force protection.
	rateLimitKey := m.key("login_attempts", req.Username)
	attempts, _ := m.rdb.Get(ctx, rateLimitKey).Int64()
	if attempts >= loginRateLimit {
		writeError(w, http.StatusTooManyRequests, "too many login attempts, try again later", "RATE_LIMITED")
		return
	}

	// Find user and verify password.
	// Always run bcrypt (even for unknown users) to prevent timing-based
	// username enumeration. The dummyHash ensures constant-time behavior.
	var found *AuthUser
	for i := range m.cfg.AuthUsers {
		if m.cfg.AuthUsers[i].Username == req.Username {
			found = &m.cfg.AuthUsers[i]
			break
		}
	}

	hash := dummyHash
	if found != nil {
		hash = []byte(found.PasswordHash)
	}

	if err := bcrypt.CompareHashAndPassword(hash, []byte(req.Password)); err != nil || found == nil {
		m.rdb.Incr(ctx, rateLimitKey)
		m.rdb.Expire(ctx, rateLimitKey, loginRateWindow)
		writeError(w, http.StatusUnauthorized, "invalid credentials", "UNAUTHORIZED")
		return
	}

	// Successful login: clear rate limit counter
	m.rdb.Del(ctx, rateLimitKey)

	// Generate session token
	token, err := generateToken()
	if err != nil {
		m.logger.Error("failed to generate session token", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	// Store session in Redis
	sessionKey := m.key("session", token)
	ttl := time.Duration(m.cfg.AuthSessionTTL) * time.Second
	if err := m.rdb.Set(ctx, sessionKey, req.Username, ttl).Err(); err != nil {
		m.logger.Error("failed to store session", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	// Set cookie — Secure only over HTTPS to avoid cookie rejection on plain HTTP
	sameSite := http.SameSiteLaxMode
	if isSecure(r) {
		sameSite = http.SameSiteStrictMode
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   isSecure(r),
		SameSite: sameSite,
		MaxAge:   m.cfg.AuthSessionTTL,
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"username": req.Username,
	})
}

// handleLogout destroys the current session.
func (m *Monitor) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		sessionKey := m.key("session", cookie.Value)
		m.rdb.Del(r.Context(), sessionKey)
	}

	// Clear cookie — match Secure flag to the request scheme
	sameSite := http.SameSiteLaxMode
	if isSecure(r) {
		sameSite = http.SameSiteStrictMode
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   isSecure(r),
		SameSite: sameSite,
		MaxAge:   -1,
	})

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleMe returns the current authenticated user.
func (m *Monitor) handleMe(w http.ResponseWriter, r *http.Request) {
	username := r.Header.Get("X-GQM-User")
	role := r.Header.Get("X-GQM-Role")
	writeJSON(w, http.StatusOK, map[string]any{
		"username": username,
		"role":     role,
	})
}

// validateSession checks a session token in Redis and returns the username.
// Validates token format (64 hex chars) before hitting Redis to avoid
// unnecessary lookups and potential key injection via malformed tokens.
func (m *Monitor) validateSession(ctx context.Context, token string) (string, error) {
	if !validSessionToken.MatchString(token) {
		return "", fmt.Errorf("invalid session token format")
	}
	sessionKey := m.key("session", token)
	username, err := m.rdb.Get(ctx, sessionKey).Result()
	if err != nil {
		return "", fmt.Errorf("session not found: %w", err)
	}
	return username, nil
}

// matchAPIKey performs a constant-time comparison of the given key against all
// configured API keys to prevent timing attacks. Uses SHA-256 hashing to
// normalize lengths before comparison, since subtle.ConstantTimeCompare is
// not constant-time for different-length inputs. Always iterates all keys
// (no early return) so total time is independent of which key matches.
// Returns the key name and effective role.
func (m *Monitor) matchAPIKey(key string) (string, string) {
	keyHash := sha256.Sum256([]byte(key))
	var matched string
	var role string
	for _, ak := range m.cfg.APIKeys {
		akHash := sha256.Sum256([]byte(ak.Key))
		if subtle.ConstantTimeCompare(keyHash[:], akHash[:]) == 1 {
			matched = ak.Name
			role = effectiveRole(ak.Role)
		}
	}
	return matched, role
}

// generateToken creates a cryptographically secure random token (32 bytes, hex encoded).
func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
