package monitor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	sessionCookieName = "gqm_session"
	apiKeyHeader      = "X-API-Key"
)

// requireAuth is middleware that checks for a valid session cookie or API key.
// If auth is disabled in config, all requests are allowed through.
func (m *Monitor) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// If auth is disabled, skip authentication
		if !m.cfg.AuthEnabled {
			next(w, r)
			return
		}

		// Check session cookie
		if cookie, err := r.Cookie(sessionCookieName); err == nil {
			username, err := m.validateSession(r.Context(), cookie.Value)
			if err == nil {
				r.Header.Set("X-GQM-User", username)
				next(w, r)
				return
			}
		}

		// Check API key header
		if key := r.Header.Get(apiKeyHeader); key != "" {
			if name, ok := m.apiKeyMap[key]; ok {
				r.Header.Set("X-GQM-User", "apikey:"+name)
				next(w, r)
				return
			}
		}

		writeError(w, http.StatusUnauthorized, "unauthorized", "UNAUTHORIZED")
	}
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
		return
	}

	if req.Username == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password required", "BAD_REQUEST")
		return
	}

	// Find user and verify password
	var found *AuthUser
	for i := range m.cfg.AuthUsers {
		if m.cfg.AuthUsers[i].Username == req.Username {
			found = &m.cfg.AuthUsers[i]
			break
		}
	}
	if found == nil {
		writeError(w, http.StatusUnauthorized, "invalid credentials", "UNAUTHORIZED")
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(found.PasswordHash), []byte(req.Password)); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid credentials", "UNAUTHORIZED")
		return
	}

	// Generate session token
	token, err := generateToken()
	if err != nil {
		m.logger.Error("failed to generate session token", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	// Store session in Redis
	ctx := r.Context()
	sessionKey := m.key("session", token)
	ttl := time.Duration(m.cfg.AuthSessionTTL) * time.Second
	if err := m.rdb.Set(ctx, sessionKey, req.Username, ttl).Err(); err != nil {
		m.logger.Error("failed to store session", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	// Set cookie
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
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

	// Clear cookie
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	})

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleMe returns the current authenticated user.
func (m *Monitor) handleMe(w http.ResponseWriter, r *http.Request) {
	username := r.Header.Get("X-GQM-User")
	writeJSON(w, http.StatusOK, map[string]any{
		"username": username,
	})
}

// validateSession checks a session token in Redis and returns the username.
func (m *Monitor) validateSession(ctx context.Context, token string) (string, error) {
	sessionKey := m.key("session", token)
	username, err := m.rdb.Get(ctx, sessionKey).Result()
	if err != nil {
		return "", fmt.Errorf("session not found: %w", err)
	}
	return username, nil
}

// generateToken creates a cryptographically secure random token (32 bytes, hex encoded).
func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
