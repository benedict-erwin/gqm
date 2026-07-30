package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Sessions live in Redis and survive both a config change and a restart, so
// removing a user from config is only a real revocation if the session stops
// working. Before this was fixed an orphaned session not only kept working but
// resolved to admin, which meant revoking a viewer promoted them.

func TestSecurity_RevokedUserSessionIsRejectedAndDeleted(t *testing.T) {
	cfg := Config{
		AuthEnabled:    true,
		AuthSessionTTL: 86400,
		AuthUsers: []AuthUser{
			// "bob" has been removed from config. Only alice remains.
			{Username: "alice", PasswordHash: "$2a$10$x", Role: "viewer"},
		},
	}
	m, rdb := testMonitorWithAdmin(t, cfg, &mockAdmin{})
	ctx := context.Background()

	token := "aaaaaaaabbbbbbbbccccccccddddddddeeeeeeeeffffffff0000000011111111"
	sessionKey := m.key("session", token)
	if err := rdb.Set(ctx, sessionKey, "bob", time.Hour).Err(); err != nil {
		t.Fatalf("seeding session: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/queues/default/empty", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	req.Header.Set("X-GQM-CSRF", "1")
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("revoked user got %d, want 401", w.Code)
	}
	// The orphaned session must be gone, not merely refused this once.
	if n, _ := rdb.Exists(ctx, sessionKey).Result(); n != 0 {
		t.Error("orphaned session still present in Redis after rejection")
	}
}

// A configured viewer must still be refused admin actions. This is the other
// half of the change: unknown users are rejected without loosening anything for
// users that are still configured.
func TestSecurity_ConfiguredViewerStillForbidden(t *testing.T) {
	cfg := Config{
		AuthEnabled:    true,
		AuthSessionTTL: 86400,
		AuthUsers:      []AuthUser{{Username: "alice", PasswordHash: "$2a$10$x", Role: "viewer"}},
	}
	m, rdb := testMonitorWithAdmin(t, cfg, &mockAdmin{})
	ctx := context.Background()

	token := "1111111122222222333333334444444455555555666666667777777788888888"
	if err := rdb.Set(ctx, m.key("session", token), "alice", time.Hour).Err(); err != nil {
		t.Fatalf("seeding session: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/queues/default/empty", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	req.Header.Set("X-GQM-CSRF", "1")
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("configured viewer got %d, want 403", w.Code)
	}
}

// A configured admin must still be allowed, so the fail-closed change cannot be
// mistaken for "everything is denied now".
func TestSecurity_ConfiguredAdminStillAllowed(t *testing.T) {
	cfg := Config{
		AuthEnabled:    true,
		AuthSessionTTL: 86400,
		AuthUsers:      []AuthUser{{Username: "root", PasswordHash: "$2a$10$x", Role: "admin"}},
	}
	m, rdb := testMonitorWithAdmin(t, cfg, &mockAdmin{})
	ctx := context.Background()

	token := "9999999988888888777777776666666655555555444444443333333322222222"
	if err := rdb.Set(ctx, m.key("session", token), "root", time.Hour).Err(); err != nil {
		t.Fatalf("seeding session: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/queues/default/empty", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	req.Header.Set("X-GQM-CSRF", "1")
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)

	if w.Code == http.StatusUnauthorized || w.Code == http.StatusForbidden {
		t.Errorf("configured admin got %d, want the request to be authorised", w.Code)
	}
}

// An API key with no explicit role gets the least privilege, matching users.
func TestSecurity_APIKeyWithoutRoleIsViewer(t *testing.T) {
	const key = "gqm_ak_test_key_at_least_32_characters_long"
	cfg := Config{
		AuthEnabled: true,
		APIKeys:     []AuthAPIKey{{Name: "ci", Key: key}}, // no Role set
	}
	m, _ := testMonitorWithAdmin(t, cfg, &mockAdmin{})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/queues/default/empty", nil)
	req.Header.Set("X-API-Key", key)
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("role-less API key got %d on an admin route, want 403", w.Code)
	}

	// It must still be able to read, or it would be useless rather than limited.
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/queues", nil)
	req2.Header.Set("X-API-Key", key)
	w2 := httptest.NewRecorder()
	m.mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Errorf("role-less API key got %d on a read route, want 200", w2.Code)
	}
}
