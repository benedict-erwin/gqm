package monitor

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The dashboard route is served without authentication on purpose: the SPA
// checks auth status itself and fetches all data through authenticated APIs.
// That is only safe while the files behind the route are genuinely assets.
// A custom_dir breaks that assumption — it is an arbitrary operator directory
// that may also contain gqm.yaml (plaintext API keys), dotenv files, or
// backups. These tests pin the guarantees that keep the route safe anyway.

func writeDashFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// dashFixture builds a custom_dir containing both legitimate assets and the
// kind of sensitive files that end up beside a deployed dashboard.
func dashFixture(t *testing.T) (*Monitor, string) {
	t.Helper()
	dir := t.TempDir()

	writeDashFile(t, filepath.Join(dir, "index.html"), "<html>dashboard</html>")
	if err := os.Mkdir(filepath.Join(dir, "css"), 0o700); err != nil {
		t.Fatalf("mkdir css: %v", err)
	}
	writeDashFile(t, filepath.Join(dir, "css", "style.css"), "body{}")
	writeDashFile(t, filepath.Join(dir, "gqm.yaml"), "api_keys:\n  - key: SECRET_API_KEY\n")
	writeDashFile(t, filepath.Join(dir, ".env"), "REDIS_PASSWORD=SECRET_PASSWORD\n")
	writeDashFile(t, filepath.Join(dir, "config.yaml.bak"), "password_hash: SECRET_HASH\n")
	writeDashFile(t, filepath.Join(dir, "notes.md"), "SECRET_NOTES")

	cfg := Config{
		AuthEnabled:    true,
		AuthSessionTTL: 86400,
		DashEnabled:    true,
		DashPathPrefix: "/dashboard",
		DashCustomDir:  dir,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: "$2a$10$x", Role: "admin"}},
	}
	m, _ := testMonitor(t, cfg)
	return m, dir
}

func getDash(m *Monitor, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestDashboardCustomDir_DoesNotServeSecrets(t *testing.T) {
	m, _ := dashFixture(t)

	// Precondition: auth really is enabled, so a 200 below would be a genuine
	// unauthenticated read rather than an artefact of auth being off.
	if w := getDash(m, "/api/v1/queues"); w.Code != http.StatusUnauthorized {
		t.Fatalf("precondition: /api/v1/queues = %d, want 401", w.Code)
	}

	for _, tc := range []struct{ path, secret string }{
		{"/dashboard/gqm.yaml", "SECRET_API_KEY"},
		{"/dashboard/.env", "SECRET_PASSWORD"},
		{"/dashboard/config.yaml.bak", "SECRET_HASH"},
		{"/dashboard/notes.md", "SECRET_NOTES"},
	} {
		w := getDash(m, tc.path)
		if strings.Contains(w.Body.String(), tc.secret) {
			t.Errorf("GET %s leaked %q (HTTP %d)", tc.path, tc.secret, w.Code)
		}
		if w.Code == http.StatusOK && strings.Contains(w.Body.String(), tc.secret) {
			t.Errorf("GET %s served the file verbatim", tc.path)
		}
	}
}

func TestDashboardCustomDir_StillServesLegitimateAssets(t *testing.T) {
	// The hardening must not break the dashboard it protects.
	m, _ := dashFixture(t)

	if w := getDash(m, "/dashboard/css/style.css"); w.Code != http.StatusOK {
		t.Errorf("css/style.css = %d, want 200", w.Code)
	}
	// http.FileServer canonicalises an explicit /index.html to ./, so a 301 here
	// is the stdlib's normal behaviour, not the allowlist rejecting the file.
	if w := getDash(m, "/dashboard/index.html"); w.Code != http.StatusMovedPermanently {
		t.Errorf("index.html = %d, want 301 (canonical redirect to ./)", w.Code)
	}
	if w := getDash(m, "/dashboard/"); w.Code != http.StatusOK {
		t.Errorf("/dashboard/ = %d, want 200", w.Code)
	}
	// SPA fallback: an extensionless route must still return index.html.
	w := getDash(m, "/dashboard/queues")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "dashboard") {
		t.Errorf("SPA fallback = %d body=%q, want 200 with index.html", w.Code, w.Body.String())
	}
}

func TestDashboardCustomDir_DoesNotFollowSymlinkOutOfTree(t *testing.T) {
	m, dir := dashFixture(t)

	outside := t.TempDir()
	writeDashFile(t, filepath.Join(outside, "secret.txt"), "OUTSIDE_SECRET")
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(dir, "link.txt")); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	// A symlink to a whole directory tree is the more dangerous shape.
	if err := os.Symlink(outside, filepath.Join(dir, "outside")); err != nil {
		t.Skipf("cannot create dir symlink: %v", err)
	}

	for _, p := range []string{"/dashboard/link.txt", "/dashboard/outside/secret.txt"} {
		w := getDash(m, p)
		if strings.Contains(w.Body.String(), "OUTSIDE_SECRET") {
			t.Errorf("GET %s escaped custom_dir via symlink (HTTP %d)", p, w.Code)
		}
	}
}

func TestDashboardCustomDir_TraversalRemainsBlocked(t *testing.T) {
	// Regression guard for the traversal defences that already worked, so a
	// future refactor of the file server cannot quietly remove them.
	m, _ := dashFixture(t)

	for _, p := range []string{
		"/dashboard/..%2f..%2f..%2fetc/hosts",
		"/dashboard/%2e%2e%2f%2e%2e%2fetc/hosts",
		"/dashboard/....//....//etc/hosts",
	} {
		w := getDash(m, p)
		if strings.Contains(w.Body.String(), "localhost") {
			t.Errorf("GET %s appears to have read /etc/hosts (HTTP %d)", p, w.Code)
		}
	}
}

func TestDashboardEmbedded_RejectsDotfilesAndNonAssets(t *testing.T) {
	// The allowlist applies to the embedded assets too: the //go:embed patterns
	// use wildcards like css/*, which would also match a dot-prefixed file
	// added later.
	cfg := Config{DashEnabled: true, DashPathPrefix: "/dashboard"}
	m, _ := testMonitor(t, cfg)

	for _, p := range []string{"/dashboard/.env", "/dashboard/secrets.yaml"} {
		if w := getDash(m, p); w.Code == http.StatusOK {
			t.Errorf("GET %s = 200, want non-200 for embedded mode", p)
		}
	}
	// The real dashboard must still load.
	if w := getDash(m, "/dashboard/"); w.Code != http.StatusOK {
		t.Errorf("GET /dashboard/ = %d, want 200", w.Code)
	}
}

func TestSafeFS_Open(t *testing.T) {
	dir := t.TempDir()
	writeDashFile(t, filepath.Join(dir, "ok.css"), "body{}")
	writeDashFile(t, filepath.Join(dir, "secret.yaml"), "x")
	writeDashFile(t, filepath.Join(dir, ".hidden"), "x")
	if err := os.Mkdir(filepath.Join(dir, ".git"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeDashFile(t, filepath.Join(dir, ".git", "config.json"), "x")

	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	s := safeFS{root.FS()}

	tests := []struct {
		name    string
		wantErr bool
	}{
		{"ok.css", false},
		{"secret.yaml", true},      // extension not allowlisted
		{".hidden", true},          // dot-prefixed
		{".git/config.json", true}, // dot-prefixed directory element
	}
	for _, tt := range tests {
		f, err := s.Open(tt.name)
		if err == nil {
			f.Close()
		}
		if (err != nil) != tt.wantErr {
			t.Errorf("Open(%q) err = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}
