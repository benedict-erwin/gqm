package monitor

import (
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/benedict-erwin/gqm/monitor/dashboard"
)

// dashboardFileServer returns an http.Handler that serves dashboard assets.
// If cfg.DashCustomDir is set, serves from the filesystem (with version check warning).
// Otherwise, serves from the embedded assets.
func (m *Monitor) dashboardFileServer() http.Handler {
	var assets fs.FS

	if m.cfg.DashCustomDir != "" {
		m.checkDashboardVersion(m.cfg.DashCustomDir)
		assets = os.DirFS(m.cfg.DashCustomDir)
		m.logger.Info("dashboard serving from custom directory", "path", m.cfg.DashCustomDir)
	} else {
		assets = dashboard.Assets
		m.logger.Info("dashboard serving from embedded assets")
	}

	fileServer := http.FileServer(http.FS(assets))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Try to serve the exact file if it has an extension
		if filepath.Ext(path) != "" {
			fileServer.ServeHTTP(w, r)
			return
		}

		// For paths without extension (SPA routes) or root, serve index.html directly
		serveIndexHTML(w, assets)
	})
}

// serveIndexHTML reads and writes index.html from the given filesystem.
func serveIndexHTML(w http.ResponseWriter, assets fs.FS) {
	f, err := assets.Open("index.html")
	if err != nil {
		http.Error(w, "index.html not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.Copy(w, f)
}

// checkDashboardVersion reads VERSION from the custom directory and logs a warning
// if it does not match the embedded version.
func (m *Monitor) checkDashboardVersion(customDir string) {
	embeddedVersion, err := fs.ReadFile(dashboard.Assets, "VERSION")
	if err != nil {
		return
	}

	customVersionPath := filepath.Join(customDir, "VERSION")
	customVersion, err := os.ReadFile(customVersionPath)
	if err != nil {
		m.logger.Warn("dashboard custom directory missing VERSION file",
			"path", customVersionPath,
			slog.String("expected", strings.TrimSpace(string(embeddedVersion))))
		return
	}

	ev := strings.TrimSpace(string(embeddedVersion))
	cv := strings.TrimSpace(string(customVersion))
	if ev != cv {
		m.logger.Warn("dashboard version mismatch — custom directory may be outdated",
			"embedded", ev,
			"custom", cv)
	}
}
