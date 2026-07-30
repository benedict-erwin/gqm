package monitor

import (
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/benedict-erwin/gqm/monitor/dashboard"
)

// allowedDashExt is the set of file extensions the dashboard will serve.
//
// The dashboard route is deliberately unauthenticated, which is safe only as
// long as the files behind it are genuinely assets. With a custom_dir that is
// not a given: it is an arbitrary operator-supplied directory that may also
// hold gqm.yaml, dotenv files, or backups. An allowlist keeps the
// "assets only" premise true instead of assuming it.
var allowedDashExt = map[string]bool{
	".html": true, ".css": true, ".js": true, ".json": true, ".map": true,
	".svg": true, ".png": true, ".jpg": true, ".jpeg": true, ".gif": true,
	".ico": true, ".webp": true, ".woff": true, ".woff2": true, ".ttf": true,
	".eot": true, ".txt": true,
}

// safeFS restricts a filesystem to dashboard assets: it rejects dot-prefixed
// path elements and any extension outside allowedDashExt.
//
// It also normalises escape errors to fs.ErrNotExist. os.Root refuses symlinks
// that leave the root, but it reports that as its own error, which would
// surface as a 500 and act as an existence oracle. A 404 reveals nothing.
type safeFS struct{ fsys fs.FS }

func (s safeFS) Open(name string) (fs.File, error) {
	for _, el := range strings.Split(name, "/") {
		if el != "." && strings.HasPrefix(el, ".") {
			return nil, fs.ErrNotExist
		}
	}
	if ext := path.Ext(name); ext != "" && !allowedDashExt[ext] {
		return nil, fs.ErrNotExist
	}
	f, err := s.fsys.Open(name)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fs.ErrNotExist
	}
	return f, err
}

// dashboardFileServer returns an http.Handler that serves dashboard assets.
// If cfg.DashCustomDir is set, serves from the filesystem (with version check warning).
// Otherwise, serves from the embedded assets.
func (m *Monitor) dashboardFileServer() http.Handler {
	var assets fs.FS

	if m.cfg.DashCustomDir != "" {
		m.checkDashboardVersion(m.cfg.DashCustomDir)
		// os.OpenRoot, not os.DirFS: DirFS is documented as not being a chroot
		// and follows symlinks out of the tree, which would turn any symlink in
		// the custom directory into an unauthenticated arbitrary-read primitive.
		root, err := os.OpenRoot(m.cfg.DashCustomDir)
		if err != nil {
			m.logger.Error("dashboard custom directory unusable, falling back to embedded assets",
				"path", m.cfg.DashCustomDir, "error", err)
			assets = safeFS{dashboard.Assets}
		} else {
			assets = safeFS{root.FS()}
			m.logger.Info("dashboard serving from custom directory", "path", m.cfg.DashCustomDir)
		}
	} else {
		// The allowlist is applied to the embedded assets too. The //go:embed
		// patterns use wildcards such as css/*, which would also match a
		// dot-prefixed file added later.
		assets = safeFS{dashboard.Assets}
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
