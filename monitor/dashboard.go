package monitor

import "net/http"

const dashboardPlaceholderHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GQM Dashboard</title>
    <style>
        body { font-family: system-ui, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; background: #f5f5f5; }
        .container { text-align: center; padding: 2rem; }
        h1 { color: #333; }
        p { color: #666; }
        a { color: #0066cc; }
    </style>
</head>
<body>
    <div class="container">
        <h1>GQM Dashboard</h1>
        <p>Coming in Phase 7.</p>
        <p>API available at <a href="/api/v1/stats">/api/v1/</a></p>
    </div>
</body>
</html>`

// handleDashboard serves the dashboard placeholder page.
func (m *Monitor) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(dashboardPlaceholderHTML))
}
