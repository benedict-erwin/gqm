// GQM Dashboard — servers.js
// Server instances list with status and pool info.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.servers = {
    render: function(container) {
        container.innerHTML =
            '<div class="page-header"><h2>Servers</h2></div>' +
            '<div id="servers-table" class="table-wrap"><div class="loading">Loading servers</div></div>';

        GQM.app.poll(function() { GQM.pages.servers.load(); }, 10000);
    },

    load: function() {
        GQM.api.get('/api/v1/servers').then(function(resp) {
            var servers = resp.data || [];
            var el = document.getElementById('servers-table');
            if (!el) return;

            if (servers.length === 0) {
                el.innerHTML = '<div class="empty-state"><p>No servers registered</p></div>';
                return;
            }

            var now = Math.floor(Date.now() / 1000);

            var rows = servers.map(function(s) {
                var hb = parseInt(s.last_heartbeat || '0');
                var isStale = hb > 0 && (now - hb) > 35; // server heartbeat = 10s, stale after ~3x
                var statusBadge = isStale ? GQM.utils.statusBadge('stale') : GQM.utils.statusBadge('active');

                var pools = '';
                if (Array.isArray(s.pools)) {
                    pools = s.pools.map(function(p) {
                        return '<a href="#/queues?pool=' + encodeURIComponent(p) + '">' + GQM.utils.escapeHTML(p) + '</a>';
                    }).join(', ');
                }

                return '<tr>' +
                    '<td class="mono truncate">' + GQM.utils.escapeHTML(s.id || '') + '</td>' +
                    '<td>' + statusBadge + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(s.hostname || '') + '</td>' +
                    '<td class="mono">' + GQM.utils.escapeHTML(s.pid || '') + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(s.num_pools || '0') + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(s.concurrency_total || '0') + '</td>' +
                    '<td>' + pools + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(s.go_version || '') + '</td>' +
                    '<td>' + (hb ? GQM.utils.formatRelative(hb) : '—') + '</td>' +
                    '<td>' + GQM.utils.formatTime(parseInt(s.started_at || '0')) + '</td>' +
                    '</tr>';
            }).join('');

            el.innerHTML =
                '<table><thead><tr>' +
                '<th>Server ID</th><th>Status</th><th>Hostname</th><th>PID</th><th>Pools</th><th>Concurrency</th><th>Pool Names</th><th>Go</th><th>Last Heartbeat</th><th>Started</th>' +
                '</tr></thead><tbody>' + rows + '</tbody></table>';
        }).catch(function() {
            var el = document.getElementById('servers-table');
            if (el) el.innerHTML = '<div class="error-state">Failed to load servers</div>';
        });
    }
};
