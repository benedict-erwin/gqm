// GQM Dashboard — workers.js
// Workers list with stale detection.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.workers = {
    // Heartbeat older than 30s is considered stale.
    staleThresholdSec: 30,

    render: function(container) {
        container.innerHTML =
            '<div class="page-header"><h2>Workers</h2></div>' +
            '<div id="workers-table" class="table-wrap"><div class="loading">Loading workers</div></div>';

        GQM.app.poll(function() { GQM.pages.workers.load(); }, 10000);
    },

    load: function() {
        GQM.api.get('/api/v1/workers').then(function(resp) {
            var workers = resp.data || [];
            var el = document.getElementById('workers-table');
            if (!el) return;

            if (workers.length === 0) {
                el.innerHTML = '<div class="empty-state"><p>No workers registered</p></div>';
                return;
            }

            var now = Math.floor(Date.now() / 1000);

            var rows = workers.map(function(w) {
                var hb = parseInt(w.last_heartbeat || '0');
                var isStale = hb > 0 && (now - hb) > GQM.pages.workers.staleThresholdSec;
                var statusBadge = isStale ? GQM.utils.statusBadge('stale') : GQM.utils.statusBadge('active');

                var queues = '';
                if (Array.isArray(w.queues)) {
                    queues = w.queues.map(function(q) {
                        return '<a href="#/queues/' + GQM.utils.escapeHTML(q) + '">' + GQM.utils.escapeHTML(q) + '</a>';
                    }).join(', ');
                } else if (w.queues) {
                    queues = GQM.utils.escapeHTML(String(w.queues));
                }

                return '<tr>' +
                    '<td class="mono truncate"><a href="#/queues?pool=' + encodeURIComponent(w.pool_id || w.id || '') + '">' + GQM.utils.escapeHTML(w.pool_id || w.id || '') + '</a></td>' +
                    '<td>' + statusBadge + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(w.concurrency || '') + '</td>' +
                    '<td>' + queues + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(w.active_jobs || '0') + '</td>' +
                    '<td>' + (hb ? GQM.utils.formatRelative(hb) : '—') + '</td>' +
                    '<td>' + GQM.utils.formatTime(parseInt(w.started_at || '0')) + '</td>' +
                    '</tr>';
            }).join('');

            el.innerHTML =
                '<table><thead><tr>' +
                '<th>Pool ID</th><th>Status</th><th>Concurrency</th><th>Queues</th><th>Active Jobs</th><th>Last Heartbeat</th><th>Started</th>' +
                '</tr></thead><tbody>' + rows + '</tbody></table>';
        }).catch(function() {
            var el = document.getElementById('workers-table');
            if (el) el.innerHTML = '<div class="error-state">Failed to load workers</div>';
        });
    }
};
