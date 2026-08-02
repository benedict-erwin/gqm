// GQM Dashboard — servers.js
// Server instances list with status and pool info.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.servers = {
    render: function(container) {
        container.innerHTML =
            GQM.utils.pageHead({
                title: 'Servers',
                sub: 'Registered server instances and their pools',
                poll: '10s'
            }) +
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
                if (Array.isArray(s.pools) && s.pools.length > 0) {
                    pools = s.pools.map(function(p) {
                        return '<a href="#/queues?pool=' + encodeURIComponent(p) + '">' + GQM.utils.escapeHTML(p) + '</a>';
                    }).join(', ');
                } else {
                    pools = '<span class="dim">' + GQM.utils.escapeHTML(s.num_pools || '0') + '</span>';
                }

                return '<tr>' +
                    '<td class="mono truncate" title="' + GQM.utils.escapeHTML(s.id || '') + '">' + GQM.utils.escapeHTML(s.id || '') + '</td>' +
                    '<td>' + statusBadge + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(s.hostname || '') + '</td>' +
                    '<td class="mono num">' + GQM.utils.escapeHTML(s.pid || '') + '</td>' +
                    '<td>' + pools + '</td>' +
                    '<td class="num">' + GQM.utils.escapeHTML(s.concurrency_total || '0') + '</td>' +
                    '<td class="mono">' + GQM.utils.escapeHTML(s.go_version || '') + '</td>' +
                    '<td class="dim" title="' + GQM.utils.escapeHTML(GQM.utils.formatTime(hb)) + '">' + (hb ? GQM.utils.formatRelative(hb) : '—') + '</td>' +
                    '<td class="dim">' + GQM.utils.formatTime(parseInt(s.started_at || '0')) + '</td>' +
                    '</tr>';
            }).join('');

            el.innerHTML =
                '<table><thead><tr>' +
                '<th>Server</th><th>Status</th><th>Host</th><th class="num">PID</th><th>Pools</th><th class="num">Concurrency</th><th>Go</th><th>Heartbeat</th><th>Started</th>' +
                '</tr></thead><tbody>' + rows + '</tbody></table>';
        }).catch(function() {
            var el = document.getElementById('servers-table');
            if (el) el.innerHTML = '<div class="error-state">Failed to load servers</div>';
        });
    }
};
