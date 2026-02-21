// GQM Dashboard — scheduler.js
// Cron entries table with actions and history.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.scheduler = {
    render: function(container) {
        container.innerHTML =
            '<div class="page-header"><h2>Scheduler (Cron)</h2></div>' +
            '<div id="cron-table" class="table-wrap"><div class="loading">Loading cron entries</div></div>' +
            '<div id="cron-detail"></div>';

        // Event delegation for cron action buttons
        document.getElementById('cron-table').addEventListener('click', function(e) {
            var btn = e.target.closest('[data-action]');
            if (!btn) return;
            var action = btn.getAttribute('data-action');
            var id = btn.getAttribute('data-id');
            if (action === 'trigger') GQM.pages.scheduler.trigger(id);
            else if (action === 'disable') GQM.pages.scheduler.disable(id);
            else if (action === 'enable') GQM.pages.scheduler.enable(id);
            else if (action === 'history') GQM.pages.scheduler.showHistory(id);
        });

        GQM.app.poll(function() { GQM.pages.scheduler.load(); }, 30000);
    },

    load: function() {
        GQM.api.get('/api/v1/cron').then(function(resp) {
            var entries = resp.data || [];
            var el = document.getElementById('cron-table');
            if (!el) return;

            if (entries.length === 0) {
                el.innerHTML = '<div class="empty-state"><p>No cron entries registered</p></div>';
                return;
            }

            var rows = entries.map(function(e) {
                var enabled = e.enabled !== false;
                var statusBadge = enabled ? GQM.utils.statusBadge('active') : GQM.utils.statusBadge('paused');
                return '<tr>' +
                    '<td class="mono">' + GQM.utils.escapeHTML(e.id || '') + '</td>' +
                    '<td class="mono">' + GQM.utils.escapeHTML(e.cron_expr || '') + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(e.job_type || '') + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(e.queue || '') + '</td>' +
                    '<td>' + statusBadge + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(e.timezone || 'UTC') + '</td>' +
                    '<td class="btn-group">' +
                    '<button class="btn btn--sm btn--primary" data-action="trigger" data-id="' + GQM.utils.escapeHTML(e.id) + '">Trigger</button>' +
                    (enabled
                        ? '<button class="btn btn--sm" data-action="disable" data-id="' + GQM.utils.escapeHTML(e.id) + '">Disable</button>'
                        : '<button class="btn btn--sm" data-action="enable" data-id="' + GQM.utils.escapeHTML(e.id) + '">Enable</button>') +
                    '<button class="btn btn--sm" data-action="history" data-id="' + GQM.utils.escapeHTML(e.id) + '">History</button>' +
                    '</td>' +
                    '</tr>';
            }).join('');

            el.innerHTML =
                '<table><thead><tr>' +
                '<th>ID</th><th>Expression</th><th>Job Type</th><th>Queue</th><th>Status</th><th>Timezone</th><th>Actions</th>' +
                '</tr></thead><tbody>' + rows + '</tbody></table>';
        }).catch(function() {
            var el = document.getElementById('cron-table');
            if (el) el.innerHTML = '<div class="error-state">Failed to load cron entries</div>';
        });
    },

    trigger: function(id) {
        GQM.api.post('/api/v1/cron/' + encodeURIComponent(id) + '/trigger').then(function(resp) {
            var d = resp.data || {};
            GQM.utils.toast('Triggered — job ID: ' + (d.job_id || '?'), 'success');
        }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
    },

    enable: function(id) {
        GQM.api.post('/api/v1/cron/' + encodeURIComponent(id) + '/enable').then(function() {
            GQM.utils.toast('Cron enabled', 'success');
            GQM.pages.scheduler.load();
        }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
    },

    disable: function(id) {
        GQM.api.post('/api/v1/cron/' + encodeURIComponent(id) + '/disable').then(function() {
            GQM.utils.toast('Cron disabled', 'success');
            GQM.pages.scheduler.load();
        }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
    },

    showHistory: function(id) {
        var detail = document.getElementById('cron-detail');
        if (!detail) return;
        detail.innerHTML = '<div class="loading">Loading history</div>';

        GQM.api.get('/api/v1/cron/' + encodeURIComponent(id) + '/history?limit=20').then(function(resp) {
            var records = resp.data || [];
            if (records.length === 0) {
                detail.innerHTML = '<div class="detail-panel"><h3>History: ' + GQM.utils.escapeHTML(id) + '</h3><p class="text-secondary mt-1">No history yet</p></div>';
                return;
            }

            var rows = records.map(function(r) {
                return '<tr>' +
                    '<td class="mono truncate"><a href="#/jobs/' + GQM.utils.escapeHTML(r.job_id) + '">' + GQM.utils.escapeHTML(r.job_id) + '</a></td>' +
                    '<td>' + GQM.utils.formatTime(r.triggered_at) + '</td>' +
                    '</tr>';
            }).join('');

            detail.innerHTML =
                '<div class="detail-panel"><h3 class="mb-1">History: ' + GQM.utils.escapeHTML(id) + '</h3>' +
                '<div class="table-wrap"><table><thead><tr><th>Job ID</th><th>Triggered At</th></tr></thead>' +
                '<tbody>' + rows + '</tbody></table></div></div>';
        }).catch(function() {
            detail.innerHTML = '<div class="error-state">Failed to load history</div>';
        });
    }
};
