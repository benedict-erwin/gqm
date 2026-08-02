// GQM Dashboard — scheduler.js
// Cron entries table with actions and history.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.scheduler = {
    render: function(container) {
        container.innerHTML =
            GQM.utils.pageHead({
                title: 'Scheduler',
                sub: 'Cron entries and their trigger history',
                poll: '30s'
            }) +
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

        // Close button in the history panel
        document.getElementById('cron-detail').addEventListener('click', function(e) {
            if (e.target.closest('[data-action="close-history"]')) {
                document.getElementById('cron-detail').innerHTML = '';
            }
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
                var human = GQM.utils.cronHuman(e.cron_expr || '');
                return '<tr>' +
                    '<td><b>' + GQM.utils.escapeHTML(e.id || '') + '</b></td>' +
                    '<td><span class="chip">' + GQM.utils.escapeHTML(e.cron_expr || '') + '</span>' +
                    (human ? ' <span class="dim text-sm">' + GQM.utils.escapeHTML(human) + '</span>' : '') + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(e.job_type || '') + '</td>' +
                    '<td><a href="#/queues/' + GQM.utils.escapeHTML(e.queue || '') + '">' + GQM.utils.escapeHTML(e.queue || '') + '</a></td>' +
                    '<td class="dim">' + GQM.utils.escapeHTML(e.timezone || 'UTC') + '</td>' +
                    '<td>' + statusBadge + '</td>' +
                    '<td class="actions-cell"><div class="btn-group btn-group--right">' +
                    '<button class="btn btn--sm btn--primary" data-action="trigger" data-id="' + GQM.utils.escapeHTML(e.id) + '">Trigger</button>' +
                    (enabled
                        ? '<button class="btn btn--sm" data-action="disable" data-id="' + GQM.utils.escapeHTML(e.id) + '">Disable</button>'
                        : '<button class="btn btn--sm" data-action="enable" data-id="' + GQM.utils.escapeHTML(e.id) + '">Enable</button>') +
                    '<button class="btn btn--sm btn--ghost" data-action="history" data-id="' + GQM.utils.escapeHTML(e.id) + '">History</button>' +
                    '</div></td>' +
                    '</tr>';
            }).join('');

            el.innerHTML =
                '<table><thead><tr>' +
                '<th>Entry</th><th>Schedule</th><th>Job type</th><th>Queue</th><th>Timezone</th><th>Status</th><th class="actions-col">Actions</th>' +
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

        var esc = GQM.utils.escapeHTML;
        var panelHead =
            '<div class="flex-between mb-1">' +
            '<h3 style="margin:0">History &mdash; ' + esc(id) + '</h3>' +
            '<button class="btn btn--sm btn--ghost" data-action="close-history">Close</button>' +
            '</div>';

        GQM.api.get('/api/v1/cron/' + encodeURIComponent(id) + '/history?limit=20').then(function(resp) {
            var records = resp.data || [];
            if (records.length === 0) {
                detail.innerHTML = '<div class="detail-panel">' + panelHead +
                    '<p class="text-secondary">No history yet</p></div>';
                return;
            }

            var rows = records.map(function(r) {
                return '<tr>' +
                    '<td class="mono truncate"><a href="#/jobs/' + esc(r.job_id) + '">' + esc(r.job_id) + '</a></td>' +
                    '<td class="dim">' + GQM.utils.formatTime(r.triggered_at) + '</td>' +
                    '</tr>';
            }).join('');

            detail.innerHTML =
                '<div class="detail-panel">' + panelHead +
                '<div class="table-wrap" style="margin:0;box-shadow:none"><table><thead><tr><th>Job</th><th>Triggered</th></tr></thead>' +
                '<tbody>' + rows + '</tbody></table></div></div>';
        }).catch(function() {
            detail.innerHTML = '<div class="error-state">Failed to load history</div>';
        });
    }
};
