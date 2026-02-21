// GQM Dashboard — queues.js
// Queue list and queue detail (jobs by status).
// Supports pool filtering via ?pool=<name> query param.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.queues = {
    poolFilter: null,   // current pool filter name
    poolQueues: null,    // array of queue names for the filtered pool

    render: function(container) {
        var r = GQM.utils.parseRoute();
        var poolName = (r.query && r.query.pool) || null;
        GQM.pages.queues.poolFilter = poolName;
        GQM.pages.queues.poolQueues = null;

        var header = '<div class="page-header"><h2>Queues</h2></div>';
        if (poolName) {
            header = '<div class="page-header">' +
                '<h2>Queues &mdash; ' + GQM.utils.escapeHTML(poolName) + '</h2>' +
                '<a href="#/queues" class="btn btn--sm">Show all queues</a>' +
                '</div>';
        }

        container.innerHTML =
            header +
            '<div id="queues-table" class="table-wrap"><div class="loading">Loading queues</div></div>';

        if (poolName) {
            // Fetch workers to resolve pool → queues mapping, then load queues
            GQM.api.get('/api/v1/workers').then(function(resp) {
                var workers = resp.data || [];
                var match = null;
                for (var i = 0; i < workers.length; i++) {
                    if ((workers[i].pool_id || workers[i].id) === poolName) {
                        match = workers[i];
                        break;
                    }
                }
                if (match && Array.isArray(match.queues)) {
                    GQM.pages.queues.poolQueues = match.queues;
                } else {
                    GQM.pages.queues.poolQueues = [];
                }
                GQM.app.poll(function() { GQM.pages.queues.load(); }, 10000);
            }).catch(function() {
                GQM.pages.queues.poolQueues = [];
                GQM.app.poll(function() { GQM.pages.queues.load(); }, 10000);
            });
        } else {
            GQM.app.poll(function() { GQM.pages.queues.load(); }, 10000);
        }
    },

    load: function() {
        GQM.api.get('/api/v1/queues').then(function(resp) {
            var queues = resp.data || [];
            var el = document.getElementById('queues-table');
            if (!el) return;

            // Apply pool filter if active
            var poolQueues = GQM.pages.queues.poolQueues;
            if (poolQueues !== null) {
                queues = queues.filter(function(q) {
                    return poolQueues.indexOf(q.name) !== -1;
                });
            }

            if (queues.length === 0) {
                var msg = GQM.pages.queues.poolFilter
                    ? 'No queues found for pool "' + GQM.utils.escapeHTML(GQM.pages.queues.poolFilter) + '"'
                    : 'No queues registered';
                el.innerHTML = '<div class="empty-state"><p>' + msg + '</p></div>';
                return;
            }

            var rows = queues.map(function(q) {
                var pausedBadge = q.paused ? ' ' + GQM.utils.statusBadge('paused') : '';
                return '<tr>' +
                    '<td><a href="#/queues/' + GQM.utils.escapeHTML(q.name) + '">' + GQM.utils.escapeHTML(q.name) + '</a>' + pausedBadge + '</td>' +
                    '<td>' + q.ready + '</td>' +
                    '<td>' + q.processing + '</td>' +
                    '<td>' + q.completed + '</td>' +
                    '<td>' + q.dead_letter + '</td>' +
                    '<td>' + GQM.utils.formatNumber(q.processed_total) + '</td>' +
                    '<td>' + GQM.utils.formatNumber(q.failed_total) + '</td>' +
                    '<td class="btn-group">' +
                    (q.paused
                        ? '<button class="btn btn--sm" onclick="GQM.pages.queues.resume(\'' + GQM.utils.escapeHTML(q.name) + '\')">Resume</button>'
                        : '<button class="btn btn--sm" onclick="GQM.pages.queues.pause(\'' + GQM.utils.escapeHTML(q.name) + '\')">Pause</button>') +
                    '<button class="btn btn--sm btn--danger" onclick="GQM.pages.queues.empty(\'' + GQM.utils.escapeHTML(q.name) + '\')">Empty</button>' +
                    '</td>' +
                    '</tr>';
            }).join('');

            el.innerHTML =
                '<table><thead><tr>' +
                '<th>Queue</th><th>Ready</th><th>Processing</th><th>Completed</th><th>Dead Letter</th><th>Processed</th><th>Failed</th><th>Actions</th>' +
                '</tr></thead><tbody>' + rows + '</tbody></table>';
        }).catch(function(err) {
            var el = document.getElementById('queues-table');
            if (el) el.innerHTML = '<div class="error-state">Failed to load queues</div>';
        });
    },

    pause: function(name) {
        GQM.utils.confirm('Pause Queue', 'Pause queue "' + name + '"? Workers will stop dequeuing.').then(function(ok) {
            if (!ok) return;
            GQM.api.post('/api/v1/queues/' + encodeURIComponent(name) + '/pause').then(function() {
                GQM.utils.toast('Queue paused', 'success');
                GQM.pages.queues.load();
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
    },

    resume: function(name) {
        GQM.api.post('/api/v1/queues/' + encodeURIComponent(name) + '/resume').then(function() {
            GQM.utils.toast('Queue resumed', 'success');
            GQM.pages.queues.load();
        }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
    },

    empty: function(name) {
        GQM.utils.confirm('Empty Queue', 'Delete ALL ready jobs from "' + name + '"? This cannot be undone.').then(function(ok) {
            if (!ok) return;
            GQM.api.del('/api/v1/queues/' + encodeURIComponent(name) + '/empty').then(function(resp) {
                var removed = (resp.data && resp.data.removed) || 0;
                GQM.utils.toast('Removed ' + removed + ' jobs', 'success');
                GQM.pages.queues.load();
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
    }
};
