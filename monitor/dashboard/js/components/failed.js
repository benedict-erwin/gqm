// GQM Dashboard — failed.js
// DLQ browser with bulk retry/delete operations.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.failed = {
    selectedQueue: '',
    selectedJobs: [],
    currentPage: 1,
    searchFilter: '',

    render: function(container) {
        GQM.pages.failed.searchFilter = '';

        container.innerHTML =
            '<div class="page-header"><h2>Failed / Dead Letter</h2></div>' +
            '<div class="filter-bar">' +
            '<div class="filter-group">' +
            '<label>Queue:</label>' +
            '<select id="dlq-queue-select" onchange="GQM.pages.failed.selectQueue(this.value)">' +
            '<option value="">Select queue...</option>' +
            '</select>' +
            '</div>' +
            '<div class="filter-group">' +
            '<label>Search job ID:</label>' +
            '<input type="text" id="dlq-job-filter" placeholder="Filter by job ID..." autocomplete="off">' +
            '</div>' +
            '</div>' +
            '<div id="dlq-bulk-actions" class="btn-group mb-2" style="display:none">' +
            '<button class="btn btn--sm btn--primary" onclick="GQM.pages.failed.bulkRetry()">Retry Selected</button>' +
            '<button class="btn btn--sm btn--danger" onclick="GQM.pages.failed.bulkDelete()">Delete Selected</button>' +
            '<button class="btn btn--sm" onclick="GQM.pages.failed.retryAll()">Retry All</button>' +
            '<button class="btn btn--sm btn--danger" onclick="GQM.pages.failed.clearAll()">Clear All</button>' +
            '</div>' +
            '<div id="dlq-table" class="table-wrap"><div class="empty-state"><p>Select a queue to view dead letter jobs</p></div></div>' +
            '<div id="dlq-pagination"></div>';

        // Job ID filter
        var filterInput = document.getElementById('dlq-job-filter');
        if (filterInput) {
            filterInput.addEventListener('input', function() {
                GQM.pages.failed.searchFilter = filterInput.value.trim().toLowerCase();
                GQM.pages.jobs.applyFilter('dlq-table', GQM.pages.failed.searchFilter);
            });
        }

        // Load queue list
        GQM.api.get('/api/v1/queues').then(function(resp) {
            var queues = resp.data || [];
            var sel = document.getElementById('dlq-queue-select');
            if (!sel) return;
            queues.forEach(function(q) {
                var opt = document.createElement('option');
                opt.value = q.name;
                opt.textContent = q.name + (q.dead_letter > 0 ? ' (' + q.dead_letter + ')' : '');
                sel.appendChild(opt);
            });
        });
    },

    selectQueue: function(name) {
        GQM.pages.failed.selectedQueue = name;
        GQM.pages.failed.selectedJobs = [];
        GQM.pages.failed.currentPage = 1;
        if (name) {
            document.getElementById('dlq-bulk-actions').style.display = '';
            GQM.app.poll(function() { GQM.pages.failed.loadDLQ(); }, 15000);
        } else {
            document.getElementById('dlq-bulk-actions').style.display = 'none';
            document.getElementById('dlq-table').innerHTML = '<div class="empty-state"><p>Select a queue</p></div>';
        }
    },

    loadDLQ: function() {
        var q = GQM.pages.failed.selectedQueue;
        if (!q) return;
        var page = GQM.pages.failed.currentPage;

        GQM.api.get('/api/v1/queues/' + encodeURIComponent(q) + '/dead-letter?page=' + page + '&limit=20').then(function(resp) {
            var jobs = resp.data || [];
            var meta = resp.meta || {};
            var el = document.getElementById('dlq-table');
            if (!el) return;

            if (jobs.length === 0) {
                el.innerHTML = '<div class="empty-state"><p>No dead letter jobs</p></div>';
                document.getElementById('dlq-pagination').innerHTML = '';
                return;
            }

            var rows = jobs.map(function(j) {
                var checked = GQM.pages.failed.selectedJobs.indexOf(j.id) >= 0 ? ' checked' : '';
                return '<tr data-job-id="' + GQM.utils.escapeHTML(j.id).toLowerCase() + '">' +
                    '<td class="checkbox-col"><input type="checkbox" data-id="' + GQM.utils.escapeHTML(j.id) + '"' + checked + ' onchange="GQM.pages.failed.toggleJob(this)"></td>' +
                    '<td class="mono truncate"><a href="#/jobs/' + GQM.utils.escapeHTML(j.id) + '">' + GQM.utils.escapeHTML(j.id) + '</a></td>' +
                    '<td>' + GQM.utils.escapeHTML(j.type || '') + '</td>' +
                    '<td>' + GQM.utils.escapeHTML(j.error || '') + '</td>' +
                    '<td>' + (j.retry_count || 0) + '/' + (j.max_retry || 0) + '</td>' +
                    '<td>' + GQM.utils.formatTime(j.created_at) + '</td>' +
                    '</tr>';
            }).join('');

            el.innerHTML =
                '<table><thead><tr>' +
                '<th class="checkbox-col"><input type="checkbox" onchange="GQM.pages.failed.toggleAll(this)"></th>' +
                '<th>Job ID</th><th>Type</th><th>Error</th><th>Retries</th><th>Created</th>' +
                '</tr></thead><tbody>' + rows + '</tbody></table>';

            var pagEl = document.getElementById('dlq-pagination');
            if (pagEl) {
                pagEl.innerHTML = GQM.utils.paginationHTML(meta.page || 1, meta.limit || 20, meta.total || 0);
                pagEl.addEventListener('click', function(e) {
                    var btn = e.target.closest('[data-page]');
                    if (btn && !btn.disabled) {
                        GQM.pages.failed.currentPage = parseInt(btn.getAttribute('data-page'));
                        GQM.pages.failed.loadDLQ();
                    }
                });
            }

            GQM.pages.jobs.applyFilter('dlq-table', GQM.pages.failed.searchFilter);
        }).catch(function() {
            var el = document.getElementById('dlq-table');
            if (el) el.innerHTML = '<div class="error-state">Failed to load DLQ</div>';
        });
    },

    toggleJob: function(checkbox) {
        var id = checkbox.getAttribute('data-id');
        var idx = GQM.pages.failed.selectedJobs.indexOf(id);
        if (checkbox.checked && idx < 0) {
            GQM.pages.failed.selectedJobs.push(id);
        } else if (!checkbox.checked && idx >= 0) {
            GQM.pages.failed.selectedJobs.splice(idx, 1);
        }
    },

    toggleAll: function(checkbox) {
        var boxes = document.querySelectorAll('#dlq-table input[data-id]');
        GQM.pages.failed.selectedJobs = [];
        boxes.forEach(function(cb) {
            cb.checked = checkbox.checked;
            if (checkbox.checked) {
                GQM.pages.failed.selectedJobs.push(cb.getAttribute('data-id'));
            }
        });
    },

    bulkRetry: function() {
        var ids = GQM.pages.failed.selectedJobs;
        if (ids.length === 0) { GQM.utils.toast('No jobs selected', 'info'); return; }
        GQM.api.post('/api/v1/jobs/batch/retry', { job_ids: ids }).then(function(resp) {
            var d = resp.data || {};
            GQM.utils.toast('Retried ' + (d.succeeded || 0) + ' jobs', 'success');
            GQM.pages.failed.selectedJobs = [];
            GQM.pages.failed.loadDLQ();
        }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
    },

    bulkDelete: function() {
        var ids = GQM.pages.failed.selectedJobs;
        if (ids.length === 0) { GQM.utils.toast('No jobs selected', 'info'); return; }
        GQM.utils.confirm('Delete Jobs', 'Delete ' + ids.length + ' jobs permanently?').then(function(ok) {
            if (!ok) return;
            GQM.api.post('/api/v1/jobs/batch/delete', { job_ids: ids }).then(function(resp) {
                var d = resp.data || {};
                GQM.utils.toast('Deleted ' + (d.succeeded || 0) + ' jobs', 'success');
                GQM.pages.failed.selectedJobs = [];
                GQM.pages.failed.loadDLQ();
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
    },

    retryAll: function() {
        var q = GQM.pages.failed.selectedQueue;
        GQM.utils.confirm('Retry All', 'Retry ALL dead letter jobs in "' + q + '"?').then(function(ok) {
            if (!ok) return;
            GQM.api.post('/api/v1/queues/' + encodeURIComponent(q) + '/dead-letter/retry-all').then(function(resp) {
                var d = resp.data || {};
                GQM.utils.toast('Retried ' + (d.retried || 0) + ' jobs', 'success');
                GQM.pages.failed.loadDLQ();
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
    },

    clearAll: function() {
        var q = GQM.pages.failed.selectedQueue;
        GQM.utils.confirm('Clear DLQ', 'Permanently delete ALL dead letter jobs in "' + q + '"? This cannot be undone.').then(function(ok) {
            if (!ok) return;
            GQM.api.del('/api/v1/queues/' + encodeURIComponent(q) + '/dead-letter/clear').then(function(resp) {
                var d = resp.data || {};
                GQM.utils.toast('Cleared ' + (d.cleared || 0) + ' jobs', 'success');
                GQM.pages.failed.loadDLQ();
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
    }
};
