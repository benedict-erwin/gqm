// GQM Dashboard — jobs.js
// Queue detail (jobs list by status) and individual job detail.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.jobs = {
    currentQueue: '',
    currentStatus: 'all',
    currentPage: 1,
    searchFilter: '',

    // Render queue detail: queue stats + job table by status.
    renderQueueDetail: function(container, queueName) {
        GQM.pages.jobs.currentQueue = queueName;
        GQM.pages.jobs.currentPage = 1;
        GQM.pages.jobs.searchFilter = '';

        container.innerHTML =
            '<div class="page-header">' +
            '<h2><a href="#/queues">Queues</a> / ' + GQM.utils.escapeHTML(queueName) + '</h2>' +
            '</div>' +
            '<div id="queue-stats" class="stat-grid"></div>' +
            '<div class="filter-bar">' +
            '<div class="filter-group">' +
            '<label>Filter by status:</label>' +
            '<select id="status-filter" onchange="GQM.pages.jobs.changeStatus(this.value)">' +
            '<option value="all">All</option>' +
            '<option value="ready">Ready</option>' +
            '<option value="processing">Processing</option>' +
            '<option value="completed">Completed</option>' +
            '<option value="dead_letter">Dead Letter</option>' +
            '</select>' +
            '</div>' +
            '<div class="filter-group">' +
            '<label>Search job ID:</label>' +
            '<input type="text" id="job-id-filter" placeholder="Filter by job ID..." autocomplete="off">' +
            '</div>' +
            '</div>' +
            '<div id="jobs-table" class="table-wrap"><div class="loading">Loading jobs</div></div>' +
            '<div id="jobs-pagination"></div>';

        // Client-side job ID filter
        var filterInput = document.getElementById('job-id-filter');
        if (filterInput) {
            filterInput.addEventListener('input', function() {
                GQM.pages.jobs.searchFilter = filterInput.value.trim().toLowerCase();
                GQM.pages.jobs.applySearchFilter();
            });
        }

        GQM.pages.jobs.loadQueueStats(queueName);
        GQM.app.poll(function() { GQM.pages.jobs.loadJobs(); }, 5000);
    },

    loadQueueStats: function(name) {
        GQM.api.get('/api/v1/queues/' + encodeURIComponent(name)).then(function(resp) {
            var d = resp.data || {};
            var el = document.getElementById('queue-stats');
            if (!el) return;
            var pausedBadge = d.paused ? ' ' + GQM.utils.statusBadge('paused') : '';
            el.innerHTML =
                '<div class="stat-card"><div class="label">Status</div><div class="value">' + (d.paused ? 'Paused' : 'Active') + '</div></div>' +
                '<div class="stat-card"><div class="label">Ready</div><div class="value">' + (d.ready || 0) + '</div></div>' +
                '<div class="stat-card"><div class="label">Processing</div><div class="value">' + (d.processing || 0) + '</div></div>' +
                '<div class="stat-card"><div class="label">Completed</div><div class="value">' + (d.completed || 0) + '</div></div>' +
                '<div class="stat-card"><div class="label">Dead Letter</div><div class="value">' + (d.dead_letter || 0) + '</div></div>';
        }).catch(function() {});
    },

    changeStatus: function(status) {
        GQM.pages.jobs.currentStatus = status;
        GQM.pages.jobs.currentPage = 1;
        GQM.pages.jobs.loadJobs();
    },

    loadJobs: function() {
        var q = GQM.pages.jobs.currentQueue;
        var status = GQM.pages.jobs.currentStatus;
        var page = GQM.pages.jobs.currentPage;

        if (status === 'all') {
            GQM.pages.jobs.loadAllJobs(q);
            return;
        }

        var url = '/api/v1/queues/' + encodeURIComponent(q) + '/jobs?status=' + status + '&page=' + page + '&limit=20';

        GQM.api.get(url).then(function(resp) {
            var jobs = resp.data || [];
            var meta = resp.meta || {};
            var el = document.getElementById('jobs-table');
            if (!el) return;

            if (jobs.length === 0) {
                el.innerHTML = '<div class="empty-state"><p>No ' + status + ' jobs</p></div>';
                document.getElementById('jobs-pagination').innerHTML = '';
                return;
            }

            GQM.pages.jobs.renderJobsTable(el, jobs);

            // Pagination
            var pagEl = document.getElementById('jobs-pagination');
            if (pagEl) {
                pagEl.innerHTML = GQM.utils.paginationHTML(meta.page || 1, meta.limit || 20, meta.total || 0);
                pagEl.addEventListener('click', function(e) {
                    var btn = e.target.closest('[data-page]');
                    if (btn && !btn.disabled) {
                        GQM.pages.jobs.currentPage = parseInt(btn.getAttribute('data-page'));
                        GQM.pages.jobs.loadJobs();
                    }
                });
            }

            GQM.pages.jobs.applySearchFilter();
        }).catch(function() {
            var el = document.getElementById('jobs-table');
            if (el) el.innerHTML = '<div class="error-state">Failed to load jobs</div>';
        });
    },

    // Load jobs from all statuses and merge (client-side).
    loadAllJobs: function(queueName) {
        var enc = encodeURIComponent(queueName);
        var statuses = ['ready', 'processing', 'completed', 'dead_letter'];
        var promises = statuses.map(function(s) {
            return GQM.api.get('/api/v1/queues/' + enc + '/jobs?status=' + s + '&page=1&limit=50')
                .then(function(resp) { return resp.data || []; })
                .catch(function() { return []; });
        });

        Promise.all(promises).then(function(results) {
            var el = document.getElementById('jobs-table');
            if (!el) return;

            // Merge all results
            var allJobs = [];
            for (var i = 0; i < results.length; i++) {
                allJobs = allJobs.concat(results[i]);
            }

            if (allJobs.length === 0) {
                el.innerHTML = '<div class="empty-state"><p>No jobs in this queue</p></div>';
                document.getElementById('jobs-pagination').innerHTML = '';
                return;
            }

            // Sort by created_at descending
            allJobs.sort(function(a, b) {
                var ta = a.created_at || '';
                var tb = b.created_at || '';
                return ta > tb ? -1 : ta < tb ? 1 : 0;
            });

            // Limit to 100 most recent
            if (allJobs.length > 100) allJobs = allJobs.slice(0, 100);

            GQM.pages.jobs.renderJobsTable(el, allJobs);

            // No server-side pagination in "all" mode
            var pagEl = document.getElementById('jobs-pagination');
            if (pagEl) {
                pagEl.innerHTML = allJobs.length >= 100
                    ? '<div class="pagination-note">Showing 100 most recent jobs. Select a specific status for full pagination.</div>'
                    : '';
            }

            GQM.pages.jobs.applySearchFilter();
        });
    },

    // Render a table of jobs into the target element.
    renderJobsTable: function(el, jobs) {
        var rows = jobs.map(function(j) {
            return '<tr data-job-id="' + GQM.utils.escapeHTML(j.id).toLowerCase() + '">' +
                '<td class="mono truncate"><a href="#/jobs/' + GQM.utils.escapeHTML(j.id) + '">' + GQM.utils.escapeHTML(j.id) + '</a></td>' +
                '<td>' + GQM.utils.escapeHTML(j.type || '') + '</td>' +
                '<td>' + GQM.utils.statusBadge(j.status) + '</td>' +
                '<td>' + GQM.utils.formatTime(j.created_at) + '</td>' +
                '<td>' + GQM.utils.escapeHTML(j.error || '') + '</td>' +
                '</tr>';
        }).join('');

        el.innerHTML =
            '<table><thead><tr>' +
            '<th>Job ID</th><th>Type</th><th>Status</th><th>Created</th><th>Error</th>' +
            '</tr></thead><tbody>' + rows + '</tbody></table>';
    },

    // Apply client-side search filter to table rows by data-job-id attribute.
    applyFilter: function(tableId, filter) {
        var el = document.getElementById(tableId);
        if (!el) return;

        var rows = el.querySelectorAll('tbody tr');
        var visible = 0;
        for (var i = 0; i < rows.length; i++) {
            var jobId = rows[i].getAttribute('data-job-id') || '';
            if (!filter || jobId.indexOf(filter) !== -1) {
                rows[i].style.display = '';
                visible++;
            } else {
                rows[i].style.display = 'none';
            }
        }

        // Show/hide "no results" message
        var noResults = el.querySelector('.filter-no-results');
        if (filter && visible === 0) {
            if (!noResults) {
                noResults = document.createElement('div');
                noResults.className = 'filter-no-results empty-state';
                noResults.innerHTML = '<p>No jobs matching "' + GQM.utils.escapeHTML(filter) + '"</p>';
                el.appendChild(noResults);
            }
        } else if (noResults) {
            noResults.remove();
        }
    },

    // Shortcut for queue detail page filter.
    applySearchFilter: function() {
        GQM.pages.jobs.applyFilter('jobs-table', GQM.pages.jobs.searchFilter);
    },

    // Render jobs by status across all queues.
    statusSearchFilter: '',

    renderJobsByStatus: function(container, status) {
        var statusLabel = status.replace(/_/g, ' ').replace(/\b\w/g, function(c) { return c.toUpperCase(); });
        GQM.pages.jobs.statusSearchFilter = '';

        container.innerHTML =
            '<div class="page-header">' +
            '<h2><a href="#/">Overview</a> / ' + GQM.utils.escapeHTML(statusLabel) + ' Jobs</h2>' +
            '</div>' +
            '<div class="filter-bar">' +
            '<div class="filter-group">' +
            '<label>Search job ID:</label>' +
            '<input type="text" id="status-job-filter" placeholder="Filter by job ID..." autocomplete="off">' +
            '</div>' +
            '</div>' +
            '<div id="jobs-table" class="table-wrap"><div class="loading">Loading jobs</div></div>';

        var filterInput = document.getElementById('status-job-filter');
        if (filterInput) {
            filterInput.addEventListener('input', function() {
                GQM.pages.jobs.statusSearchFilter = filterInput.value.trim().toLowerCase();
                GQM.pages.jobs.applyFilter('jobs-table', GQM.pages.jobs.statusSearchFilter);
            });
        }

        GQM.pages.jobs.loadJobsByStatus(status);
        GQM.app.poll(function() { GQM.pages.jobs.loadJobsByStatus(status); }, 10000);
    },

    loadJobsByStatus: function(status) {
        // Step 1: get all queues
        GQM.api.get('/api/v1/queues').then(function(resp) {
            var queues = resp.data || [];
            if (queues.length === 0) {
                var el = document.getElementById('jobs-table');
                if (el) el.innerHTML = '<div class="empty-state"><p>No queues registered</p></div>';
                return;
            }

            // Step 2: for each queue, fetch jobs with this status
            var promises = queues.map(function(q) {
                return GQM.api.get('/api/v1/queues/' + encodeURIComponent(q.name) + '/jobs?status=' + status + '&page=1&limit=50')
                    .then(function(r) {
                        // Tag each job with queue name
                        var jobs = r.data || [];
                        jobs.forEach(function(j) { j._queue = q.name; });
                        return jobs;
                    })
                    .catch(function() { return []; });
            });

            Promise.all(promises).then(function(results) {
                var el = document.getElementById('jobs-table');
                if (!el) return;

                var allJobs = [];
                for (var i = 0; i < results.length; i++) {
                    allJobs = allJobs.concat(results[i]);
                }

                if (allJobs.length === 0) {
                    el.innerHTML = '<div class="empty-state"><p>No ' + status.replace(/_/g, ' ') + ' jobs</p></div>';
                    return;
                }

                // Sort by created_at descending
                allJobs.sort(function(a, b) {
                    var ta = a.created_at || '';
                    var tb = b.created_at || '';
                    return ta > tb ? -1 : ta < tb ? 1 : 0;
                });

                if (allJobs.length > 100) allJobs = allJobs.slice(0, 100);

                // Render with queue column
                var rows = allJobs.map(function(j) {
                    var queue = j._queue || j.queue || '';
                    return '<tr data-job-id="' + GQM.utils.escapeHTML(j.id).toLowerCase() + '">' +
                        '<td class="mono truncate"><a href="#/jobs/' + GQM.utils.escapeHTML(j.id) + '">' + GQM.utils.escapeHTML(j.id) + '</a></td>' +
                        '<td><a href="#/queues/' + GQM.utils.escapeHTML(queue) + '">' + GQM.utils.escapeHTML(queue) + '</a></td>' +
                        '<td>' + GQM.utils.escapeHTML(j.type || '') + '</td>' +
                        '<td>' + GQM.utils.statusBadge(j.status) + '</td>' +
                        '<td>' + GQM.utils.formatTime(j.created_at) + '</td>' +
                        '<td>' + GQM.utils.escapeHTML(j.error || '') + '</td>' +
                        '</tr>';
                }).join('');

                el.innerHTML =
                    '<table><thead><tr>' +
                    '<th>Job ID</th><th>Queue</th><th>Type</th><th>Status</th><th>Created</th><th>Error</th>' +
                    '</tr></thead><tbody>' + rows + '</tbody></table>';

                GQM.pages.jobs.applyFilter('jobs-table', GQM.pages.jobs.statusSearchFilter);
            });
        }).catch(function() {
            var el = document.getElementById('jobs-table');
            if (el) el.innerHTML = '<div class="error-state">Failed to load queues</div>';
        });
    },

    // Render individual job detail.
    renderJobDetail: function(container, jobId) {
        container.innerHTML =
            '<div class="page-header">' +
            '<h2 id="job-breadcrumb">Job Detail</h2>' +
            '</div>' +
            '<div id="job-detail"><div class="loading">Loading job</div></div>';

        GQM.pages.jobs.loadJobDetail(jobId);
    },

    loadJobDetail: function(jobId) {
        GQM.api.get('/api/v1/jobs/' + encodeURIComponent(jobId)).then(function(resp) {
            var j = resp.data || {};
            var el = document.getElementById('job-detail');
            if (!el) return;

            // Update breadcrumb with queue context
            var bc = document.getElementById('job-breadcrumb');
            if (bc && j.queue) {
                bc.innerHTML = '<a href="#/queues">Queues</a> / <a href="#/queues/' +
                    GQM.utils.escapeHTML(j.queue) + '">' + GQM.utils.escapeHTML(j.queue) +
                    '</a> / <span class="text-secondary">' + GQM.utils.escapeHTML(j.id) + '</span>';
            }

            var payloadStr = '';
            if (j.payload != null) {
                payloadStr = typeof j.payload === 'object' ? JSON.stringify(j.payload, null, 2) : String(j.payload);
            }

            var resultStr = '';
            if (j.result != null) {
                resultStr = typeof j.result === 'object' ? JSON.stringify(j.result, null, 2) : String(j.result);
            }

            var metaStr = '';
            if (j.meta != null) {
                metaStr = typeof j.meta === 'object' ? JSON.stringify(j.meta, null, 2) : String(j.meta);
            }

            var html = '<div class="detail-panel">' +
                '<dl class="detail-grid">' +
                '<dt>ID</dt><dd class="mono">' + GQM.utils.escapeHTML(j.id) + '</dd>' +
                '<dt>Type</dt><dd>' + GQM.utils.escapeHTML(j.type) + '</dd>' +
                '<dt>Queue</dt><dd><a href="#/queues/' + GQM.utils.escapeHTML(j.queue) + '">' + GQM.utils.escapeHTML(j.queue) + '</a></dd>' +
                '<dt>Status</dt><dd>' + GQM.utils.statusBadge(j.status) + '</dd>' +
                '<dt>Created</dt><dd>' + GQM.utils.formatTime(j.created_at) + '</dd>' +
                (j.started_at ? '<dt>Started</dt><dd>' + GQM.utils.formatTime(j.started_at) + '</dd>' : '') +
                (j.completed_at ? '<dt>Completed</dt><dd>' + GQM.utils.formatTime(j.completed_at) + '</dd>' : '') +
                (j.scheduled_at ? '<dt>Scheduled</dt><dd>' + GQM.utils.formatTime(j.scheduled_at) + '</dd>' : '') +
                '<dt>Retry</dt><dd>' + (j.retry_count || 0) + ' / ' + (j.max_retry || 0) + '</dd>' +
                (j.timeout ? '<dt>Timeout</dt><dd>' + GQM.utils.formatDuration(Number(j.timeout)) + '</dd>' : '') +
                (j.execution_duration ? '<dt>Duration</dt><dd>' + GQM.utils.formatDuration(Number(j.execution_duration)) + '</dd>' : '') +
                (j.worker_id ? '<dt>Worker</dt><dd class="mono">' + GQM.utils.escapeHTML(j.worker_id) + '</dd>' : '') +
                (j.enqueued_by ? '<dt>Enqueued By</dt><dd>' + GQM.utils.escapeHTML(j.enqueued_by) + '</dd>' : '') +
                (j.error ? '<dt>Error</dt><dd style="color:var(--color-danger)">' + GQM.utils.escapeHTML(j.error) + '</dd>' : '') +
                '</dl>' +
                '</div>';

            if (payloadStr) {
                html += '<div class="detail-panel"><h3 class="mb-1">Payload</h3><pre>' + GQM.utils.escapeHTML(payloadStr) + '</pre></div>';
            }
            if (resultStr) {
                html += '<div class="detail-panel"><h3 class="mb-1">Result</h3><pre>' + GQM.utils.escapeHTML(resultStr) + '</pre></div>';
            }
            if (metaStr) {
                html += '<div class="detail-panel"><h3 class="mb-1">Meta</h3><pre>' + GQM.utils.escapeHTML(metaStr) + '</pre></div>';
            }

            // Action buttons — Back always first (leftmost)
            html += '<div class="btn-group">';
            html += '<button class="btn btn--secondary" onclick="window.history.back()">&laquo; Back</button>';
            if (j.status === 'dead_letter' || j.status === 'failed' || j.status === 'canceled') {
                html += '<button class="btn btn--primary" onclick="GQM.pages.jobs.retry(\'' + GQM.utils.escapeHTML(j.id) + '\')">Retry</button>';
            }
            if (j.status === 'ready' || j.status === 'processing' || j.status === 'deferred') {
                html += '<button class="btn btn--danger" onclick="GQM.pages.jobs.cancel(\'' + GQM.utils.escapeHTML(j.id) + '\')">Cancel</button>';
            }
            html += '<button class="btn btn--danger" onclick="GQM.pages.jobs.deleteJob(\'' + GQM.utils.escapeHTML(j.id) + '\')">Delete</button>';
            html += '</div>';

            el.innerHTML = html;
        }).catch(function(err) {
            var el = document.getElementById('job-detail');
            if (el) el.innerHTML = '<div class="error-state">Job not found</div>';
        });
    },

    retry: function(jobId) {
        GQM.api.post('/api/v1/jobs/' + encodeURIComponent(jobId) + '/retry').then(function() {
            GQM.utils.toast('Job queued for retry', 'success');
            GQM.pages.jobs.loadJobDetail(jobId);
        }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
    },

    cancel: function(jobId) {
        GQM.utils.confirm('Cancel Job', 'Cancel job ' + jobId + '?').then(function(ok) {
            if (!ok) return;
            GQM.api.post('/api/v1/jobs/' + encodeURIComponent(jobId) + '/cancel').then(function() {
                GQM.utils.toast('Job canceled', 'success');
                GQM.pages.jobs.loadJobDetail(jobId);
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
    },

    deleteJob: function(jobId) {
        GQM.utils.confirm('Delete Job', 'Permanently delete job ' + jobId + '? This cannot be undone.').then(function(ok) {
            if (!ok) return;
            GQM.api.del('/api/v1/jobs/' + encodeURIComponent(jobId)).then(function() {
                GQM.utils.toast('Job deleted', 'success');
                window.history.back();
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
    }
};
