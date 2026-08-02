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
        GQM.pages.jobs.currentStatus = 'all';
        GQM.pages.jobs.currentPage = 1;
        GQM.pages.jobs.searchFilter = '';

        var esc = GQM.utils.escapeHTML;

        container.innerHTML =
            GQM.utils.pageHead({
                crumbs: '<a href="#/queues">Queues</a> / ',
                title: esc(queueName) + ' <span id="qd-status-pill"></span>',
                poll: '5s',
                right: '<span id="qd-actions" class="btn-group"></span>'
            }) +
            '<div id="queue-stats" class="stat-grid"></div>' +
            '<div class="filter-bar">' +
            '<div class="seg" id="qd-status-seg" role="tablist">' +
            '<button class="on" data-status="all">All</button>' +
            '<button data-status="ready">Ready</button>' +
            '<button data-status="processing">Processing</button>' +
            '<button data-status="completed">Completed</button>' +
            '<button data-status="dead_letter">Dead letter</button>' +
            '</div>' +
            '<div class="filter-group">' +
            '<input type="text" id="job-id-filter" placeholder="Filter by job ID..." autocomplete="off" aria-label="Filter by job ID">' +
            '</div>' +
            '</div>' +
            '<div id="jobs-table" class="table-wrap"><div class="loading">Loading jobs</div></div>' +
            '<div id="jobs-pagination"></div>';

        // Status segmented control
        document.getElementById('qd-status-seg').addEventListener('click', function(e) {
            var btn = e.target.closest('[data-status]');
            if (!btn) return;
            this.querySelectorAll('button').forEach(function(b) { b.classList.remove('on'); });
            btn.classList.add('on');
            GQM.pages.jobs.changeStatus(btn.getAttribute('data-status'));
        });

        // Client-side job ID filter
        var filterInput = document.getElementById('job-id-filter');
        if (filterInput) {
            filterInput.addEventListener('input', function() {
                GQM.pages.jobs.searchFilter = filterInput.value.trim().toLowerCase();
                GQM.pages.jobs.applySearchFilter();
            });
        }

        // Pagination — attached once; loadJobs only rewrites the innerHTML.
        document.getElementById('jobs-pagination').addEventListener('click', function(e) {
            var btn = e.target.closest('[data-page]');
            if (btn && !btn.disabled) {
                GQM.pages.jobs.currentPage = parseInt(btn.getAttribute('data-page'));
                GQM.pages.jobs.loadJobs();
            }
        });

        // Header queue actions (pause/resume/empty)
        document.getElementById('qd-actions').addEventListener('click', function(e) {
            var btn = e.target.closest('[data-action]');
            if (!btn) return;
            var action = btn.getAttribute('data-action');
            if (action === 'pause') GQM.pages.jobs.pauseQueue();
            else if (action === 'resume') GQM.pages.jobs.resumeQueue();
            else if (action === 'empty') GQM.pages.jobs.emptyQueue();
        });

        GQM.pages.jobs.loadQueueStats(queueName);
        GQM.app.poll(function() { GQM.pages.jobs.loadJobs(); }, 5000);
    },

    loadQueueStats: function(name) {
        GQM.api.get('/api/v1/queues/' + encodeURIComponent(name)).then(function(resp) {
            var d = resp.data || {};

            var pill = document.getElementById('qd-status-pill');
            if (pill) pill.innerHTML = d.paused ? GQM.utils.statusBadge('paused') : GQM.utils.statusBadge('active');

            var actions = document.getElementById('qd-actions');
            if (actions) {
                actions.innerHTML =
                    (d.paused
                        ? '<button class="btn btn--sm" data-action="resume">Resume</button>'
                        : '<button class="btn btn--sm" data-action="pause">Pause</button>') +
                    '<button class="btn btn--sm btn--danger" data-action="empty">Empty</button>';
            }

            var el = document.getElementById('queue-stats');
            if (!el) return;
            var card = function(label, value, tone) {
                return '<div class="stat-card' + (tone ? ' stat-card--' + tone : '') + '">' +
                    '<div class="label">' + label + '</div>' +
                    '<div class="value">' + (value || 0) + '</div></div>';
            };
            el.innerHTML =
                card('Ready', d.ready) +
                card('Processing', d.processing) +
                card('Completed', d.completed) +
                card('Dead letter', d.dead_letter, d.dead_letter > 0 ? 'bad' : '');
        }).catch(function() {});
    },

    pauseQueue: function() {
        var name = GQM.pages.jobs.currentQueue;
        GQM.utils.confirm('Pause Queue', 'Pause queue "' + name + '"? Workers will stop dequeuing.').then(function(ok) {
            if (!ok) return;
            GQM.api.post('/api/v1/queues/' + encodeURIComponent(name) + '/pause').then(function() {
                GQM.utils.toast('Queue paused', 'success');
                GQM.pages.jobs.loadQueueStats(name);
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
    },

    resumeQueue: function() {
        var name = GQM.pages.jobs.currentQueue;
        GQM.api.post('/api/v1/queues/' + encodeURIComponent(name) + '/resume').then(function() {
            GQM.utils.toast('Queue resumed', 'success');
            GQM.pages.jobs.loadQueueStats(name);
        }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
    },

    emptyQueue: function() {
        var name = GQM.pages.jobs.currentQueue;
        GQM.utils.confirm('Empty Queue', 'Delete ALL ready jobs from "' + name + '"? This cannot be undone.').then(function(ok) {
            if (!ok) return;
            GQM.api.del('/api/v1/queues/' + encodeURIComponent(name) + '/empty').then(function(resp) {
                var removed = (resp.data && resp.data.removed) || 0;
                GQM.utils.toast('Removed ' + removed + ' jobs', 'success');
                GQM.pages.jobs.loadQueueStats(name);
                GQM.pages.jobs.loadJobs();
            }).catch(function(err) { GQM.utils.toast(err.message, 'error'); });
        });
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
                el.innerHTML = '<div class="empty-state"><p>No ' + status.replace(/_/g, ' ') + ' jobs</p></div>';
                document.getElementById('jobs-pagination').innerHTML = '';
                return;
            }

            GQM.pages.jobs.renderJobsTable(el, jobs);

            var pagEl = document.getElementById('jobs-pagination');
            if (pagEl) {
                pagEl.innerHTML = GQM.utils.paginationHTML(meta.page || 1, meta.limit || 20, meta.total || 0);
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
                    ? '<div class="pagination-note">Showing the 100 most recent jobs — select a specific status for full pagination.</div>'
                    : '';
            }

            GQM.pages.jobs.applySearchFilter();
        });
    },

    // Render a table of jobs into the target element.
    renderJobsTable: function(el, jobs) {
        var rows = jobs.map(function(j) {
            var err = GQM.utils.escapeHTML(j.error || '');
            return '<tr data-job-id="' + GQM.utils.escapeHTML(j.id).toLowerCase() + '">' +
                '<td class="mono truncate"><a href="#/jobs/' + GQM.utils.escapeHTML(j.id) + '">' + GQM.utils.escapeHTML(j.id) + '</a></td>' +
                '<td>' + GQM.utils.escapeHTML(j.type || '') + '</td>' +
                '<td>' + GQM.utils.statusBadge(j.status) + '</td>' +
                '<td class="dim">' + GQM.utils.formatTime(j.created_at) + '</td>' +
                (err ? '<td class="err-cell" title="' + err + '">' + err + '</td>' : '<td class="dim">—</td>') +
                '</tr>';
        }).join('');

        el.innerHTML =
            '<table><thead><tr>' +
            '<th>Job</th><th>Type</th><th>Status</th><th>Created</th><th>Error</th>' +
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
        GQM.pages.jobs.statusSearchFilter = '';

        container.innerHTML =
            GQM.utils.pageHead({
                crumbs: '<a href="#/">Overview</a> / ',
                title: 'Jobs ' + GQM.utils.statusBadge(status.replace(/_/g, ' ')),
                sub: 'Across all queues, most recent first',
                poll: '10s'
            }) +
            '<div class="filter-bar">' +
            '<div class="filter-group">' +
            '<input type="text" id="status-job-filter" placeholder="Filter by job ID..." autocomplete="off" aria-label="Filter by job ID">' +
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

                var truncated = allJobs.length > 100;
                if (truncated) allJobs = allJobs.slice(0, 100);

                // Render with queue column
                var rows = allJobs.map(function(j) {
                    var queue = j._queue || j.queue || '';
                    var err = GQM.utils.escapeHTML(j.error || '');
                    return '<tr data-job-id="' + GQM.utils.escapeHTML(j.id).toLowerCase() + '">' +
                        '<td class="mono truncate"><a href="#/jobs/' + GQM.utils.escapeHTML(j.id) + '">' + GQM.utils.escapeHTML(j.id) + '</a></td>' +
                        '<td><a href="#/queues/' + GQM.utils.escapeHTML(queue) + '">' + GQM.utils.escapeHTML(queue) + '</a></td>' +
                        '<td>' + GQM.utils.escapeHTML(j.type || '') + '</td>' +
                        '<td>' + GQM.utils.statusBadge(j.status) + '</td>' +
                        '<td class="dim">' + GQM.utils.formatTime(j.created_at) + '</td>' +
                        (err ? '<td class="err-cell" title="' + err + '">' + err + '</td>' : '<td class="dim">—</td>') +
                        '</tr>';
                }).join('');

                el.innerHTML =
                    '<table><thead><tr>' +
                    '<th>Job</th><th>Queue</th><th>Type</th><th>Status</th><th>Created</th><th>Error</th>' +
                    '</tr></thead><tbody>' + rows + '</tbody></table>' +
                    (truncated ? '<div class="pagination-note">Showing the 100 most recent — open a queue for full pagination.</div>' : '');

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
            '<div id="job-page">' +
            '<div id="job-head">' +
            GQM.utils.pageHead({ title: 'Job Detail', titleClass: 'mono-title' }) +
            '</div>' +
            '<div id="job-detail"><div class="loading">Loading job</div></div>' +
            '</div>';

        // Event delegation for job detail action buttons (header + body).
        // Attached to #job-page — recreated on every render — NOT to the
        // persistent container, where listeners would stack per visit and
        // fire the action once per accumulated listener.
        document.getElementById('job-page').addEventListener('click', function(e) {
            var btn = e.target.closest('[data-action]');
            if (!btn) return;
            var action = btn.getAttribute('data-action');
            var id = btn.getAttribute('data-id');
            if (action === 'back') window.history.back();
            else if (action === 'retry') GQM.pages.jobs.retry(id);
            else if (action === 'cancel') GQM.pages.jobs.cancel(id);
            else if (action === 'delete') GQM.pages.jobs.deleteJob(id);
        });

        GQM.pages.jobs.loadJobDetail(jobId);
    },

    // Build the timeline strip for a job.
    jobTimeline: function(j) {
        var t = GQM.utils.formatTime;
        var nodes = [];
        nodes.push({ label: 'Created', time: t(j.created_at), cls: '' });
        if (j.scheduled_at) nodes.push({ label: 'Scheduled', time: t(j.scheduled_at), cls: '' });
        if (j.started_at) nodes.push({ label: 'Started', time: t(j.started_at), cls: '' });

        var terminal = {
            completed: 'Completed',
            failed: 'Failed',
            dead_letter: 'Dead letter',
            canceled: 'Canceled'
        }[j.status];
        if (terminal) {
            var extra = (j.status === 'dead_letter' || j.status === 'failed')
                ? ' · ' + (j.retry_count || 0) + ' of ' + (j.max_retry || 0) + ' retries'
                : '';
            nodes.push({
                label: terminal,
                time: t(j.completed_at) + GQM.utils.escapeHTML(extra),
                cls: 'tl-dot--' + j.status
            });
        } else if (j.status === 'processing') {
            nodes.push({ label: 'Processing', time: 'running', cls: 'tl-dot--processing' });
        }

        var html = '<div class="timeline">';
        nodes.forEach(function(n, i) {
            if (i > 0) html += '<div class="tl-line"></div>';
            html += '<div class="tl-node"><span class="tl-dot ' + n.cls + '"></span>' +
                '<span class="tl-label"><b>' + n.label + '</b>' + n.time + '</span></div>';
        });
        html += '</div>';
        return html;
    },

    loadJobDetail: function(jobId) {
        GQM.api.get('/api/v1/jobs/' + encodeURIComponent(jobId)).then(function(resp) {
            var j = resp.data || {};
            var el = document.getElementById('job-detail');
            if (!el) return;
            var esc = GQM.utils.escapeHTML;

            // Header: breadcrumb, monospace job id + status pill, actions.
            var head = document.getElementById('job-head');
            if (head) {
                var actions =
                    '<button class="btn btn--sm btn--ghost" data-action="back">' +
                    '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M10.5 3 5.5 8l5 5"/></svg>' +
                    'Back</button>';
                if (j.status === 'dead_letter' || j.status === 'failed' || j.status === 'canceled') {
                    actions += '<button class="btn btn--sm btn--primary" data-action="retry" data-id="' + esc(j.id) + '">Retry</button>';
                }
                if (j.status === 'ready' || j.status === 'processing' || j.status === 'deferred') {
                    actions += '<button class="btn btn--sm btn--danger" data-action="cancel" data-id="' + esc(j.id) + '">Cancel</button>';
                }
                actions += '<button class="btn btn--sm btn--danger" data-action="delete" data-id="' + esc(j.id) + '">Delete</button>';

                head.innerHTML = GQM.utils.pageHead({
                    crumbs: '<a href="#/queues">Queues</a> / ' +
                        (j.queue ? '<a href="#/queues/' + esc(j.queue) + '">' + esc(j.queue) + '</a> / ' : ''),
                    title: esc(j.id) + ' ' + GQM.utils.statusBadge(j.status),
                    titleClass: 'mono-title',
                    right: actions
                });
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

            var details = '<div class="detail-panel">' +
                '<h3>Details</h3>' +
                '<dl class="detail-grid">' +
                '<dt>Type</dt><dd>' + esc(j.type) + '</dd>' +
                '<dt>Queue</dt><dd><a href="#/queues/' + esc(j.queue) + '">' + esc(j.queue) + '</a></dd>' +
                '<dt>Retry</dt><dd class="mono">' + (j.retry_count || 0) + ' / ' + (j.max_retry || 0) + '</dd>' +
                (j.timeout ? '<dt>Timeout</dt><dd class="mono">' + GQM.utils.formatDuration(Number(j.timeout)) + '</dd>' : '') +
                (j.execution_duration ? '<dt>Duration</dt><dd class="mono">' + GQM.utils.formatDuration(Number(j.execution_duration)) + '</dd>' : '') +
                (j.worker_id ? '<dt>Worker</dt><dd class="mono">' + esc(j.worker_id) + '</dd>' : '') +
                (j.enqueued_by ? '<dt>Enqueued by</dt><dd>' + esc(j.enqueued_by) + '</dd>' : '') +
                (j.error ? '<dt>Error</dt><dd style="color:var(--danger)">' + esc(j.error) + '</dd>' : '') +
                '</dl>' +
                '</div>';

            var panels = '';
            if (payloadStr) {
                panels += '<div class="detail-panel"><h3>Payload</h3><pre>' + esc(payloadStr) + '</pre></div>';
            }
            if (resultStr) {
                panels += '<div class="detail-panel"><h3>Result</h3><pre>' + esc(resultStr) + '</pre></div>';
            }
            if (metaStr) {
                panels += '<div class="detail-panel"><h3>Meta</h3><pre>' + esc(metaStr) + '</pre></div>';
            }

            var html = GQM.pages.jobs.jobTimeline(j);
            if (panels) {
                html += '<div class="detail-cols">' + details + '<div class="detail-stack">' + panels + '</div></div>';
            } else {
                html += details;
            }

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
