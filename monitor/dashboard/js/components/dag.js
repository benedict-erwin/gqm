// GQM Dashboard — dag.js
// DAG dependency explorer with Cytoscape.js graph visualization.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.dag = {
    cy: null,

    // Status color mapping (bg + border).
    statusColor: function(status) {
        var colors = {
            ready:       { bg: '#dbeafe', border: '#1d4ed8' },
            processing:  { bg: '#fef3c7', border: '#92400e' },
            completed:   { bg: '#d1fae5', border: '#065f46' },
            failed:      { bg: '#fee2e2', border: '#991b1b' },
            dead_letter: { bg: '#fce7f3', border: '#9d174d' },
            deferred:    { bg: '#e0e7ff', border: '#3730a3' },
            canceled:    { bg: '#f1f5f9', border: '#475569' },
            unknown:     { bg: '#f1f5f9', border: '#94a3b8' }
        };
        return colors[status] || colors.unknown;
    },

    // Render the full DAG page.
    render: function(container, jobId) {
        var self = this;

        // Destroy previous Cytoscape instance.
        if (self.cy) {
            self.cy.destroy();
            self.cy = null;
        }

        var esc = GQM.utils.escapeHTML;

        // Build page layout.
        var html =
            '<div class="page-header"><h2>DAG Dependencies</h2></div>' +

            // Search toolbar
            '<div class="card mb-2">' +
            '<div class="dag-toolbar">' +
            '<input type="text" id="dag-search" class="input" placeholder="Enter job ID..." value="' + esc(jobId || '') + '">' +
            '<button id="dag-load-btn" class="btn btn--primary">Load Graph</button>' +
            '</div>' +
            '</div>' +

            // DAG Chains table (from roots endpoint — always has data)
            '<div class="card mb-2">' +
            '<h3 class="mb-1">DAG Chains</h3>' +
            '<p class="text-secondary text-sm mb-1">Parent jobs with dependency graphs. Click View Graph to visualize.</p>' +
            '<div id="dag-roots"></div>' +
            '</div>' +

            // Deferred jobs table (collapsible secondary)
            '<div class="card mb-2">' +
            '<div style="display:flex;align-items:center;gap:0.5rem;cursor:pointer" id="dag-deferred-toggle">' +
            '<h3>Deferred Jobs</h3>' +
            '<span class="text-secondary text-sm" id="dag-deferred-count"></span>' +
            '<span class="text-secondary" style="margin-left:auto">&#9660;</span>' +
            '</div>' +
            '<div id="dag-deferred" style="display:none;margin-top:0.5rem"></div>' +
            '</div>' +

            // Graph section
            '<div id="dag-graph-section" style="display:none">' +
            '<div class="card mb-2">' +
            '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">' +
            '<h3>Graph Visualization</h3>' +
            '<button id="dag-fit-btn" class="btn btn--sm">Fit View</button>' +
            '</div>' +
            '<div id="dag-legend" class="dag-legend"></div>' +
            '<div id="dag-graph" class="dag-graph-container"></div>' +
            '</div>' +

            // Node detail
            '<div id="dag-node-detail" class="dag-node-detail mb-2" style="display:none"></div>' +
            '</div>';

        container.innerHTML = html;

        // Event: Load graph button.
        document.getElementById('dag-load-btn').addEventListener('click', function() {
            var id = document.getElementById('dag-search').value.trim();
            if (id) {
                window.location.hash = '#/dag/' + id;
            }
        });

        // Event: Enter key in search.
        document.getElementById('dag-search').addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                var id = this.value.trim();
                if (id) {
                    window.location.hash = '#/dag/' + id;
                }
            }
        });

        // Event: Fit view button.
        document.getElementById('dag-fit-btn').addEventListener('click', function() {
            if (self.cy) self.cy.fit(undefined, 30);
        });

        // Event: Toggle deferred section.
        document.getElementById('dag-deferred-toggle').addEventListener('click', function() {
            var el = document.getElementById('dag-deferred');
            var arrow = this.querySelector('span:last-child');
            if (el.style.display === 'none') {
                el.style.display = '';
                arrow.innerHTML = '&#9650;';
            } else {
                el.style.display = 'none';
                arrow.innerHTML = '&#9660;';
            }
        });

        // Load data.
        self.loadRoots(1);
        self.loadDeferred(1);

        // Auto-load graph if jobId provided.
        if (jobId) {
            self.loadGraph(jobId);
        }

        // Poll every 15s.
        GQM.app.poll(function() {
            self.loadRoots(1);
            self.loadDeferred(1);
        }, 15000);
    },

    // Load and render DAG roots (parent jobs with dependents).
    loadRoots: function(page) {
        var self = this;
        GQM.api.get('/api/v1/dag/roots?page=' + page + '&limit=20').then(function(resp) {
            var el = document.getElementById('dag-roots');
            if (!el) return;

            var jobs = resp.data || [];
            var meta = resp.meta || {};

            if (jobs.length === 0) {
                el.innerHTML = '<p class="text-secondary text-sm">No DAG chains found.</p>';
                return;
            }

            var esc = GQM.utils.escapeHTML;
            var html = '<div class="table-wrapper"><table class="table">' +
                '<thead><tr>' +
                '<th>Job ID</th><th>Type</th><th>Status</th><th>Queue</th><th>Children</th><th>Created</th><th>Actions</th>' +
                '</tr></thead><tbody>';

            jobs.forEach(function(job) {
                var created = GQM.utils.formatRelative(parseInt(job.created_at, 10) || 0);

                html += '<tr>' +
                    '<td><a href="#/jobs/' + esc(job.id) + '"><code>' + esc(job.id) + '</code></a></td>' +
                    '<td>' + esc(job.type) + '</td>' +
                    '<td>' + GQM.utils.statusBadge(job.status) + '</td>' +
                    '<td>' + esc(job.queue) + '</td>' +
                    '<td>' + esc(job.child_count || 0) + '</td>' +
                    '<td>' + esc(created) + '</td>' +
                    '<td><button class="btn btn--sm btn--primary dag-view-btn" data-job-id="' + esc(job.id) + '">View Graph</button></td>' +
                    '</tr>';
            });

            html += '</tbody></table></div>';
            html += GQM.utils.paginationHTML(meta.page || 1, meta.limit || 20, meta.total || 0);
            el.innerHTML = html;

            // View Graph buttons.
            el.querySelectorAll('.dag-view-btn').forEach(function(btn) {
                btn.addEventListener('click', function() {
                    var id = this.getAttribute('data-job-id');
                    document.getElementById('dag-search').value = id;
                    self.loadGraph(id);
                });
            });

            // Pagination clicks.
            el.querySelectorAll('[data-page]').forEach(function(btn) {
                btn.addEventListener('click', function() {
                    var p = parseInt(this.getAttribute('data-page'), 10);
                    if (p >= 1) self.loadRoots(p);
                });
            });
        }).catch(function() {
            var el = document.getElementById('dag-roots');
            if (el) el.innerHTML = '<p class="text-secondary text-sm">Failed to load DAG chains.</p>';
        });
    },

    // Load and render the deferred jobs table.
    loadDeferred: function(page) {
        var self = this;
        GQM.api.get('/api/v1/dag/deferred?page=' + page + '&limit=20').then(function(resp) {
            var el = document.getElementById('dag-deferred');
            if (!el) return;

            var jobs = resp.data || [];
            var meta = resp.meta || {};

            // Update count badge.
            var countEl = document.getElementById('dag-deferred-count');
            if (countEl) countEl.textContent = '(' + (meta.total || 0) + ')';

            if (jobs.length === 0) {
                el.innerHTML = '<p class="text-secondary text-sm">No deferred jobs currently waiting.</p>';
                return;
            }

            var esc = GQM.utils.escapeHTML;
            var html = '<div class="table-wrapper"><table class="table">' +
                '<thead><tr>' +
                '<th>ID</th><th>Type</th><th>Queue</th><th>Pending Deps</th><th>Actions</th>' +
                '</tr></thead><tbody>';

            jobs.forEach(function(job) {
                var pending = job.pending_deps || [];
                var pendingHtml = pending.length > 0
                    ? pending.map(function(p) { return '<a href="#/jobs/' + esc(p) + '">' + esc(p.substring(0, 8)) + '</a>'; }).join(', ')
                    : '<span class="text-secondary">none</span>';

                html += '<tr>' +
                    '<td><code>' + esc((job.id || '').substring(0, 12)) + '</code></td>' +
                    '<td>' + esc(job.type) + '</td>' +
                    '<td>' + esc(job.queue) + '</td>' +
                    '<td>' + pendingHtml + '</td>' +
                    '<td><button class="btn btn--sm dag-view-btn" data-job-id="' + esc(job.id) + '">View Graph</button></td>' +
                    '</tr>';
            });

            html += '</tbody></table></div>';
            html += GQM.utils.paginationHTML(meta.page || 1, meta.limit || 20, meta.total || 0);
            el.innerHTML = html;

            // View Graph buttons.
            el.querySelectorAll('.dag-view-btn').forEach(function(btn) {
                btn.addEventListener('click', function() {
                    var id = this.getAttribute('data-job-id');
                    document.getElementById('dag-search').value = id;
                    self.loadGraph(id);
                });
            });

            // Pagination clicks.
            el.querySelectorAll('[data-page]').forEach(function(btn) {
                btn.addEventListener('click', function() {
                    var p = parseInt(this.getAttribute('data-page'), 10);
                    if (p >= 1) self.loadDeferred(p);
                });
            });
        }).catch(function() {
            var el = document.getElementById('dag-deferred');
            if (el) el.innerHTML = '<p class="text-secondary text-sm">Failed to load deferred jobs.</p>';
        });
    },

    // Load graph data and render Cytoscape visualization.
    loadGraph: function(jobId) {
        var self = this;

        GQM.api.get('/api/v1/dag/graph/' + encodeURIComponent(jobId) + '?depth=10&max_nodes=50').then(function(resp) {
            var data = resp.data || {};
            var nodes = data.nodes || [];
            var edges = data.edges || [];

            var section = document.getElementById('dag-graph-section');
            if (!section) return;
            section.style.display = '';

            // Hide node detail.
            var detailEl = document.getElementById('dag-node-detail');
            if (detailEl) detailEl.style.display = 'none';

            if (nodes.length === 0) {
                document.getElementById('dag-graph').innerHTML =
                    '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--color-text-secondary)">' +
                    'Job not found or no graph data available.</div>';
                document.getElementById('dag-legend').innerHTML = '';
                return;
            }

            // Render legend.
            self.renderLegend(nodes);

            // Build Cytoscape elements.
            var cyNodes = nodes.map(function(n) {
                var c = self.statusColor(n.status);
                var label = (n.type || 'unknown');
                if (label.length > 16) label = label.substring(0, 14) + '..';
                label += '\n' + (n.id || '').substring(0, 8);

                return {
                    data: {
                        id: n.id,
                        label: label,
                        status: n.status || 'unknown',
                        bgColor: c.bg,
                        borderColor: c.border,
                        borderWidth: n.id === data.root_id ? 4 : 2,
                        nodeData: n
                    }
                };
            });

            var cyEdges = edges.map(function(e) {
                return {
                    data: {
                        source: e.source,
                        target: e.target
                    }
                };
            });

            // Destroy previous instance.
            if (self.cy) {
                self.cy.destroy();
                self.cy = null;
            }

            var graphEl = document.getElementById('dag-graph');
            graphEl.innerHTML = '';

            // Scroll graph into view.
            section.scrollIntoView({ behavior: 'smooth', block: 'start' });

            self.cy = cytoscape({
                container: graphEl,
                elements: { nodes: cyNodes, edges: cyEdges },
                layout: {
                    name: 'dagre',
                    rankDir: 'TB',
                    nodeSep: 50,
                    rankSep: 80,
                    padding: 30
                },
                style: [
                    {
                        selector: 'node',
                        style: {
                            'label': 'data(label)',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'text-wrap': 'wrap',
                            'font-size': '11px',
                            'font-family': 'system-ui, -apple-system, sans-serif',
                            'background-color': 'data(bgColor)',
                            'border-color': 'data(borderColor)',
                            'border-width': 'data(borderWidth)',
                            'shape': 'round-rectangle',
                            'width': 130,
                            'height': 50,
                            'color': '#1e293b'
                        }
                    },
                    {
                        selector: 'edge',
                        style: {
                            'width': 2,
                            'line-color': '#94a3b8',
                            'target-arrow-color': '#64748b',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'arrow-scale': 1.2
                        }
                    },
                    {
                        selector: 'node:active',
                        style: {
                            'overlay-opacity': 0.1
                        }
                    }
                ],
                userZoomingEnabled: true,
                userPanningEnabled: true,
                boxSelectionEnabled: false,
                minZoom: 0.3,
                maxZoom: 3
            });

            // Click handler: show node detail.
            self.cy.on('tap', 'node', function(evt) {
                var nodeData = evt.target.data('nodeData');
                if (nodeData) self.showNodeDetail(nodeData);
            });

            // Click on background: hide detail.
            self.cy.on('tap', function(evt) {
                if (evt.target === self.cy) {
                    var d = document.getElementById('dag-node-detail');
                    if (d) d.style.display = 'none';
                }
            });

            // Show truncation warning.
            if (data.truncated) {
                GQM.utils.toast('Graph truncated — too many nodes. Adjust depth/max_nodes.', 'warning');
            }
        }).catch(function(err) {
            var section = document.getElementById('dag-graph-section');
            if (section) section.style.display = '';
            var graphEl = document.getElementById('dag-graph');
            if (graphEl) {
                graphEl.innerHTML =
                    '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--color-danger)">' +
                    'Failed to load graph: ' + GQM.utils.escapeHTML(err.message) + '</div>';
            }
        });
    },

    // Render the color legend based on statuses present in nodes.
    renderLegend: function(nodes) {
        var legendEl = document.getElementById('dag-legend');
        if (!legendEl) return;

        var self = this;
        var seen = {};
        nodes.forEach(function(n) {
            var s = n.status || 'unknown';
            seen[s] = true;
        });

        var html = '';
        var order = ['ready', 'processing', 'completed', 'failed', 'dead_letter', 'deferred', 'canceled', 'unknown'];
        order.forEach(function(s) {
            if (!seen[s]) return;
            var c = self.statusColor(s);
            html += '<span class="dag-legend-item">' +
                '<span class="dag-legend-swatch" style="background:' + c.bg + ';border-color:' + c.border + '"></span>' +
                s +
                '</span>';
        });
        legendEl.innerHTML = html;
    },

    // Show detail panel for a clicked node.
    showNodeDetail: function(nodeData) {
        var el = document.getElementById('dag-node-detail');
        if (!el) return;

        var esc = GQM.utils.escapeHTML;
        var html = '<h4 style="margin-bottom:0.5rem">Node Detail</h4><dl>' +
            '<dt>ID</dt><dd><code>' + esc(nodeData.id) + '</code></dd>' +
            '<dt>Type</dt><dd>' + esc(nodeData.type || '—') + '</dd>' +
            '<dt>Status</dt><dd>' + GQM.utils.statusBadge(nodeData.status || 'unknown') + '</dd>' +
            '<dt>Queue</dt><dd>' + esc(nodeData.queue || '—') + '</dd>' +
            '<dt>Allow Failure</dt><dd>' + (nodeData.allow_failure ? 'Yes' : 'No') + '</dd>' +
            '<dt>Created</dt><dd>' + GQM.utils.formatTime(parseInt(nodeData.created_at, 10) || 0) + '</dd>' +
            '</dl>' +
            '<div style="margin-top:0.75rem">' +
            '<a href="#/jobs/' + esc(nodeData.id) + '" class="btn btn--sm">View Job</a>' +
            '</div>';

        el.innerHTML = html;
        el.style.display = '';
        el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
};
