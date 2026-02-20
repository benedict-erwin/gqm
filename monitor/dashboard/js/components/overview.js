// GQM Dashboard — overview.js
// Overview page: stat cards + throughput chart.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.overview = {
    chart: null,

    render: function(container) {
        container.innerHTML =
            '<div class="page-header"><h2>Overview</h2></div>' +
            '<div id="stats-grid" class="stat-grid"><div class="loading">Loading stats</div></div>' +
            '<div id="runtime-grid" class="stat-grid"></div>' +
            '<div id="throughput-chart" class="chart-container">' +
            '<h3>Throughput (last 7 days)</h3>' +
            '<canvas id="chart-canvas"></canvas>' +
            '</div>';

        GQM.app.poll(function() {
            GQM.pages.overview.loadStats();
            GQM.pages.overview.loadRuntime();
        }, 5000);
        GQM.pages.overview.loadDaily();
    },

    loadStats: function() {
        GQM.api.get('/api/v1/stats').then(function(resp) {
            var d = resp.data || {};
            var grid = document.getElementById('stats-grid');
            if (!grid) return;
            grid.innerHTML =
                GQM.pages.overview.statCard('Queues', d.queues, '#/queues') +
                GQM.pages.overview.statCard('Workers', d.workers, '#/workers') +
                GQM.pages.overview.statCard('Ready', d.ready, '#/status/ready') +
                GQM.pages.overview.statCard('Processing', d.processing, '#/status/processing') +
                GQM.pages.overview.statCard('Scheduled', d.scheduled) +
                GQM.pages.overview.statCard('Completed', d.completed, '#/status/completed') +
                GQM.pages.overview.statCard('Dead Letter', d.dead_letter, '#/status/dead_letter') +
                GQM.pages.overview.statCard('Processed Total', GQM.utils.formatNumber(d.processed_total)) +
                GQM.pages.overview.statCard('Failed Total', GQM.utils.formatNumber(d.failed_total));
        }).catch(function() {});
    },

    loadRuntime: function() {
        GQM.api.get('/api/v1/stats/runtime').then(function(resp) {
            var d = resp.data || {};
            var grid = document.getElementById('runtime-grid');
            if (!grid) return;
            grid.innerHTML =
                GQM.pages.overview.statCard('Uptime', d.uptime || '—') +
                GQM.pages.overview.statCard('Go Version', d.go_version || '—') +
                GQM.pages.overview.statCard('Goroutines', d.goroutines) +
                GQM.pages.overview.statCard('Memory', (d.alloc_mb || 0) + ' MB') +
                GQM.pages.overview.statCard('GC Cycles', d.num_gc);
        }).catch(function() {});
    },

    loadDaily: function() {
        GQM.api.get('/api/v1/stats/daily?days=7').then(function(resp) {
            var queues = resp.data || [];
            // Aggregate across all queues
            var dayMap = {};
            queues.forEach(function(q) {
                (q.days || []).forEach(function(d) {
                    if (!dayMap[d.date]) dayMap[d.date] = { date: d.date, processed: 0, failed: 0 };
                    dayMap[d.date].processed += d.processed || 0;
                    dayMap[d.date].failed += d.failed || 0;
                });
            });
            var days = Object.values(dayMap).sort(function(a, b) {
                return a.date > b.date ? 1 : -1;
            });
            GQM.pages.overview.renderChart(days);
        }).catch(function() {});
    },

    renderChart: function(days) {
        var canvas = document.getElementById('chart-canvas');
        if (!canvas) return;

        // If Chart.js is not loaded, show text fallback
        if (typeof Chart === 'undefined') {
            var parent = canvas.parentNode;
            canvas.style.display = 'none';
            var existing = parent.querySelector('.chart-fallback');
            if (existing) existing.remove();
            var fallback = document.createElement('div');
            fallback.className = 'chart-fallback';
            fallback.innerHTML = GQM.pages.overview.textChart(days);
            parent.appendChild(fallback);
            return;
        }

        var labels = days.map(function(d) { return d.date; });
        var processed = days.map(function(d) { return d.processed || 0; });
        var failed = days.map(function(d) { return d.failed || 0; });

        if (GQM.pages.overview.chart) {
            GQM.pages.overview.chart.destroy();
        }

        GQM.pages.overview.chart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Processed',
                        data: processed,
                        borderColor: '#22c55e',
                        backgroundColor: 'rgba(34,197,94,0.1)',
                        fill: true,
                        tension: 0.3
                    },
                    {
                        label: 'Failed',
                        data: failed,
                        borderColor: '#ef4444',
                        backgroundColor: 'rgba(239,68,68,0.1)',
                        fill: true,
                        tension: 0.3
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'bottom' } },
                scales: {
                    y: { beginAtZero: true, ticks: { precision: 0 } }
                }
            }
        });
    },

    textChart: function(days) {
        if (!days || !days.length) return '<p class="text-secondary">No data available</p>';
        var rows = days.map(function(d) {
            return '<tr><td>' + GQM.utils.escapeHTML(d.date) + '</td>' +
                '<td>' + (d.processed || 0) + '</td>' +
                '<td>' + (d.failed || 0) + '</td></tr>';
        }).join('');
        return '<div class="table-wrap"><table><thead><tr><th>Date</th><th>Processed</th><th>Failed</th></tr></thead><tbody>' + rows + '</tbody></table></div>';
    },

    statCard: function(label, value, href) {
        if (value == null) value = 0;
        var inner = '<div class="label">' + GQM.utils.escapeHTML(label) +
            '</div><div class="value">' + GQM.utils.escapeHTML(String(value)) + '</div>';
        if (href) {
            return '<a href="' + href + '" class="stat-card stat-card--clickable">' + inner + '</a>';
        }
        return '<div class="stat-card">' + inner + '</div>';
    }
};
