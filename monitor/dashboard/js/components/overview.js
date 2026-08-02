// GQM Dashboard — overview.js
// Overview page: stat cards + throughput chart.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.overview = {
    chart: null,

    render: function(container) {
        container.innerHTML =
            GQM.utils.pageHead({
                title: 'Overview',
                sub: 'Cluster health at a glance',
                poll: '5s'
            }) +
            '<p class="section-label">Right now</p>' +
            '<div id="stats-now" class="stat-grid"><div class="loading">Loading stats</div></div>' +
            '<p class="section-label">Lifetime</p>' +
            '<div id="stats-lifetime" class="stat-grid"></div>' +
            '<div id="throughput-chart" class="chart-container">' +
            '<div class="chart-head">' +
            '<h3>Throughput &mdash; last 7 days</h3>' +
            '<div class="chart-legend">' +
            '<span><i id="legend-swatch-ok"></i>Processed</span>' +
            '<span><i id="legend-swatch-bad"></i>Failed</span>' +
            '</div>' +
            '</div>' +
            '<canvas id="chart-canvas"></canvas>' +
            '</div>' +
            '<p class="section-label">Runtime</p>' +
            '<div id="runtime-strip" class="runtime-strip"></div>';

        var swOk = document.getElementById('legend-swatch-ok');
        var swBad = document.getElementById('legend-swatch-bad');
        if (swOk) swOk.style.background = GQM.utils.themeVar('--chart-good', '#0e8a41');
        if (swBad) swBad.style.background = GQM.utils.themeVar('--chart-bad', '#c73f3f');

        GQM.app.poll(function() {
            GQM.pages.overview.loadStats();
            GQM.pages.overview.loadRuntime();
        }, 5000);
        GQM.pages.overview.loadDaily();
    },

    loadStats: function() {
        GQM.api.get('/api/v1/stats').then(function(resp) {
            var d = resp.data || {};
            var sc = GQM.pages.overview.statCard;
            var fmt = GQM.utils.formatNumber;

            var now = document.getElementById('stats-now');
            if (now) {
                now.innerHTML =
                    sc({ label: 'Ready', value: d.ready, href: '#/status/ready', sub: 'waiting for workers' }) +
                    sc({ label: 'Processing', value: d.processing, href: '#/status/processing' }) +
                    sc({ label: 'Scheduled', value: d.scheduled }) +
                    sc({ label: 'Queues', value: d.queues, href: '#/queues' }) +
                    sc({ label: 'Workers', value: d.workers, href: '#/workers' });
            }

            var life = document.getElementById('stats-lifetime');
            if (life) {
                life.innerHTML =
                    sc({ label: 'Completed', value: d.completed, href: '#/status/completed' }) +
                    sc({ label: 'Processed total', value: fmt(d.processed_total) }) +
                    sc({ label: 'Failed total', value: fmt(d.failed_total), tone: 'warn' }) +
                    sc({ label: 'Dead letter', value: d.dead_letter, href: '#/status/dead_letter', tone: 'bad' });
            }
        }).catch(function() {});
    },

    loadRuntime: function() {
        GQM.api.get('/api/v1/stats/runtime').then(function(resp) {
            var d = resp.data || {};
            var el = document.getElementById('runtime-strip');
            if (!el) return;
            var esc = GQM.utils.escapeHTML;
            var cell = function(k, v) {
                return '<div class="cell"><div class="k">' + k + '</div><div class="v">' + esc(String(v)) + '</div></div>';
            };
            el.innerHTML =
                cell('Uptime', d.uptime || '—') +
                cell('Go version', d.go_version || '—') +
                cell('Goroutines', d.goroutines != null ? d.goroutines : '—') +
                cell('Memory', (d.alloc_mb || 0) + ' MB') +
                cell('GC cycles', d.num_gc != null ? d.num_gc : '—');
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

        // Resolve theme colors at render time so the chart matches the
        // active theme (re-rendered on theme toggle via route()).
        var good = GQM.utils.themeVar('--chart-good', '#0e8a41');
        var bad = GQM.utils.themeVar('--chart-bad', '#c73f3f');
        var grid = GQM.utils.themeVar('--grid-line', '#e4eae8');
        var tick = GQM.utils.themeVar('--text-3', '#7f8f89');

        GQM.pages.overview.chart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Processed',
                        data: processed,
                        borderColor: good,
                        backgroundColor: GQM.pages.overview.alpha(good, 0.08),
                        fill: true,
                        tension: 0.3,
                        borderWidth: 2,
                        pointRadius: 2.5,
                        pointBackgroundColor: good
                    },
                    {
                        label: 'Failed',
                        data: failed,
                        borderColor: bad,
                        backgroundColor: 'transparent',
                        fill: false,
                        tension: 0.3,
                        borderWidth: 2,
                        pointRadius: 2.5,
                        pointBackgroundColor: bad
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { precision: 0, color: tick, font: { size: 10 } },
                        grid: { color: grid },
                        border: { display: false }
                    },
                    x: {
                        ticks: { color: tick, font: { size: 10 } },
                        grid: { display: false },
                        border: { display: false }
                    }
                }
            }
        });
    },

    // "#rrggbb" -> "rgba(r,g,b,a)". Non-hex input falls back to the raw value.
    alpha: function(hex, a) {
        var m = /^#([0-9a-f]{6})$/i.exec(hex.trim());
        if (!m) return hex;
        var n = parseInt(m[1], 16);
        return 'rgba(' + (n >> 16 & 255) + ',' + (n >> 8 & 255) + ',' + (n & 255) + ',' + a + ')';
    },

    textChart: function(days) {
        if (!days || !days.length) return '<p class="text-secondary">No data available</p>';
        var rows = days.map(function(d) {
            return '<tr><td>' + GQM.utils.escapeHTML(d.date) + '</td>' +
                '<td class="num">' + (d.processed || 0) + '</td>' +
                '<td class="num">' + (d.failed || 0) + '</td></tr>';
        }).join('');
        return '<div class="table-wrap"><table><thead><tr><th>Date</th><th class="num">Processed</th><th class="num">Failed</th></tr></thead><tbody>' + rows + '</tbody></table></div>';
    },

    // opts: { label, value, href, sub, tone: 'warn'|'bad' }
    statCard: function(opts) {
        var value = opts.value == null ? 0 : opts.value;
        var tone = opts.tone ? ' stat-card--' + opts.tone : '';
        var inner = '<div class="label">' + GQM.utils.escapeHTML(opts.label) +
            '</div><div class="value">' + GQM.utils.escapeHTML(String(value)) + '</div>' +
            (opts.sub ? '<div class="subtext">' + GQM.utils.escapeHTML(opts.sub) + '</div>' : '');
        if (opts.href) {
            return '<a href="' + opts.href + '" class="stat-card stat-card--clickable' + tone + '">' + inner + '</a>';
        }
        return '<div class="stat-card' + tone + '">' + inner + '</div>';
    }
};
