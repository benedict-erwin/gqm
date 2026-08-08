// GQM Dashboard — utils.js
// Shared utility functions.

var GQM = window.GQM || {};

GQM.utils = {
    // Format a Unix timestamp (seconds) to locale string.
    formatTime: function(ts) {
        if (!ts) return '—';
        var d = new Date(ts * 1000);
        return d.toLocaleString();
    },

    // Format a Unix timestamp to relative time (e.g., "3m ago").
    formatRelative: function(ts) {
        if (!ts) return '—';
        var diff = Math.floor(Date.now() / 1000) - ts;
        if (diff < 0) return 'just now';
        if (diff < 60) return diff + 's ago';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        return Math.floor(diff / 86400) + 'd ago';
    },

    // Format duration in seconds to human readable.
    formatDuration: function(secs) {
        if (secs == null || secs === 0) return '—';
        if (secs < 1) return Math.round(secs * 1000) + 'ms';
        if (secs < 60) return Math.round(secs * 10) / 10 + 's';
        if (secs < 3600) return Math.floor(secs / 60) + 'm ' + Math.round(secs % 60) + 's';
        return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm';
    },

    // Format a number with commas.
    formatNumber: function(n) {
        if (n == null) return '0';
        return Number(n).toLocaleString();
    },

    // Return a status badge HTML string.
    statusBadge: function(status) {
        if (!status) return '';
        var cls = status.replace(/\s+/g, '_').toLowerCase();
        return '<span class="badge badge--' + cls + '">' + GQM.utils.escapeHTML(status) + '</span>';
    },

    // Return a status badge for a job. A processing job the API flagged as
    // stale is one whose claim outlived any deadline a live worker could hold,
    // so it renders as stale rather than as healthy work in flight.
    jobStatusBadge: function(job) {
        if (job && job.stale) {
            return '<span class="badge badge--stale" title="stale — worker presumed dead">stale</span>';
        }
        return GQM.utils.statusBadge(job ? job.status : '');
    },

    // Escape HTML special characters (safe for both element content and attributes).
    escapeHTML: function(str) {
        if (str == null) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    },

    // Simple HTML template — replaces {{key}} with escaped values.
    template: function(tpl, data) {
        return tpl.replace(/\{\{(\w+)\}\}/g, function(_, key) {
            return GQM.utils.escapeHTML(data[key] != null ? data[key] : '');
        });
    },

    // Show a toast notification.
    toast: function(message, type) {
        type = type || 'info';
        var container = document.getElementById('toast-container');
        var el = document.createElement('div');
        el.className = 'toast toast--' + type;
        el.textContent = message;
        container.appendChild(el);
        setTimeout(function() {
            el.style.opacity = '0';
            el.style.transition = 'opacity 0.3s';
            setTimeout(function() { el.remove(); }, 300);
        }, 3000);
    },

    // Show a confirm modal. Returns a Promise that resolves to true/false.
    confirm: function(title, message) {
        return new Promise(function(resolve) {
            var overlay = document.createElement('div');
            overlay.className = 'modal-overlay';
            overlay.innerHTML =
                '<div class="modal-box">' +
                '<h3>' + GQM.utils.escapeHTML(title) + '</h3>' +
                '<p>' + GQM.utils.escapeHTML(message) + '</p>' +
                '<div class="modal-actions">' +
                '<button class="btn" data-action="cancel">Cancel</button>' +
                '<button class="btn btn--danger" data-action="confirm">Confirm</button>' +
                '</div></div>';

            overlay.addEventListener('click', function(e) {
                var action = e.target.getAttribute('data-action');
                if (action === 'confirm') { overlay.remove(); resolve(true); }
                else if (action === 'cancel' || e.target === overlay) { overlay.remove(); resolve(false); }
            });

            document.body.appendChild(overlay);
        });
    },

    // Build pagination HTML.
    paginationHTML: function(page, limit, total) {
        var totalPages = Math.ceil(total / limit) || 1;
        if (totalPages <= 1) return '';
        var html = '<div class="pagination">';
        html += '<button class="btn btn--sm" data-page="' + (page - 1) + '"' + (page <= 1 ? ' disabled' : '') + '>&laquo;</button>';
        html += '<span class="page-info">Page ' + page + ' of ' + totalPages + '</span>';
        html += '<button class="btn btn--sm" data-page="' + (page + 1) + '"' + (page >= totalPages ? ' disabled' : '') + '>&raquo;</button>';
        html += '</div>';
        return html;
    },

    // Build a standard page header.
    // opts: { title (HTML), sub, crumbs (HTML), right (HTML), poll (e.g. "10s"), titleClass }
    pageHead: function(opts) {
        var right = '';
        if (opts.poll) {
            right += '<span class="live"><span class="dot"></span>Live &middot; ' + opts.poll + '</span>';
        }
        if (opts.right) right += opts.right;
        return '<div class="page-header"><div>' +
            (opts.crumbs ? '<div class="crumbs">' + opts.crumbs + '</div>' : '') +
            '<h2' + (opts.titleClass ? ' class="' + opts.titleClass + '"' : '') + '>' + opts.title + '</h2>' +
            (opts.sub ? '<p class="sub">' + opts.sub + '</p>' : '') +
            '</div>' +
            (right ? '<div class="head-right">' + right + '</div>' : '') +
            '</div>';
    },

    // Resolve a CSS custom property to its computed value (theme-aware).
    themeVar: function(name, fallback) {
        var v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
        return v || fallback || '';
    },

    // Human-readable description for common cron expressions.
    // Returns '' when the expression doesn't match a simple pattern.
    cronHuman: function(expr) {
        if (!expr) return '';
        var p = expr.trim().split(/\s+/);
        if (p.length !== 5) return '';
        var min = p[0], hour = p[1], dom = p[2], mon = p[3], dow = p[4];
        var pad = function(n) { return (n.length < 2 ? '0' : '') + n; };
        var dows = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        if (min === '*' && hour === '*' && dom === '*' && mon === '*' && dow === '*') return 'Every minute';
        if (/^\d+$/.test(min) && hour === '*' && dom === '*' && mon === '*' && dow === '*') {
            return min === '0' ? 'Every hour' : 'Hourly at :' + pad(min);
        }
        if (/^\d+$/.test(min) && /^\d+$/.test(hour) && dom === '*' && mon === '*') {
            var t = pad(hour) + ':' + pad(min);
            if (dow === '*') return 'Daily at ' + t;
            if (/^\d+$/.test(dow) && dows[+dow]) return dows[+dow] + 's at ' + t;
        }
        if (min.indexOf('*/') === 0 && hour === '*' && dom === '*' && mon === '*' && dow === '*') {
            return 'Every ' + min.slice(2) + ' min';
        }
        return '';
    },

    // Parse hash route: "#/queues/email?pool=x" -> { page: "queues", param: "email", query: {pool:"x"} }
    parseRoute: function() {
        var hash = window.location.hash.replace(/^#\/?/, '');
        var queryIdx = hash.indexOf('?');
        var query = {};
        if (queryIdx !== -1) {
            var qs = hash.substring(queryIdx + 1);
            hash = hash.substring(0, queryIdx);
            qs.split('&').forEach(function(pair) {
                var kv = pair.split('=');
                if (kv[0]) query[decodeURIComponent(kv[0])] = decodeURIComponent(kv[1] || '');
            });
        }
        var parts = hash.split('/');
        return {
            page: parts[0] || '',
            param: parts.slice(1).join('/') || '',
            query: query
        };
    }
};
