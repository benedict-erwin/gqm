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

    // Escape HTML special characters.
    escapeHTML: function(str) {
        if (str == null) return '';
        var div = document.createElement('div');
        div.textContent = String(str);
        return div.innerHTML;
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

    // Parse hash route: "#/queues/email" -> { page: "queues", param: "email" }
    parseRoute: function() {
        var hash = window.location.hash.replace(/^#\/?/, '');
        var parts = hash.split('/');
        return {
            page: parts[0] || '',
            param: parts.slice(1).join('/') || ''
        };
    }
};
