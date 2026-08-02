// GQM Dashboard — app.js
// Hash-based router, navigation manager, polling lifecycle.

var GQM = window.GQM || {};

GQM.app = {
    currentPage: null,
    pollingTimers: [],
    authenticated: false,
    user: null,

    // Initialize the application.
    init: function() {
        // Detect base path from current location
        // If dashboard is at /dashboard/, API is relative to origin
        GQM.api.basePath = '';

        // Listen for hash changes
        window.addEventListener('hashchange', function() { GQM.app.route(); });

        // Listen for visibility changes — refresh on return
        document.addEventListener('visibilitychange', function() {
            if (document.visibilityState === 'visible' && GQM.app.authenticated) {
                GQM.app.route();
            }
        });

        // Logout button
        var logoutBtn = document.getElementById('logout-btn');
        if (logoutBtn) {
            logoutBtn.addEventListener('click', function() {
                GQM.api.logout().then(function() {
                    GQM.app.authenticated = false;
                    GQM.app.user = null;
                    GQM.app.showLogin();
                }).catch(function() {
                    // Force show login even if logout fails
                    GQM.app.authenticated = false;
                    GQM.app.showLogin();
                });
            });
        }

        // Theme toggle — persists to localStorage; the inline script in
        // index.html re-applies it before first paint on the next load.
        var themeBtn = document.getElementById('theme-toggle');
        if (themeBtn) {
            themeBtn.addEventListener('click', function() {
                var root = document.documentElement;
                var cur = root.getAttribute('data-theme');
                var dark = cur ? cur === 'dark'
                    : (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches);
                var next = dark ? 'light' : 'dark';
                root.setAttribute('data-theme', next);
                try { localStorage.setItem('gqm-theme', next); } catch (e) { /* ignore */ }
                // Re-render the current page so canvas-based charts and the
                // DAG graph pick up the new theme colors.
                if (GQM.app.authenticated) GQM.app.route();
            });
        }

        // Check auth and route
        GQM.api.checkAuth().then(function(data) {
            GQM.app.authenticated = true;
            GQM.app.user = data.data || data;
            // Hide login page if present, show layout
            var loginPage = document.getElementById('login-page');
            if (loginPage) loginPage.style.display = 'none';
            document.getElementById('layout').classList.add('visible');
            GQM.app.updateAuthUI();
            GQM.app.startNavCounts();
            GQM.app.route();
        }).catch(function() {
            GQM.app.showLogin();
        });
    },

    // Sidebar count badges (queues / workers / DLQ), refreshed on an
    // independent slow timer that survives route changes.
    navCountTimer: null,
    startNavCounts: function() {
        if (GQM.app.navCountTimer) return;
        var update = function() {
            if (document.visibilityState !== 'visible' || !GQM.app.authenticated) return;
            GQM.api.get('/api/v1/stats').then(function(resp) {
                var d = resp.data || {};
                var set = function(id, v) {
                    var el = document.getElementById(id);
                    if (el) el.textContent = (v > 0 ? String(v) : '');
                };
                set('nav-count-queues', d.queues || 0);
                set('nav-count-workers', d.workers || 0);
                set('nav-count-dlq', d.dead_letter || 0);
            }).catch(function() {});
        };
        update();
        GQM.app.navCountTimer = setInterval(update, 30000);
    },

    // Show the login page (hides sidebar).
    showLogin: function() {
        GQM.app.stopPolling();
        GQM.app.authenticated = false;
        document.getElementById('layout').classList.remove('visible');
        var app = document.getElementById('app');
        // Create login container outside layout
        var container = document.getElementById('login-page');
        if (!container) {
            container = document.createElement('div');
            container.id = 'login-page';
            document.body.appendChild(container);
        }
        container.style.display = '';
        if (GQM.pages.login) {
            GQM.pages.login.render(container);
        }
        // Show session expiry message if set
        if (GQM.app.loginMessage) {
            var errorEl = document.getElementById('login-error');
            if (errorEl) {
                errorEl.textContent = GQM.app.loginMessage;
                errorEl.style.display = 'block';
            }
            GQM.app.loginMessage = null;
        }
    },

    // Called after successful login.
    onLoginSuccess: function(userData) {
        GQM.app.authenticated = true;
        GQM.app.user = userData;
        // Hide login, show layout
        var loginPage = document.getElementById('login-page');
        if (loginPage) loginPage.style.display = 'none';
        document.getElementById('layout').classList.add('visible');
        GQM.app.updateAuthUI();
        window.location.hash = '#/';
        GQM.app.route();
    },

    // Update sidebar auth display.
    updateAuthUI: function() {
        var userEl = document.getElementById('auth-user');
        var logoutBtn = document.getElementById('logout-btn');
        if (GQM.app.user) {
            var name = GQM.app.user.username || GQM.app.user.api_key_name || '';
            userEl.textContent = name;
            logoutBtn.style.display = '';
        } else {
            userEl.textContent = '';
            logoutBtn.style.display = 'none';
        }
    },

    // Route based on current hash.
    route: function() {
        if (!GQM.app.authenticated) return;

        GQM.app.stopPolling();

        var r = GQM.utils.parseRoute();
        var page = r.page || 'overview';
        var param = r.param;

        // Update active nav link
        var links = document.querySelectorAll('.nav-link');
        links.forEach(function(link) {
            var linkPage = link.getAttribute('data-page');
            link.classList.toggle('active', linkPage === page || (page === '' && linkPage === 'overview'));
        });

        var app = document.getElementById('app');
        GQM.app.currentPage = page;

        // Route to page component
        var pages = GQM.pages || {};
        switch (page) {
            case 'overview':
            case '':
                if (pages.overview) pages.overview.render(app);
                break;
            case 'servers':
                if (pages.servers) pages.servers.render(app);
                break;
            case 'queues':
                if (param && pages.jobs) {
                    pages.jobs.renderQueueDetail(app, param);
                } else if (pages.queues) {
                    pages.queues.render(app);
                }
                break;
            case 'jobs':
                if (param && pages.jobs) {
                    pages.jobs.renderJobDetail(app, param);
                }
                break;
            case 'status':
                if (param && pages.jobs) {
                    pages.jobs.renderJobsByStatus(app, param);
                }
                break;
            case 'workers':
                if (pages.workers) pages.workers.render(app);
                break;
            case 'failed':
                if (pages.failed) pages.failed.render(app);
                break;
            case 'scheduler':
                if (pages.scheduler) pages.scheduler.render(app);
                break;
            case 'dag':
                if (pages.dag) pages.dag.render(app, param);
                break;
            default:
                app.innerHTML = '<div class="empty-state"><p>Page not found</p></div>';
        }
    },

    // Register a polling interval. Automatically paused when tab hidden.
    poll: function(fn, intervalMs) {
        // Run immediately
        fn();
        var timer = setInterval(function() {
            if (document.visibilityState === 'visible' && GQM.app.authenticated) {
                fn();
            }
        }, intervalMs);
        GQM.app.pollingTimers.push(timer);
        return timer;
    },

    // Stop all polling timers.
    stopPolling: function() {
        GQM.app.pollingTimers.forEach(function(t) { clearInterval(t); });
        GQM.app.pollingTimers = [];
    }
};

// Page registry
GQM.pages = GQM.pages || {};

// Boot on DOM ready
document.addEventListener('DOMContentLoaded', function() {
    GQM.app.init();
});
