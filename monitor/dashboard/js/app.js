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

        // Check auth and route
        GQM.api.checkAuth().then(function(data) {
            GQM.app.authenticated = true;
            GQM.app.user = data.data || data;
            GQM.app.updateAuthUI();
            GQM.app.route();
        }).catch(function() {
            GQM.app.showLogin();
        });
    },

    // Show the login page (hides sidebar).
    showLogin: function() {
        GQM.app.stopPolling();
        GQM.app.authenticated = false;
        document.getElementById('layout').style.display = 'none';
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
    },

    // Called after successful login.
    onLoginSuccess: function(userData) {
        GQM.app.authenticated = true;
        GQM.app.user = userData;
        // Hide login, show layout
        var loginPage = document.getElementById('login-page');
        if (loginPage) loginPage.style.display = 'none';
        document.getElementById('layout').style.display = '';
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
                if (pages.dag) pages.dag.render(app);
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
