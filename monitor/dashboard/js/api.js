// GQM Dashboard — api.js
// Fetch wrapper with auth handling and CSRF.

var GQM = window.GQM || {};

GQM.api = {
    // Base path for API — auto-detected from dashboard location.
    basePath: '',

    // Make an API request. Returns parsed JSON or throws.
    request: function(method, path, body) {
        var url = GQM.api.basePath + path;
        var opts = {
            method: method,
            headers: {},
            credentials: 'same-origin'
        };

        if (body != null) {
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(body);
        }

        // CSRF header for write operations
        if (method !== 'GET') {
            opts.headers['X-GQM-CSRF'] = '1';
        }

        return fetch(url, opts).then(function(resp) {
            if (resp.status === 401) {
                // Session expired or unauthenticated — show login with message.
                if (GQM.app.authenticated) {
                    GQM.app.loginMessage = 'Session expired. Please log in again.';
                }
                GQM.app.showLogin();
                return Promise.reject(new Error('Unauthorized'));
            }
            // Handle 204 No Content (empty body)
            if (resp.status === 204) {
                return { data: null };
            }
            return resp.json().then(function(data) {
                if (!resp.ok) {
                    var msg = (data && data.error) ? data.error : 'Request failed';
                    return Promise.reject(new Error(msg));
                }
                return data;
            });
        });
    },

    // Convenience GET
    get: function(path) {
        return GQM.api.request('GET', path);
    },

    // Convenience POST (with optional body)
    post: function(path, body) {
        return GQM.api.request('POST', path, body);
    },

    // Convenience DELETE
    del: function(path, body) {
        return GQM.api.request('DELETE', path, body);
    },

    // Check authentication status
    checkAuth: function() {
        return GQM.api.get('/auth/me');
    },

    // Login
    login: function(username, password) {
        return GQM.api.post('/auth/login', { username: username, password: password });
    },

    // Logout
    logout: function() {
        return GQM.api.post('/auth/logout');
    }
};
