// GQM Dashboard — login.js
// Login form component.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.login = {
    render: function(container) {
        container.innerHTML =
            '<div class="login-container">' +
            '<div class="login-box">' +
            '<div class="login-brand">' +
            '<div class="brand-mark">' +
            '<svg width="20" height="20" viewBox="0 0 16 16" fill="none" stroke="#08211c" stroke-width="2" stroke-linecap="round">' +
            '<path d="M2 4h9M2 8h12M2 12h7"/><circle cx="13.5" cy="4" r="1.4" fill="#08211c" stroke="none"/>' +
            '</svg>' +
            '</div>' +
            '<div><span class="name">GQM</span><span class="tagline">Go Queue Manager</span></div>' +
            '</div>' +
            '<div id="login-error" class="login-error"></div>' +
            '<form id="login-form">' +
            '<div class="form-group">' +
            '<label for="username">Username</label>' +
            '<input type="text" id="username" name="username" autocomplete="username" required>' +
            '</div>' +
            '<div class="form-group">' +
            '<label for="password">Password</label>' +
            '<input type="password" id="password" name="password" autocomplete="current-password" required>' +
            '</div>' +
            '<button type="submit" class="btn btn--primary" id="login-btn">Sign in</button>' +
            '</form>' +
            '<div class="login-footer">GQM Dashboard</div>' +
            '</div>' +
            '</div>';

        var form = document.getElementById('login-form');
        form.addEventListener('submit', function(e) {
            e.preventDefault();
            var username = document.getElementById('username').value.trim();
            var password = document.getElementById('password').value;
            var errorEl = document.getElementById('login-error');
            var btn = document.getElementById('login-btn');

            if (!username || !password) {
                errorEl.textContent = 'Please enter username and password';
                errorEl.style.display = 'block';
                return;
            }

            btn.disabled = true;
            btn.textContent = 'Signing in...';
            errorEl.style.display = 'none';

            GQM.api.login(username, password).then(function(data) {
                GQM.app.onLoginSuccess(data.data || { username: username });
            }).catch(function(err) {
                errorEl.textContent = err.message || 'Login failed';
                errorEl.style.display = 'block';
                btn.disabled = false;
                btn.textContent = 'Sign in';
            });
        });

        // Auto-focus username
        var usernameInput = document.getElementById('username');
        if (usernameInput) usernameInput.focus();
    }
};
