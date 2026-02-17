// GQM Dashboard — login.js
// Login form component.

var GQM = window.GQM || {};
GQM.pages = GQM.pages || {};

GQM.pages.login = {
    render: function(container) {
        container.innerHTML =
            '<div class="login-container">' +
            '<div class="login-box">' +
            '<h1>GQM</h1>' +
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
            '<button type="submit" class="btn btn--primary" id="login-btn">Sign In</button>' +
            '</form>' +
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
                errorEl.style.display = '';
                return;
            }

            btn.disabled = true;
            btn.textContent = 'Signing in...';
            errorEl.style.display = 'none';

            GQM.api.login(username, password).then(function(data) {
                GQM.app.onLoginSuccess(data.data || { username: username });
            }).catch(function(err) {
                errorEl.textContent = err.message || 'Login failed';
                errorEl.style.display = '';
                btn.disabled = false;
                btn.textContent = 'Sign In';
            });
        });

        // Auto-focus username
        var usernameInput = document.getElementById('username');
        if (usernameInput) usernameInput.focus();
    }
};
