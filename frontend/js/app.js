const API_URL = "http://localhost:5000/api/auth";

// ===== Tab switching =====
function showTab(tab) {
    document.getElementById("login-form").classList.toggle("hidden", tab !== "login");
    document.getElementById("register-form").classList.toggle("hidden", tab !== "register");
    document.getElementById("tab-login").classList.toggle("active", tab === "login");
    document.getElementById("tab-register").classList.toggle("active", tab === "register");
    clearAlerts();
}

// ===== Password toggle =====
function togglePassword(inputId, btn) {
    const input = document.getElementById(inputId);
    if (input.type === "password") {
        input.type = "text";
        btn.innerHTML = "&#128064;";
    } else {
        input.type = "password";
        btn.innerHTML = "&#128065;";
    }
}

// ===== Alert helpers =====
function showError(id, msg) {
    const el = document.getElementById(id);
    el.textContent = msg;
    el.classList.remove("hidden");
}

function clearAlerts() {
    ["login-error", "register-error", "register-success"].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.textContent = "";
            el.classList.add("hidden");
        }
    });
}

function setLoading(btnId, loading) {
    const btn = document.getElementById(btnId);
    const text = btn.querySelector(".btn-text");
    const spinner = btn.querySelector(".spinner");
    btn.disabled = loading;
    text.classList.toggle("hidden", loading);
    spinner.classList.toggle("hidden", !loading);
}

// ===== Login =====
async function handleLogin(e) {
    e.preventDefault();
    clearAlerts();

    const email = document.getElementById("login-email").value.trim();
    const password = document.getElementById("login-password").value;

    setLoading("login-btn", true);

    try {
        const res = await fetch(`${API_URL}/login`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email, password })
        });

        const data = await res.json();

        if (!res.ok) {
            const msg = data.message || "Đăng nhập thất bại";
            showError("login-error", translateError(msg));
            return;
        }

        // Store token and user
        localStorage.setItem("token", data.token);
        localStorage.setItem("user", JSON.stringify(data.user));

        showDashboard(data.user);
    } catch (err) {
        showError("login-error", "Không thể kết nối đến máy chủ. Hãy kiểm tra backend đang chạy.");
    } finally {
        setLoading("login-btn", false);
    }
}

// ===== Register =====
async function handleRegister(e) {
    e.preventDefault();
    clearAlerts();

    const username = document.getElementById("reg-username").value.trim();
    const email = document.getElementById("reg-email").value.trim();
    const password = document.getElementById("reg-password").value;
    const confirm = document.getElementById("reg-confirm").value;

    if (password !== confirm) {
        showError("register-error", "Mật khẩu xác nhận không khớp.");
        return;
    }

    setLoading("register-btn", true);

    try {
        const res = await fetch(`${API_URL}/register`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username, email, password })
        });

        const data = await res.json();

        if (!res.ok) {
            const msg = data.message || "Đăng ký thất bại";
            showError("register-error", translateError(msg));
            return;
        }

        // Show success then switch to login
        const successEl = document.getElementById("register-success");
        successEl.textContent = "Đăng ký thành công! Chuyển sang đăng nhập...";
        successEl.classList.remove("hidden");

        document.getElementById("register-form").reset();

        setTimeout(() => {
            showTab("login");
            document.getElementById("login-email").value = email;
        }, 1500);

    } catch (err) {
        showError("register-error", "Không thể kết nối đến máy chủ. Hãy kiểm tra backend đang chạy.");
    } finally {
        setLoading("register-btn", false);
    }
}

// ===== Logout =====
function handleLogout() {
    localStorage.removeItem("token");
    localStorage.removeItem("user");
    document.getElementById("dashboard").classList.add("hidden");
    document.getElementById("dashboard").style.display = "";
    document.querySelector(".container").classList.remove("hidden");
    clearAlerts();
    document.getElementById("login-form").reset();
    document.getElementById("register-form").reset();
    showTab("login");
}

// ===== Dashboard =====
function showDashboard(user) {
    document.querySelector(".container").classList.add("hidden");

    const dashboard = document.getElementById("dashboard");
    dashboard.classList.remove("hidden");
    dashboard.style.display = "flex";

    document.getElementById("user-display").textContent = user.email;
    document.getElementById("welcome-name").textContent = user.username || user.email;

    document.getElementById("user-info").innerHTML = `
        <div class="info-row">
            <span class="info-label">ID</span>
            <span class="info-value">${user.id}</span>
        </div>
        <div class="info-row">
            <span class="info-label">Tên</span>
            <span class="info-value">${user.username}</span>
        </div>
        <div class="info-row">
            <span class="info-label">Email</span>
            <span class="info-value">${user.email}</span>
        </div>
    `;
}

// ===== Error translation =====
function translateError(msg) {
    const map = {
        "Missing fields": "Vui lòng điền đầy đủ thông tin.",
        "Email already exists": "Email này đã được đăng ký.",
        "User not found": "Email không tồn tại.",
        "Wrong password": "Mật khẩu không đúng."
    };
    return map[msg] || msg;
}

// ===== Auto-login if token exists =====
(function init() {
    const token = localStorage.getItem("token");
    const user = localStorage.getItem("user");
    if (token && user) {
        try {
            showDashboard(JSON.parse(user));
        } catch {
            localStorage.clear();
        }
    }
})();
