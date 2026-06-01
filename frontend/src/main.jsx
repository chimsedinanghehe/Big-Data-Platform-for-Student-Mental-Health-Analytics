import "./App.css";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";
const DASHBOARD_URL = import.meta.env.VITE_DASHBOARD_URL || "http://127.0.0.1:8501";
const SESSION_KEY = "student-support-chat-session";
const AUTH_TOKEN_KEY = "student-platform-auth-token";

const state = {
  activeView: "chat",
  authMode: "login",
  authChecking: false,
  authToken: "",
  currentUser: null,
  messages: [
    {
      id: "welcome",
      role: "assistant",
      content:
        "Hi. Ask a student wellbeing question and I will answer using the trusted knowledge base when relevant.",
    },
  ],
  input: "",
  isLoading: false,
  error: "",
  sessionId: "",
  auth: {
    email: "",
    password: "",
    displayName: "",
    role: "user",
    status: "",
    error: "",
    fieldErrors: {},
    focusField: "",
    isSaving: false,
    studentProfile: {
      age: "",
      gender: "other",
      learnerType: "university",
    },
  },
};

function createSessionId() {
  const browserCrypto = globalThis.crypto;
  if (browserCrypto?.randomUUID) {
    return browserCrypto.randomUUID();
  }
  return `session-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function initializeSession() {
  const stored = localStorage.getItem(SESSION_KEY);
  state.sessionId = stored || createSessionId();
  localStorage.setItem(SESSION_KEY, state.sessionId);

  state.authToken = localStorage.getItem(AUTH_TOKEN_KEY) || "";
  if (state.authToken) {
    state.authChecking = true;
    loadCurrentUser();
  }
}

function render() {
  const root = document.getElementById("root");
  root.innerHTML = "";

  const main = el("main", {
    className: state.currentUser ? "app-shell app-main-shell" : "app-shell auth-main-shell",
  });
  if (!state.currentUser) {
    main.appendChild(renderAuthWorkspace());
  } else {
    main.appendChild(renderWorkspace());
  }

  root.appendChild(main);

  const messageList = root.querySelector(".message-list");
  if (messageList) {
    messageList.scrollTop = messageList.scrollHeight;
  }

  if (state.auth.focusField) {
    const field = root.querySelector(`[data-field="${state.auth.focusField}"]`);
    if (field) {
      field.focus();
      field.scrollIntoView({ block: "center", behavior: "smooth" });
    }
    state.auth.focusField = "";
  }
}

function renderAuthWorkspace() {
  const workspace = el("section", { className: "workspace auth-workspace" });
  const header = el("header", { className: "app-header auth-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h1", {}, "Student Mental Health Platform"));
  titleWrap.appendChild(el("p", {}, "Login first to use the dashboard and chatbot."));
  header.appendChild(titleWrap);
  workspace.appendChild(header);

  if (state.authChecking) {
    const panel = el("section", { className: "auth-panel" });
    panel.appendChild(el("div", { className: "empty-state" }, "Checking session..."));
    workspace.appendChild(panel);
    return workspace;
  }

  workspace.appendChild(renderAuth());
  return workspace;
}

function renderWorkspace() {
  normalizeActiveView();

  const workspace = el("section", { className: "workspace app-workspace role-user" });
  workspace.appendChild(renderTopBar());

  const body = el("div", { className: "app-body" });
  body.appendChild(renderSidebar());

  const content = el("section", { className: "main-content" });
  content.appendChild(renderActiveView());
  body.appendChild(content);
  workspace.appendChild(body);
  return workspace;
}

function renderTopBar() {
  const header = el("header", { className: "topbar" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h1", {}, "Student Mental Health Platform"));
  titleWrap.appendChild(el("p", {}, "Chatbot, analytics dashboard, and account profile."));
  header.appendChild(titleWrap);

  const right = el("div", { className: "header-actions" });
  right.appendChild(el("div", { className: "account-chip" }, state.currentUser.display_name));
  const logoutButton = el("button", { type: "button", className: "secondary-button" }, "Logout");
  logoutButton.addEventListener("click", logout);
  right.appendChild(logoutButton);
  header.appendChild(right);
  return header;
}

function renderSidebar() {
  const sidebar = el("aside", { className: "sidebar", "aria-label": "Workspace navigation" });
  const label = el("div", { className: "sidebar-label" }, "Workspace");
  sidebar.appendChild(label);

  const nav = el("nav", { className: "side-nav" });
  for (const item of getNavItems()) {
    nav.appendChild(renderSideNavItem(item.view, item.label));
  }
  sidebar.appendChild(nav);
  return sidebar;
}

function renderSideNavItem(view, label) {
  const button = el(
    "button",
    {
      type: "button",
      className: state.activeView === view ? "side-nav-item active" : "side-nav-item",
    },
    label,
  );
  button.addEventListener("click", () => {
    state.activeView = view;
    render();
  });
  return button;
}

function getNavItems() {
  return [
    { view: "chat", label: "Chatbot" },
    { view: "dashboard", label: "Dashboard" },
    { view: "profile", label: "Profile" },
  ];
}

function normalizeActiveView() {
  const allowed = getNavItems().map((item) => item.view);
  if (!allowed.includes(state.activeView)) {
    state.activeView = allowed[0];
  }
}

function renderActiveView() {
  if (state.activeView === "chat") {
    return renderChat();
  }
  if (state.activeView === "profile") {
    return renderProfile();
  }
  return renderDashboard();
}

function renderAuth() {
  const panel = el("section", {
    className: `auth-panel auth-mode-${state.authMode}`,
    "aria-label": "Authentication",
  });

  const modeTabs = el("div", { className: "mode-tabs" });
  modeTabs.appendChild(renderModeButton("login", "Login"));
  modeTabs.appendChild(renderModeButton("register", "Register"));
  panel.appendChild(modeTabs);

  const form = el("form", { className: `user-form wide auth-form auth-form-${state.authMode}` });
  form.appendChild(renderInput("Email", "email", state.auth.email, "email", "you@example.com", (value) => {
    updateAuthField("email", value);
  }));
  form.appendChild(renderInput("Password", "password", state.auth.password, "password", state.authMode === "login" ? "Enter your password" : "At least 8 characters", (value) => {
    updateAuthField("password", value);
  }));

  if (state.authMode === "register") {
    form.appendChild(renderInput("Display name", "text", state.auth.displayName, "displayName", "Name shown in the app", (value) => {
      updateAuthField("displayName", value);
    }));
    appendStudentFields(form);
  }

  const buttonText = state.auth.isSaving ? "Working..." : state.authMode === "login" ? "Login" : "Register";
  const button = el("button", { type: "submit", className: "primary-button" }, buttonText);
  button.disabled = state.auth.isSaving;
  form.appendChild(button);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    state.authMode === "login" ? login() : register();
  });
  panel.appendChild(form);

  appendStatus(panel);
  return panel;
}

function renderModeButton(mode, label) {
  const button = el("button", { type: "button", className: state.authMode === mode ? "tab active" : "tab" }, label);
  button.addEventListener("click", () => {
    state.authMode = mode;
    state.auth.status = "";
    state.auth.error = "";
    state.auth.fieldErrors = {};
    state.auth.focusField = "";
    render();
  });
  return button;
}

function renderDashboard() {
  const panel = el("section", { className: "dashboard-panel", "aria-label": "Analytics dashboard" });
  panel.appendChild(el("iframe", {
    className: "dashboard-frame",
    title: "Mental school analytics dashboard",
    src: DASHBOARD_URL,
  }));
  return panel;
}

function renderChat() {
  const panel = el("section", {
    className: "chat-panel",
    "aria-label": "Student support chatbot",
  });

  panel.appendChild(renderChatHeader());
  panel.appendChild(renderMessages());

  if (state.error) {
    panel.appendChild(el("div", { className: "error-banner" }, state.error));
  }

  panel.appendChild(renderComposer());
  return panel;
}

function renderChatHeader() {
  const header = el("header", { className: "panel-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h2", {}, "Student Support Chat"));
  titleWrap.appendChild(el("p", {}, "Grounded wellbeing guidance from the project knowledge base."));
  header.appendChild(titleWrap);
  header.appendChild(el("div", { className: "status-pill" }, "Qdrant RAG"));
  return header;
}

function renderProfile() {
  const panel = el("section", { className: "user-panel", "aria-label": "User profile" });
  const header = el("header", { className: "panel-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h2", {}, "Profile"));
  titleWrap.appendChild(el("p", {}, "Only account fields required for the app are shown here."));
  header.appendChild(titleWrap);
  header.appendChild(el("div", { className: "status-pill" }, "PostgreSQL"));
  panel.appendChild(header);

  const form = el("form", { className: "user-form wide" });
  form.appendChild(renderReadonlyField("Email", state.currentUser.email));
  form.appendChild(renderInput("Display name", "text", state.auth.displayName, "displayName", "Name shown in the app", (value) => {
    updateAuthField("displayName", value);
  }));
  appendStudentFields(form);

  const button = el("button", { type: "submit", className: "primary-button" }, state.auth.isSaving ? "Saving..." : "Save profile");
  button.disabled = state.auth.isSaving;
  form.appendChild(button);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    saveProfile();
  });

  panel.appendChild(form);
  appendStatus(panel);
  return panel;
}

function appendStudentFields(form) {
  form.appendChild(renderInput("Age", "number", state.auth.studentProfile.age, "age", "e.g. 20", (value) => {
    state.auth.studentProfile.age = value;
    clearFieldError("age");
  }));
  form.appendChild(renderSelect("Gender", state.auth.studentProfile.gender, [
    ["male", "Male"],
    ["female", "Female"],
    ["other", "Other"],
  ], (value) => {
    state.auth.studentProfile.gender = value;
    clearFieldError("gender");
  }));
  form.appendChild(renderSelect("Learner type", state.auth.studentProfile.learnerType, [
    ["elementary", "Elementary"],
    ["middle_school", "Middle school"],
    ["high_school", "High school"],
    ["college", "College"],
    ["university", "University"],
    ["graduate", "Graduate"],
    ["other", "Other"],
  ], (value) => {
    state.auth.studentProfile.learnerType = value;
    clearFieldError("learnerType");
  }));
}

function appendStatus(panel) {
  if (state.auth.status) {
    panel.appendChild(el("div", { className: "success-banner" }, state.auth.status));
  }
  if (state.auth.error) {
    panel.appendChild(el("div", { className: "error-banner" }, state.auth.error));
  }
}

function renderInput(label, type, value, fieldName, placeholder, onInput) {
  const error = state.auth.fieldErrors[fieldName];
  const wrap = el("label", { className: error ? "field has-error" : "field" });
  wrap.appendChild(el("span", {}, label));
  const input = el("input", {
    type,
    placeholder,
    "data-field": fieldName,
    "aria-invalid": error ? "true" : "false",
  });
  input.value = value ?? "";
  input.addEventListener("input", (event) => onInput(event.target.value));
  wrap.appendChild(input);
  appendFieldError(wrap, fieldName);
  return wrap;
}

function renderReadonlyField(label, value) {
  const wrap = el("label", { className: "field" });
  wrap.appendChild(el("span", {}, label));
  const input = el("input", { type: "text", readonly: "readonly" });
  input.value = value ?? "";
  wrap.appendChild(input);
  return wrap;
}

function renderSelect(label, value, options, onInput) {
  const fieldName = label === "Gender" ? "gender" : "learnerType";
  const error = state.auth.fieldErrors[fieldName];
  const wrap = el("label", { className: error ? "field has-error" : "field" });
  wrap.appendChild(el("span", {}, label));
  const select = el("select", {
    "data-field": fieldName,
    "aria-invalid": error ? "true" : "false",
  });
  for (const [optionValue, optionLabel] of options) {
    const option = el("option", { value: optionValue }, optionLabel);
    if (value === optionValue) {
      option.selected = true;
    }
    select.appendChild(option);
  }
  select.addEventListener("change", (event) => onInput(event.target.value));
  wrap.appendChild(select);
  appendFieldError(wrap, fieldName);
  return wrap;
}

function renderMessages() {
  const list = el("div", { className: "message-list" });

  for (const message of state.messages) {
    list.appendChild(renderMessage(message));
  }

  if (state.isLoading) {
    const node = renderMessage({ id: "loading", role: "assistant", content: "Thinking..." });
    node.querySelector(".message-body").classList.add("loading");
    list.appendChild(node);
  }

  return list;
}

function renderMessage(message) {
  const article = el("article", { className: `message ${message.role}` });
  const avatar = el("div", { className: "avatar", "aria-hidden": "true" }, message.role === "assistant" ? "AI" : "You");
  const body = el("div", { className: "message-body" });
  body.appendChild(el("p", {}, message.content));

  article.appendChild(avatar);
  article.appendChild(body);
  return article;
}

function renderComposer() {
  const form = el("form", { className: "composer" });
  const textarea = el("textarea", {
    "aria-label": "Message",
    placeholder: "Type your question...",
    rows: "2",
  });
  textarea.value = state.input;
  textarea.addEventListener("input", (event) => {
    state.input = event.target.value;
    updateSendButton(form);
  });
  textarea.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      handleSubmit();
    }
  });

  const button = el("button", { type: "submit", "aria-label": "Send message" }, state.isLoading ? "..." : ">");
  button.disabled = state.isLoading || !state.input.trim();

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    handleSubmit();
  });

  form.appendChild(textarea);
  form.appendChild(button);
  return form;
}

function updateSendButton(form) {
  const button = form.querySelector("button");
  button.disabled = state.isLoading || !state.input.trim();
}

async function handleSubmit() {
  const question = state.input.trim();
  if (!question || state.isLoading) {
    return;
  }

  const chatHistory = state.messages
    .filter((message) => message.id !== "welcome")
    .map((message) => ({ role: message.role, content: message.content }))
    .slice(-10);

  state.messages.push({ id: createSessionId(), role: "user", content: question });
  state.input = "";
  state.error = "";
  state.isLoading = true;
  render();

  try {
    const response = await fetch(`${API_BASE_URL}/api/rag/ask`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        question,
        session_id: state.sessionId || createSessionId(),
        chat_history: chatHistory,
      }),
    });

    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.detail?.message || "The chat service returned an error.");
    }

    if (payload.session_id) {
      state.sessionId = payload.session_id;
      localStorage.setItem(SESSION_KEY, state.sessionId);
    }

    state.messages.push({ id: createSessionId(), role: "assistant", content: payload.answer });
  } catch (error) {
    state.error = error instanceof Error ? error.message : "Unable to send the message.";
  } finally {
    state.isLoading = false;
    render();
  }
}

async function register() {
  if (!validateAuthForm("register")) {
    render();
    return;
  }

  const payload = {
    email: state.auth.email,
    password: state.auth.password,
    display_name: state.auth.displayName,
    role: "user",
    student_profile: buildStudentProfilePayload(),
  };
  await authenticate("/api/auth/register", payload, "Registration complete.");
}

async function login() {
  if (!validateAuthForm("login")) {
    render();
    return;
  }

  await authenticate("/api/auth/login", {
    email: state.auth.email,
    password: state.auth.password,
  }, "Login complete.");
}

async function authenticate(path, body, successMessage) {
  state.auth.isSaving = true;
  state.auth.status = "";
  state.auth.error = "";
  state.auth.fieldErrors = {};
  render();

  try {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.detail?.message || "Authentication failed.");
    }
    applyAuth(payload);
    state.auth.status = successMessage;
    state.activeView = defaultView();
  } catch (error) {
    state.auth.error = error instanceof Error ? error.message : "Authentication failed.";
  } finally {
    state.auth.isSaving = false;
    render();
  }
}

async function loadCurrentUser() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/auth/me`, {
      headers: authHeaders(),
    });
    if (!response.ok) {
      throw new Error("Session expired.");
    }
    state.currentUser = await response.json();
    syncAuthFormFromUser(state.currentUser);
    normalizeActiveView();
  } catch {
    logout(false);
  } finally {
    state.authChecking = false;
    render();
  }
}

async function saveProfile() {
  if (!validateProfileForm()) {
    render();
    return;
  }

  state.auth.isSaving = true;
  state.auth.status = "";
  state.auth.error = "";
  state.auth.fieldErrors = {};
  render();

  const payload = {
    display_name: state.auth.displayName,
    role: "user",
    student_profile: buildStudentProfilePayload(),
  };

  try {
    const response = await fetch(`${API_BASE_URL}/api/users/me`, {
      method: "PUT",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result?.detail?.message || "Unable to save profile.");
    }
    state.currentUser = result;
    syncAuthFormFromUser(result);
    normalizeActiveView();
    state.auth.status = "Profile saved.";
  } catch (error) {
    state.auth.error = error instanceof Error ? error.message : "Unable to save profile.";
  } finally {
    state.auth.isSaving = false;
    render();
  }
}

function applyAuth(payload) {
  state.authToken = payload.access_token;
  localStorage.setItem(AUTH_TOKEN_KEY, state.authToken);
  state.currentUser = payload.user;
  syncAuthFormFromUser(payload.user);
}

function logout(shouldRender = true) {
  state.authToken = "";
  state.currentUser = null;
  resetAuthForm();
  state.activeView = "chat";
  state.authChecking = false;
  localStorage.removeItem(AUTH_TOKEN_KEY);
  if (shouldRender) {
    render();
  }
}

function syncAuthFormFromUser(user) {
  state.auth.email = user.email || "";
  state.auth.displayName = user.display_name || "";
  state.auth.role = "user";
  const profile = user.profile || {};
  state.auth.studentProfile = {
    age: profile.age ?? "",
    gender: profile.gender || "other",
    learnerType: profile.learner_type || "university",
  };
}

function defaultView() {
  return "chat";
}

function resetAuthForm() {
  state.authMode = "login";
  state.auth.email = "";
  state.auth.password = "";
  state.auth.displayName = "";
  state.auth.role = "user";
  state.auth.status = "";
  state.auth.error = "";
  state.auth.fieldErrors = {};
  state.auth.focusField = "";
  state.auth.isSaving = false;
  state.auth.studentProfile = {
    age: "",
    gender: "other",
    learnerType: "university",
  };
}

function updateAuthField(fieldName, value) {
  state.auth[fieldName] = value;
  clearFieldError(fieldName);
}

function clearFieldError(fieldName) {
  if (state.auth.fieldErrors[fieldName]) {
    delete state.auth.fieldErrors[fieldName];
  }
}

function appendFieldError(wrap, fieldName) {
  const message = state.auth.fieldErrors[fieldName];
  if (message) {
    wrap.appendChild(el("span", { className: "field-error" }, message));
  }
}

function validateAuthForm(mode) {
  const errors = {};
  if (!state.auth.email.trim()) {
    errors.email = "Email is required.";
  } else if (!isValidEmail(state.auth.email)) {
    errors.email = "Enter a valid email address.";
  }

  if (!state.auth.password.trim()) {
    errors.password = "Password is required.";
  } else if (mode === "register" && state.auth.password.length < 8) {
    errors.password = "Password must contain at least 8 characters.";
  }

  if (mode === "register") {
    if (!state.auth.displayName.trim()) {
      errors.displayName = "Display name is required.";
    }
    validateStudentFields(errors);
  }

  return applyValidationErrors(errors);
}

function validateProfileForm() {
  const errors = {};
  if (!state.auth.displayName.trim()) {
    errors.displayName = "Display name is required.";
  }
  validateStudentFields(errors);
  return applyValidationErrors(errors);
}

function validateStudentFields(errors) {
  const age = Number(state.auth.studentProfile.age);
  if (String(state.auth.studentProfile.age).trim() === "") {
    errors.age = "Age is required.";
  } else if (!Number.isFinite(age) || age < 5 || age > 100) {
    errors.age = "Age must be between 5 and 100.";
  }
  if (!state.auth.studentProfile.gender) {
    errors.gender = "Gender is required.";
  }
  if (!state.auth.studentProfile.learnerType) {
    errors.learnerType = "Learner type is required.";
  }
}

function applyValidationErrors(errors) {
  state.auth.fieldErrors = errors;
  state.auth.error = "";
  state.auth.status = "";
  const firstField = Object.keys(errors)[0];
  state.auth.focusField = firstField || "";
  return !firstField;
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

function buildStudentProfilePayload() {
  return {
    age: optionalNumber(state.auth.studentProfile.age),
    gender: state.auth.studentProfile.gender,
    learner_type: state.auth.studentProfile.learnerType,
  };
}

function authHeaders() {
  return {
    Authorization: `Bearer ${state.authToken}`,
  };
}

function optionalNumber(value) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return null;
  }
  return Number(value);
}

function el(tagName, attributes = {}, text = "") {
  const node = document.createElement(tagName);
  for (const [name, value] of Object.entries(attributes)) {
    if (name === "className") {
      node.className = value;
    } else if (name === "readonly") {
      node.readOnly = true;
    } else {
      node.setAttribute(name, value);
    }
  }
  if (text) {
    node.textContent = text;
  }
  return node;
}

initializeSession();
render();
