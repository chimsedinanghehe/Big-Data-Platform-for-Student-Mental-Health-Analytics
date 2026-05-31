import "./App.css";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const SESSION_KEY = "student-support-chat-session";

const state = {
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
}

function render() {
  const root = document.getElementById("root");
  root.innerHTML = "";

  const main = el("main", { className: "app-shell" });
  const panel = el("section", {
    className: "chat-panel",
    "aria-label": "Student support chatbot",
  });

  panel.appendChild(renderHeader());
  panel.appendChild(renderMessages());

  if (state.error) {
    panel.appendChild(el("div", { className: "error-banner" }, state.error));
  }

  panel.appendChild(renderComposer());
  main.appendChild(panel);
  root.appendChild(main);

  const messageList = panel.querySelector(".message-list");
  messageList.scrollTop = messageList.scrollHeight;
}

function renderHeader() {
  const header = el("header", { className: "chat-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h1", {}, "Student Support Chat"));
  titleWrap.appendChild(el("p", {}, "Grounded wellbeing guidance from the project knowledge base."));
  header.appendChild(titleWrap);
  header.appendChild(el("div", { className: "status-pill" }, "Qdrant RAG"));
  return header;
}

function renderMessages() {
  const list = el("div", { className: "message-list" });

  for (const message of state.messages) {
    list.appendChild(renderMessage(message));
  }

  if (state.isLoading) {
    const loading = {
      id: "loading",
      role: "assistant",
      content: "Thinking...",
    };
    const node = renderMessage(loading);
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
    .map((message) => ({
      role: message.role,
      content: message.content,
    }))
    .slice(-10);

  state.messages.push({
    id: createSessionId(),
    role: "user",
    content: question,
  });
  state.input = "";
  state.error = "";
  state.isLoading = true;
  render();

  try {
    const response = await fetch(`${API_BASE_URL}/api/rag/ask`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
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

    state.messages.push({
      id: createSessionId(),
      role: "assistant",
      content: payload.answer,
    });
  } catch (error) {
    state.error = error instanceof Error ? error.message : "Unable to send the message.";
  } finally {
    state.isLoading = false;
    render();
  }
}

function el(tagName, attributes = {}, text = "") {
  const node = document.createElement(tagName);
  for (const [name, value] of Object.entries(attributes)) {
    if (name === "className") {
      node.className = value;
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
