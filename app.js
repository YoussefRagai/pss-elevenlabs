const messagesEl = document.getElementById("messages");
const composer = document.getElementById("composer");
const userInput = document.getElementById("userInput");
const systemPrompt = document.getElementById("systemPrompt");
const systemBlock = document.getElementById("systemBlock");
const toggleSystemBtn = document.getElementById("toggleSystem");
const temperatureInput = document.getElementById("temperature");
const temperatureValue = document.getElementById("temperatureValue");
const maxTokensInput = document.getElementById("maxTokens");
const saveSettingsBtn = document.getElementById("saveSettings");
const statusEl = document.getElementById("status");
const envStatus = document.getElementById("envStatus");
const clearChatBtn = document.getElementById("clearChat");

const SETTINGS_KEY = "openrouterSettings";
const CHAT_KEY = "openrouterChat";
const MODEL_ID = "openai/gpt-oss-120b:free";

const state = {
  messages: [],
  busy: false,
};
const pendingInputs = [];
let fillerTimer = null;
let fillerStep = 0;

function renderMessage(role, content) {
  const bubble = document.createElement("div");
  bubble.className = `message ${role}`;
  bubble.textContent = content;
  messagesEl.appendChild(bubble);
  messagesEl.scrollTop = messagesEl.scrollHeight;
  return bubble;
}

function renderMessages() {
  messagesEl.innerHTML = "";
  if (!state.messages.length) {
    renderMessage("system", "Start a new conversation.");
  }
  state.messages.forEach((message) => {
    renderMessage(message.role, message.content);
  });
}

function setStatus(text, tone = "") {
  statusEl.textContent = text;
  statusEl.style.color = tone === "error" ? "#f28b82" : "var(--accent-2)";
}

function loadSettings() {
  const saved = JSON.parse(localStorage.getItem(SETTINGS_KEY) || "{}");
  if (saved.temperature != null) temperatureInput.value = saved.temperature;
  if (saved.maxTokens) maxTokensInput.value = saved.maxTokens;
  if (saved.systemPrompt) systemPrompt.value = saved.systemPrompt;
  temperatureValue.textContent = Number(temperatureInput.value).toFixed(2);
}

function saveSettings() {
  const settings = {
    temperature: Number(temperatureInput.value),
    maxTokens: Number(maxTokensInput.value),
    systemPrompt: systemPrompt.value,
  };
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
  setStatus("Settings saved.");
}

function loadChat() {
  const saved = JSON.parse(localStorage.getItem(CHAT_KEY) || "[]");
  state.messages = Array.isArray(saved) ? saved : [];
}

function saveChat() {
  localStorage.setItem(CHAT_KEY, JSON.stringify(state.messages));
}

function setEnvStatus() {
  envStatus.textContent = "Supabase MCP ready · key stays on server";
}

async function sendToOpenRouter(messageList, source) {
  const settings = JSON.parse(localStorage.getItem(SETTINGS_KEY) || "{}");

  const response = await fetch("/api/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: MODEL_ID,
      temperature: settings.temperature ?? 0.7,
      max_tokens: settings.maxTokens ?? 1024,
      messages: messageList,
      source,
    }),
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    const detail = payload?.error || payload?.message || response.statusText;
    throw new Error(`OpenRouter error: ${detail}`);
  }

  const data = await response.json();
  return {
    content: data.choices?.[0]?.message?.content || "(no response)",
    image: data.image || null,
  };
}

function buildMessagePayload() {
  const payload = [];
  if (systemPrompt.value.trim()) {
    payload.push({ role: "system", content: systemPrompt.value.trim() });
  }
  payload.push(...state.messages);
  return payload;
}

function lockComposer(locked) {
  state.busy = locked;
  userInput.disabled = locked;
  composer.querySelector("button").disabled = locked;
}

function startFiller(messageEl) {
  const fillers = [
    "Processing the dataset…",
    "Pulling events from the database…",
    "Preparing the visualization…",
    "Finalizing the chart…",
  ];
  fillerStep = 0;
  fillerTimer = setInterval(() => {
    if (!messageEl) return;
    const next = fillers[Math.min(fillerStep, fillers.length - 1)];
    messageEl.textContent = next;
    fillerStep += 1;
  }, 1200);
}

function stopFiller() {
  if (fillerTimer) {
    clearInterval(fillerTimer);
    fillerTimer = null;
  }
}

async function processUserMessage(content, options = {}) {
  if (state.busy) {
    pendingInputs.push(content);
    return;
  }
  const userMessage = { role: "user", content };
  state.messages.push(userMessage);
  renderMessage("user", content);
  saveChat();

  const thinking = renderMessage("system", "Listening…");
  startFiller(thinking);
  lockComposer(true);

  try {
    const reply = await sendToOpenRouter(buildMessagePayload(), options.source);
    stopFiller();
    thinking.remove();
    const assistantMessage = { role: "assistant", content: reply.content };
    state.messages.push(assistantMessage);
    renderMessage("assistant", reply.content);
    if (reply.image?.image_base64) {
      const img = document.createElement("img");
      img.src = `data:${reply.image.mime || "image/png"};base64,${reply.image.image_base64}`;
      img.alt = "Visualization";
      img.className = "chat-image";
      messagesEl.appendChild(img);
      messagesEl.scrollTop = messagesEl.scrollHeight;
    }
    saveChat();
  } catch (error) {
    stopFiller();
    thinking.remove();
    renderMessage("system", error.message);
  } finally {
    lockComposer(false);
    if (pendingInputs.length) {
      const next = pendingInputs.shift();
      if (next) processUserMessage(next);
    }
  }
}

composer.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (state.busy) return;

  const content = userInput.value.trim();
  if (!content) return;

  userInput.value = "";
  processUserMessage(content);
});

clearChatBtn.addEventListener("click", () => {
  state.messages = [];
  saveChat();
  renderMessages();
});

systemPrompt.addEventListener("input", saveSettings);

saveSettingsBtn.addEventListener("click", saveSettings);

temperatureInput.addEventListener("input", () => {
  temperatureValue.textContent = Number(temperatureInput.value).toFixed(2);
});

toggleSystemBtn.addEventListener("click", () => {
  systemBlock.classList.toggle("hidden");
});

loadSettings();
loadChat();
renderMessages();
setEnvStatus();

window.handleVoiceInput = (text) => {
  const content = String(text || "").trim();
  if (!content) return;
  processUserMessage(content, { source: "voice" });
};

window.handleVoiceAssistant = (text) => {
  const content = String(text || "").trim();
  if (!content) return;
  const assistantMessage = { role: "assistant", content };
  state.messages.push(assistantMessage);
  renderMessage("assistant", content);
  saveChat();
};
