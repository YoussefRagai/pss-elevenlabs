import { Conversation } from "https://cdn.jsdelivr.net/npm/@elevenlabs/client@0.6.1/+esm";

const button = document.createElement("button");
button.className = "voice-fab";
button.type = "button";
button.innerHTML = `
  <span class="voice-fab-icon">🎙️</span>
  <span class="voice-fab-text">Hold to talk</span>
`;
button.setAttribute("aria-label", "Hold to talk with Kora");
document.body.appendChild(button);

const status = document.createElement("div");
status.className = "voice-status hidden";
status.textContent = "Kora ready";
document.body.appendChild(status);

let conversation = null;
let ready = false;
let pressed = false;

function setStatus(text, tone = "idle") {
  status.textContent = text;
  status.classList.remove("hidden", "active", "error");
  status.classList.add(tone);
}

async function getConversationToken() {
  const response = await fetch("/api/elevenlabs/conversation-token");
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload?.error || "Unable to start Kora.");
  }
  const data = await response.json();
  return data?.token;
}

async function ensureConversation() {
  if (conversation) return;
  setStatus("Connecting to Kora...", "active");
  const token = await getConversationToken();
  conversation = await Conversation.startSession({
    conversationToken: token,
    connectionType: "webrtc",
    onConnect: () => {
      ready = true;
      setStatus("Kora ready", "idle");
    },
    onDisconnect: () => {
      ready = false;
      conversation = null;
      setStatus("Kora disconnected", "error");
    },
    onError: (error) => {
      ready = false;
      conversation = null;
      setStatus(error?.message || "Kora error", "error");
    },
  });
  conversation.setMicMuted(true);
}

async function startTalking() {
  pressed = true;
  try {
    await ensureConversation();
    if (conversation) {
      conversation.setMicMuted(false);
      setStatus("Listening…", "active");
    }
  } catch (error) {
    setStatus(error.message || "Unable to start Kora.", "error");
  }
}

function stopTalking() {
  pressed = false;
  if (conversation) {
    conversation.setMicMuted(true);
    if (ready) setStatus("Kora ready", "idle");
  }
}

button.addEventListener("pointerdown", (event) => {
  event.preventDefault();
  if (!pressed) startTalking();
});
button.addEventListener("pointerup", (event) => {
  event.preventDefault();
  stopTalking();
});
button.addEventListener("pointerleave", stopTalking);
button.addEventListener("pointercancel", stopTalking);
