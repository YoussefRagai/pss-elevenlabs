import { Conversation } from "https://cdn.jsdelivr.net/npm/@elevenlabs/client@0.6.1/+esm";

const orb = document.getElementById("voiceOrb");
const toggleBtn = document.getElementById("voiceToggle");
const statusLabel = document.getElementById("voiceStatus");

let conversation = null;
let active = false;

function setStatus(text, tone = "idle") {
  statusLabel.textContent = text;
  statusLabel.dataset.state = tone;
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

async function startConversation() {
  if (active) return;
  setStatus("Connecting…", "active");
  const token = await getConversationToken();
  conversation = await Conversation.startSession({
    conversationToken: token,
    connectionType: "webrtc",
    onConnect: () => {
      setStatus("Listening…", "active");
    },
    onDisconnect: () => {
      active = false;
      conversation = null;
      orb.classList.remove("active");
      toggleBtn.textContent = "Start Kora";
      setStatus("Kora idle", "idle");
    },
    onError: (error) => {
      active = false;
      conversation = null;
      orb.classList.remove("active");
      toggleBtn.textContent = "Start Kora";
      setStatus(error?.message || "Kora error", "error");
    },
  });
  conversation.setMicMuted(false);
  active = true;
  orb.classList.add("active");
  toggleBtn.textContent = "Hang Up";
  setStatus("Listening…", "active");
}

async function stopConversation() {
  if (!conversation) {
    setStatus("Kora idle", "idle");
    return;
  }
  try {
    conversation.setMicMuted(true);
    if (typeof conversation.endSession === "function") {
      await conversation.endSession();
    }
  } catch (error) {
    // ignore disconnect errors
  }
  active = false;
  orb.classList.remove("active");
  toggleBtn.textContent = "Start Kora";
  setStatus("Kora idle", "idle");
}

toggleBtn.addEventListener("click", async () => {
  if (!active) {
    try {
      await startConversation();
    } catch (error) {
      setStatus(error.message || "Unable to start Kora.", "error");
    }
  } else {
    await stopConversation();
  }
});

orb.addEventListener("click", async () => {
  if (!active) {
    try {
      await startConversation();
    } catch (error) {
      setStatus(error.message || "Unable to start Kora.", "error");
    }
  } else {
    await stopConversation();
  }
});
