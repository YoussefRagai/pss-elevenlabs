#!/usr/bin/env node
const fs = require("fs");
const { execFileSync } = require("child_process");

const API_BASE = "https://api.elevenlabs.io/v1";
const apiKey = process.env.ELEVENLABS_API_KEY;
const toolUrl =
  process.env.PSS_TOOL_URL ||
  "https://pss-elevenlabs-production.up.railway.app/api/voice_tool";
const toolSecret = process.env.ELEVENLABS_TOOL_SECRET || "";

if (!apiKey) {
  console.error("Missing ELEVENLABS_API_KEY in environment.");
  process.exit(1);
}

const headers = {
  "xi-api-key": apiKey,
  "Content-Type": "application/json",
};

function request(method, path, body) {
  const url = `${API_BASE}${path}`;
  const args = [
    "-s",
    "-X",
    method,
    url,
    "--max-time",
    "60",
    "-H",
    `xi-api-key: ${apiKey}`,
    "-H",
    "Content-Type: application/json",
    "-w",
    "\\n__STATUS__%{http_code}",
  ];
  if (body) {
    args.push("-d", JSON.stringify(body));
  }
  let raw = "";
  try {
    raw = execFileSync("curl", args, { encoding: "utf8" });
  } catch (error) {
    throw new Error(`curl request failed for ${path}`);
  }
  const parts = raw.split("\n__STATUS__");
  const bodyText = parts[0] || "{}";
  const statusCode = Number(parts[1] || "0");
  let payload = {};
  try {
    payload = JSON.parse(bodyText || "{}");
  } catch (error) {
    payload = {};
  }
  if (statusCode < 200 || statusCode >= 300) {
    const detail =
      payload?.detail || payload?.error || payload?.message || payload;
    const message = typeof detail === "string" ? detail : JSON.stringify(detail);
    throw new Error(message || `HTTP ${statusCode}`);
  }
  return payload;
}

async function getTools() {
  return request("GET", "/convai/tools");
}

async function upsertTool() {
  const toolConfig = {
    name: "pss_query",
    description: "Query the PSS football analytics backend for answers and visuals.",
    type: "webhook",
    response_timeout_secs: 120,
    api_schema: {
      url: toolUrl,
      method: "POST",
      request_headers: {
        "Content-Type": "application/json",
        ...(toolSecret ? { "x-pss-tool-secret": toolSecret } : {}),
      },
      request_body_schema: {
        type: "object",
        required: ["query"],
        properties: {
          query: { type: "string", description: "User request to answer." },
        },
      },
    },
  };

  const tools = await getTools();
  const existing = (tools?.tools || []).find((tool) => tool?.tool_config?.name === "pss_query");
  if (existing?.tool_id) {
    const updated = await request("PATCH", `/convai/tools/${existing.tool_id}`, {
      tool_config: toolConfig,
    });
    return updated?.tool_id || existing.tool_id;
  }

  const created = await request("POST", "/convai/tools", { tool_config: toolConfig });
  return created?.tool_id;
}

async function upsertAgent(toolId) {
  const agentIdEnv = process.env.ELEVENLABS_AGENT_ID;
  const systemPrompt = [
    "You are Kora, the Premium Sports Solutions (PSS) voice assistant.",
    "Always use the pss_query tool to answer football analytics or database questions.",
    "If a visualization is requested, call the tool and then tell the user that the visual is available in the dashboard.",
    "If the question is not about football analytics, answer briefly and politely.",
    "Ask at most one clarification question when needed.",
  ].join(" ");

  const agentConfig = {
    name: "Kora",
    conversation_config: {
      agent: {
        prompt: {
          text: systemPrompt,
          tool_ids: toolId ? [toolId] : [],
        },
        llm: {
          model: "eleven-multilingual-v1",
          temperature: 0.2,
        },
        language: "en",
      },
      tts: {
        model_id: "eleven_turbo_v2",
        voice_id: "pNInz6obpgDQGcFmaJgB",
        agent_output_audio_format: "pcm_16000",
      },
      asr: {
        model_id: "nova-2-general",
        language: "auto",
      },
      conversation: {
        max_duration_seconds: 1800,
        text_only: false,
      },
    },
  };

  if (agentIdEnv) {
    const updated = await request("PATCH", `/convai/agents/${agentIdEnv}`, agentConfig);
    return updated?.agent_id || agentIdEnv;
  }

  const created = await request("POST", "/convai/agents/create", agentConfig);
  return created?.agent_id;
}

async function main() {
  const toolIdEnv = process.env.ELEVENLABS_TOOL_ID;
  let toolId = toolIdEnv || "";
  if (!toolId) {
    console.log("Setting up tool...");
    toolId = await upsertTool();
    console.log("Tool ready:", toolId);
  } else {
    console.log("Using existing tool:", toolId);
  }
  console.log("Setting up agent...");
  const agentId = await upsertAgent(toolId);
  const output = { tool_id: toolId, agent_id: agentId, tool_url: toolUrl };
  fs.writeFileSync("kora.agent.json", JSON.stringify(output, null, 2));
  console.log("Kora agent ready:", output);
}

main().catch((error) => {
  console.error("Setup failed:", error.message || error);
  process.exit(1);
});
