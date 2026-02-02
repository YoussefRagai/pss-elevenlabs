#!/usr/bin/env node
const fs = require("fs");
const https = require("https");

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
  return new Promise((resolve, reject) => {
    const url = new URL(`${API_BASE}${path}`);
    const data = body ? JSON.stringify(body) : null;
    const requestHeaders = { ...headers };
    if (data) requestHeaders["Content-Length"] = Buffer.byteLength(data);

    const req = https.request(
      {
        method,
        hostname: url.hostname,
        path: `${url.pathname}${url.search}`,
        headers: requestHeaders,
        family: 4,
      },
      (res) => {
        let raw = "";
        res.on("data", (chunk) => {
          raw += chunk;
        });
        res.on("end", () => {
          let payload = {};
          try {
            payload = JSON.parse(raw || "{}");
          } catch (error) {
            payload = {};
          }
          if (res.statusCode < 200 || res.statusCode >= 300) {
            const detail =
              payload?.detail || payload?.error || payload?.message || JSON.stringify(payload);
            reject(new Error(detail || `HTTP ${res.statusCode}`));
            return;
          }
          resolve(payload);
        });
      }
    );

    req.setTimeout(15000, () => {
      req.destroy(new Error("Request timeout"));
    });
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
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

async function getAgents() {
  return request("GET", "/convai/agents?search=Kora&archived=false");
}

async function upsertAgent(toolId) {
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
          built_in_tools: ["end_call"],
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

  const agents = await getAgents();
  const existing = (agents?.agents || []).find((agent) => agent?.name === "Kora");
  if (existing?.agent_id) {
    const updated = await request("PATCH", `/convai/agents/${existing.agent_id}`, agentConfig);
    return updated?.agent_id || existing.agent_id;
  }

  const created = await request("POST", "/convai/agents/create", agentConfig);
  return created?.agent_id;
}

async function main() {
  const toolId = await upsertTool();
  const agentId = await upsertAgent(toolId);
  const output = { tool_id: toolId, agent_id: agentId, tool_url: toolUrl };
  fs.writeFileSync("kora.agent.json", JSON.stringify(output, null, 2));
  console.log("Kora agent ready:", output);
}

main().catch((error) => {
  console.error("Setup failed:", error.message || error);
  process.exit(1);
});
