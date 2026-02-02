const http = require("http");
const fs = require("fs");
const path = require("path");
const { randomUUID } = require("crypto");

const PORT = process.env.PORT || 8080;
const ROOT = __dirname;
const DEBUG_TOOLS = process.env.DEBUG_TOOLS === "true";
const SCHEMA_TTL_MS = 5 * 60 * 1000;
const SEMANTIC_PATH = path.join(ROOT, "semantic.json");
const MEMORY_PATH = path.join(ROOT, "memory.json");
const PENDING_PATH = path.join(ROOT, "pending.json");
const voiceEventClients = new Set();

const schemaCache = {
  data: null,
  loadedAt: 0,
};

function parseEnvFile() {
  const envPath = path.join(ROOT, ".env");
  const env = { ...process.env };
  try {
    const text = fs.readFileSync(envPath, "utf8");
    const lines = text.split("\n");
    for (const raw of lines) {
      const line = raw.trim();
      if (!line || line.startsWith("#") || !line.includes("=")) continue;
      const [key, ...rest] = line.split("=");
      const value = rest.join("=").trim().replace(/^"|"$/g, "");
      env[key.trim()] = value;
    }
    return env;
  } catch (error) {
    return env;
  }
}

function loadSemanticHints() {
  try {
    const raw = fs.readFileSync(SEMANTIC_PATH, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

async function getSemanticHints(env) {
  const semantic = loadSemanticHints() || {};
  try {
    const schema = await fetchSchema(env);
    const tables = schema.tables || [];
    const snapshot = {
      updated_at: new Date().toISOString(),
      tables,
    };
    semantic.schema_cache = snapshot;
    semantic.schema_snapshot = tables;
    saveJson(SEMANTIC_PATH, semantic);
  } catch (error) {
    // keep existing semantic hints if schema fetch fails
  }
  return semantic;
}

function loadJson(path, fallback) {
  try {
    return JSON.parse(fs.readFileSync(path, "utf8"));
  } catch (error) {
    return fallback;
  }
}

function saveJson(path, data) {
  fs.writeFileSync(path, JSON.stringify(data, null, 2));
}

function normalizePromptKey(text) {
  return normalizePrompt(text).toLowerCase();
}

function getLearnedTemplates() {
  const semantic = loadSemanticHints();
  const templates = semantic?.learned_templates || [];
  return templates.map((t) => {
    if (!t.query_template && t.query) {
      return { ...t, query_template: t.query };
    }
    return t;
  });
}

function simpleHash(input) {
  let hash = 0;
  for (let i = 0; i < input.length; i += 1) {
    hash = (hash << 5) - hash + input.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function slugify(input) {
  return String(input)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 60);
}

function buildIntentKeywords(prompt) {
  const normalized = normalizePromptKey(prompt);
  const keywords = [];
  ["shot", "shots", "conceded", "pass", "passes", "heatmap", "pitch plot", "shot map", "pass map", "goal", "goals"].forEach(
    (key) => {
      if (normalized.includes(key)) keywords.push(key);
    }
  );
  return Array.from(new Set(keywords));
}

function detectMultiTeamPrompt(prompt) {
  const normalized = normalizePrompt(prompt);
  return /(\bvs\b|versus|against|comparing|between)/i.test(normalized);
}

function isDatabaseIntent(prompt) {
  const normalized = normalizePrompt(prompt).toLowerCase();
  if (isVisualizationPrompt(normalized)) return true;
  return /(goal|goals|shot|shots|pass|passes|assist|assists|xg|block|blocks|tackle|tackles|interception|interceptions|foul|fouls|card|cards|corner|corners|match|season|team|player|nickname)/i.test(
    normalized
  );
}

function buildActionPlan(prompt, memory) {
  const cleaned = stripGreetingPreamble(prompt || "");
  const params = extractParamsFromPrompt(cleaned, memory);
  const plan = {
    intent: isVisualizationPrompt(cleaned)
      ? "visual"
      : isDatabaseIntent(cleaned)
      ? "database"
      : "general",
    forceTool: null,
    summary: "",
    clarification: null,
  };

  if (plan.intent === "visual") {
    plan.forceTool = "render_mplsoccer";
  } else if (plan.intent === "database") {
    plan.forceTool = "run_sql_rpc";
  }

  const entities = [];
  if (params.team) entities.push(`team=${params.team}`);
  if (params.team_a && params.team_b) entities.push(`teams=${params.team_a} vs ${params.team_b}`);
  if (params.season) entities.push(`season=${params.season}`);
  if (params.last_n) entities.push(`last_n=${params.last_n}`);
  plan.summary = `intent=${plan.intent}${entities.length ? `; ${entities.join("; ")}` : ""}`;

  if (plan.intent === "visual" && /match/i.test(cleaned)) {
    const hasSeason = /\d{4}\/\d{4}/.test(cleaned);
    const hasMatchId = /match\s*id/i.test(cleaned);
    const hasOpponent = detectMultiTeamPrompt(cleaned);
    if (!hasMatchId && !hasSeason) {
      plan.clarification =
        "Which match should I use? You can provide a season (e.g., 2023/2024), a match ID, or the opponent.";
    } else if (!hasMatchId && !hasOpponent) {
      plan.clarification =
        "Which opponent or match ID should I use for the match request?";
    }
  }

  return plan;
}

function isLearnedTemplateCompatible(prompt, template, memory) {
  if (!template) return false;
  if (!detectMultiTeamPrompt(prompt || "")) return true;
  const queryText = (template.query_template || template.query || "").toLowerCase();
  const params = extractParamsFromPrompt(prompt || "", memory || {});
  if (queryText.includes("{{team_a}}") || queryText.includes("{{team_b}}")) return true;
  if (params.team_a && params.team_b) {
    const a = String(params.team_a).toLowerCase();
    const b = String(params.team_b).toLowerCase();
    return queryText.includes(a) && queryText.includes(b);
  }
  return false;
}

function ensureSelectIncludes(query, column) {
  if (!query || !column) return query;
  const lower = query.toLowerCase();
  if (lower.includes(column.toLowerCase())) return query;
  const selectRegex = /select\\s+(distinct\\s+)?/i;
  let targetIndex = lower.indexOf("select");
  if (lower.startsWith("with")) {
    const lastSelect = lower.lastIndexOf("select");
    if (lastSelect !== -1) {
      targetIndex = lastSelect;
    }
  }
  if (targetIndex === -1) return query;
  const match = query.slice(targetIndex).match(selectRegex);
  if (!match) return query;
  const insertPos = targetIndex + match[0].length;
  return query.slice(0, insertPos) + `${column}, ` + query.slice(insertPos);
}

function ensureTeamNameInQuery(query) {
  const lower = (query || "").toLowerCase();
  if (!lower.includes("team_name")) {
    if (lower.includes("from viz_match_events_with_match") || lower.includes("from viz_match_events")) {
      return ensureSelectIncludes(query, "team_name");
    }
  }
  return query;
}

function ensureSeasonNameQuery(query) {
  if (!query) return query;
  if (!/season_name/i.test(query)) return query;
  if (/from\\s+viz_match_events\\b/i.test(query)) {
    return query.replace(/from\\s+viz_match_events\\b/i, "from viz_match_events_with_match");
  }
  return query;
}

function extractVisualOverrides(prompt) {
  const normalized = normalizePrompt(prompt);
  const markerRules = [];
  const highlightRules = [];
  const markerMap = {
    square: "s",
    squares: "s",
    triangle: "^",
    triangles: "^",
    circle: "o",
    circles: "o",
    diamond: "D",
    diamonds: "D",
  };
  const colorMap = {
    red: "#ff3b30",
    blue: "#003C71",
    navy: "#003C71",
    yellow: "#FFD000",
    teal: "#2E7D6D",
    slate: "#1F2E3D",
    green: "#0B6623",
    white: "#FFFFFF",
    gray: "#ECECEC",
    grey: "#ECECEC",
    black: "#000000",
  };

  const shotMatch = normalized.match(/shots? in (squares|square|triangles|triangle|circles|circle|diamonds|diamond)/i);
  if (shotMatch) {
    markerRules.push({ target: "shot", marker: markerMap[shotMatch[1].toLowerCase()] });
  }
  const passMatch = normalized.match(/passes? in (squares|square|triangles|triangle|circles|circle|diamonds|diamond)/i);
  if (passMatch) {
    markerRules.push({ target: "pass", marker: markerMap[passMatch[1].toLowerCase()] });
  }
  const penaltyMatch =
    normalized.match(/(inside|within) (?:the )?penalty area.*?(red|blue|navy|yellow|teal|slate|green|white|gray|grey|black)/i) ||
    normalized.match(/penalty area.*?(red|blue|navy|yellow|teal|slate|green|white|gray|grey|black)/i);
  if (penaltyMatch) {
    const colorKey = penaltyMatch[2] ? penaltyMatch[2].toLowerCase() : penaltyMatch[1]?.toLowerCase();
    const color = colorMap[colorKey] || colorKey;
    if (color) {
      highlightRules.push({ type: "penalty_area", color });
    }
  }

  return {
    marker_rules: markerRules.length ? markerRules : undefined,
    highlight_rules: highlightRules.length ? highlightRules : undefined,
  };
}

function applyVisualOverrides(params, prompt) {
  if (!prompt) return params;
  const overrides = extractVisualOverrides(prompt);
  if (!params.marker_rules && overrides.marker_rules) {
    params.marker_rules = overrides.marker_rules;
  }
  if (!params.highlight_rules && overrides.highlight_rules) {
    params.highlight_rules = overrides.highlight_rules;
  }
  return params;
}

function cleanEntityName(value) {
  let text = String(value || "").trim();
  text = text.replace(/^(show me|show|compare|comparing|between|give me|display|plot|draw|visualize)\\s+/i, "");
  text = stripTeamPrefix(text);
  text = text.replace(/^(the\\s+)?team\\s+/i, "");
  text = text.replace(/^(the\\s+)?player\\s+/i, "");
  return text.trim();
}

function extractParamsFromPrompt(prompt, memory) {
  const params = {};
  const normalized = normalizePrompt(prompt);
  const lastMatch = normalized.match(/last\\s+(\\d+)\\s+matches?/i);
  if (lastMatch) params.last_n = Number(lastMatch[1]);
  const seasonMatch = normalized.match(/(\\d{4}\/\\d{4})/);
  if (seasonMatch) params.season = seasonMatch[1];
  const compareMatch =
    normalized.match(/(?:between|comparing) (.+?) and (.+?)$/i) ||
    normalized.match(/(.+?)\\s+(?:vs\\.?|versus|against)\\s+(.+)$/i);
  if (compareMatch) {
    params.team_a = cleanEntityName(compareMatch[1]);
    params.team_b = cleanEntityName(compareMatch[2]);
  }
  const teamFromMemory = findKnownTeam(normalized, memory);
  if (teamFromMemory) params.team = teamFromMemory;
  const concededMatch =
    normalized.match(/shots? (?:that )?(.+?) conceded/i) ||
    normalized.match(/conceded shots? (?:by|for) (.+?)(?:\\s+in|\\s+last|$)/i);
  if (!params.team && concededMatch) params.team = concededMatch[1].trim();
  return params;
}

function parseBlockedShotsPlayersPrompt(text) {
  const normalized = normalizePrompt(text);
  if (!/(block|blocked|blocks)/i.test(normalized)) return null;
  if (!/shot/i.test(normalized)) return null;
  if (!/(who|which players|players)/i.test(normalized)) return null;
  return { type: "players_blocked_shots" };
}

function buildQueryTemplate(query, params) {
  if (!query) return null;
  let template = query;
  if (params.team_a) {
    const escaped = String(params.team_a).replace(/'/g, "''");
    const regex = new RegExp(escaped.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&"), "gi");
    template = template.replace(regex, "{{team_a}}");
  }
  if (params.team_b) {
    const escaped = String(params.team_b).replace(/'/g, "''");
    const regex = new RegExp(escaped.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&"), "gi");
    template = template.replace(regex, "{{team_b}}");
  }
  if (params.team) {
    const escaped = String(params.team).replace(/'/g, "''");
    const regex = new RegExp(escaped.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&"), "gi");
    template = template.replace(regex, "{{team}}");
  }
  if (params.season) {
    const escaped = String(params.season).replace(/'/g, "''");
    const regex = new RegExp(escaped.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&"), "gi");
    template = template.replace(regex, "{{season}}");
  }
  if (params.last_n) {
    const regex = new RegExp(`\\b${params.last_n}\\b`, "g");
    template = template.replace(regex, "{{last_n}}");
  }
  return template;
}

function fillQueryTemplate(template, params) {
  let query = template;
  if (params.team_a) {
    const escaped = String(params.team_a).replace(/'/g, "''");
    query = query.replace(/\\{\\{team_a\\}\\}/gi, escaped);
  }
  if (params.team_b) {
    const escaped = String(params.team_b).replace(/'/g, "''");
    query = query.replace(/\\{\\{team_b\\}\\}/gi, escaped);
  }
  if (params.team) {
    const escaped = String(params.team).replace(/'/g, "''");
    query = query.replace(/\\{\\{team\\}\\}/gi, escaped);
  }
  if (params.season) {
    const escaped = String(params.season).replace(/'/g, "''");
    query = query.replace(/\\{\\{season\\}\\}/gi, escaped);
  }
  if (params.last_n != null) {
    query = query.replace(/\\{\\{last_n\\}\\}/gi, String(params.last_n));
  }
  return query;
}

function insertClauseBeforeLimit(query, clause) {
  if (!/\\blimit\\b/i.test(query)) {
    return `${query} ${clause}`;
  }
  return query.replace(/\\blimit\\b[\\s\\S]*$/i, (match) => `${clause} ${match}`);
}

function ensureConcededQuery(query, team) {
  if (!query || !team) return query;
  const lower = query.toLowerCase();
  if (lower.includes("team_name not") && (lower.includes("home_team_name") || lower.includes("away_team_name"))) {
    return query;
  }
  const safeTeam = String(team).replace(/'/g, "''");
  const clause =
    "and team_name is not null and team_name not ilike '%" +
    safeTeam +
    "%' and (home_team_name ilike '%" +
    safeTeam +
    "%' or away_team_name ilike '%" +
    safeTeam +
    "%')";
  if (/\\bwhere\\b/i.test(query)) {
    return insertClauseBeforeLimit(query, clause);
  }
  return insertClauseBeforeLimit(query, `where ${clause.replace(/^and\\s+/i, "")}`);
}

function addLearnedTemplate({ chartType, query, sourcePrompt, memory }) {
  if (!chartType || !query || !sourcePrompt) return;
  const semantic = loadSemanticHints() || {};
  semantic.learned_templates = semantic.learned_templates || [];
  const params = extractParamsFromPrompt(sourcePrompt, memory || {});
  const queryTemplate = buildQueryTemplate(query, params);
  if (detectMultiTeamPrompt(sourcePrompt || "") && queryTemplate) {
    if (!queryTemplate.includes("{{team_a}}") || !queryTemplate.includes("{{team_b}}")) {
      return;
    }
  }
  const keyBase = `${chartType}:${normalizePromptKey(sourcePrompt)}:${queryTemplate || query}`;
  const name = `learned_${chartType}_${slugify(sourcePrompt)}_${simpleHash(keyBase)}`;
  if (semantic.learned_templates.some((t) => t.name === name)) return;
  semantic.learned_templates.push({
    name,
    chart_type: chartType,
    query_template: queryTemplate || query,
    params: Object.keys(params),
    intent_keywords: buildIntentKeywords(sourcePrompt),
    source_prompt: normalizePrompt(sourcePrompt)
  });
  saveJson(SEMANTIC_PATH, semantic);
}

function maybeLearnTemplateFromArgs(args, prompt, memory) {
  if (!args?.chart_type || !prompt) return;
  let effectiveQuery = args.query || null;
  if (!effectiveQuery && args.template) {
    effectiveQuery = buildTemplateQuery(args.template, args.template_vars || {});
  }
  if (!effectiveQuery) return;
  addLearnedTemplate({
    chartType: args.chart_type,
    query: effectiveQuery,
    sourcePrompt: prompt,
    memory
  });
}

async function renderMplSoccerAndLearn(params, env, prompt, memory) {
  params.prompt_text = prompt;
  applyVisualOverrides(params, prompt);
  if (!params.series_split_field && detectMultiTeamPrompt(prompt || "")) {
    params.series_split_field = "team_name";
  }
  if (params.series_split_field === "team_name" && params.query) {
    params.query = ensureTeamNameInQuery(params.query);
  }
  params.query = enforceLastNLimit(params.query, prompt, params.series_split_field);
  const image = await renderMplSoccer(params, env);
  if (image?.image_base64) {
    maybeLearnTemplateFromArgs(params, prompt, memory);
    const analysis = await analyzeVisualization(prompt, image, env.OPENROUTER_API_KEY);
    if (analysis) {
      image.analysis_text = analysis;
    }
  }
  return image;
}

function enforceLastNLimit(query, prompt, seriesField) {
  if (!query || !prompt) return query;
  const normalized = normalizePrompt(prompt);
  const lastMatch = normalized.match(/last\s+(\d+)\s+(shots?|passes?|events?|matches?)/i);
  if (!lastMatch) return query;
  const lastN = Number(lastMatch[1]);
  if (!lastN || lastN <= 0) return query;
  const lower = query.toLowerCase();
  if (lower.includes(" limit ")) return query;
  if (/(row_number\s*\()/.test(lower)) return query;

  const partitionField = seriesField === "player_name" ? "player_name" : "team_name";
  if (detectMultiTeamPrompt(prompt)) {
    let limitedQuery = query;
    if (!lower.includes(partitionField)) {
      limitedQuery = ensureTeamNameInQuery(limitedQuery);
    }
    if (!lower.includes("date_time")) {
      limitedQuery = ensureSelectIncludes(limitedQuery, "date_time");
    }
    const limitedLower = limitedQuery.toLowerCase();
    if (limitedLower.includes(partitionField) && limitedLower.includes("date_time")) {
      return (
        "with ranked as (" +
        limitedQuery +
        ") select * from (" +
        "select *, row_number() over (partition by " +
        partitionField +
        " order by date_time desc) as rn from ranked" +
        ") as limited where rn <= " +
        lastN
      );
    }
  }
  return query + " limit " + lastN;
}

function getMemory() {
  return loadJson(MEMORY_PATH, { aliases: {}, scopes: {} });
}

function saveMemory(memory) {
  saveJson(MEMORY_PATH, memory);
}

function rememberLastPassMap(memory, payload) {
  if (!payload?.team || !payload?.match_id) return;
  const updated = { ...(memory || {}) };
  updated.scopes = updated.scopes || {};
  updated.scopes.last_pass_map = {
    team: payload.team,
    match_id: payload.match_id,
    saved_at: new Date().toISOString(),
  };
  saveMemory(updated);
}

function getLastPassMap(memory) {
  return memory?.scopes?.last_pass_map || null;
}

function getLastMatchContext(memory) {
  return memory?.scopes?.last_match || null;
}

function setLastMatchContext(memory, payload) {
  if (!payload?.match_id) return;
  const updated = { ...memory };
  updated.scopes = updated.scopes || {};
  updated.scopes.last_match = {
    match_id: payload.match_id,
    teams: payload.teams || null,
  };
  saveMemory(updated);
}

function getLastTeams(memory) {
  return memory?.scopes?.last_teams || null;
}

function setLastTeams(memory, teamA, teamB) {
  if (!teamA || !teamB) return;
  const updated = { ...memory };
  updated.scopes = updated.scopes || {};
  updated.scopes.last_teams = {
    team_a: teamA,
    team_b: teamB,
  };
  saveMemory(updated);
}

function getPending() {
  return loadJson(PENDING_PATH, null);
}

function savePending(pending) {
  if (!pending) {
    try {
      fs.unlinkSync(PENDING_PATH);
    } catch (error) {
      // ignore
    }
    return;
  }
  saveJson(PENDING_PATH, pending);
}

function broadcastVoiceEvent(payload) {
  const data = `data: ${JSON.stringify(payload)}\n\n`;
  for (const client of voiceEventClients) {
    try {
      client.write(data);
    } catch (error) {
      // ignore broken clients
    }
  }
}

function handleVoiceEvents(req, res) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });
  res.write("\n");
  voiceEventClients.add(res);
  req.on("close", () => {
    voiceEventClients.delete(res);
  });
}

function sendJson(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function sendAssistantReply(res, content, image) {
  let finalContent = content;
  let cleanedImage = image;
  if (image && typeof image === "object") {
    const { analysis_text, data_preview, ...rest } = image;
    cleanedImage = rest;
    if (analysis_text) {
      finalContent = `${content}\n\nAnalysis:\n${analysis_text}`;
    }
  }
  sendJson(res, 200, {
    choices: [
      {
        message: {
          role: "assistant",
          content: finalContent,
        },
      },
    ],
    image: cleanedImage,
  });
}

function buildTemplateQuery(template, vars) {
  if (!template) return null;
  const learned = getLearnedTemplates();
  const learnedMatch = learned.find((t) => t.name === template);
  if (learnedMatch?.query_template) {
    const filled = fillQueryTemplate(learnedMatch.query_template, vars || {});
    return filled;
  }
  const v = vars || {};
  const teamA = String(v.team_a || "").replace(/'/g, "''");
  const teamB = String(v.team_b || "").replace(/'/g, "''");
  const teamFilter =
    teamA && teamB
      ? `(team_name ilike '%${teamA}%' or team_name ilike '%${teamB}%')`
      : teamA
      ? `team_name ilike '%${teamA}%'`
      : teamB
      ? `team_name ilike '%${teamB}%'`
      : "team_name is not null";
  if (template === "shots_by_team") {
    return (
      "select team_name, x, y from viz_match_events_with_match " +
      "where event_name in ('Shoot','Shoot Location','Penalty') and " +
      teamFilter
    );
  }
  if (template === "passes_success_by_team") {
    return (
      "select team_name, x, y from viz_match_events_with_match " +
      "where event_name = 'Pass' and result_name = 'Success' and " +
      teamFilter
    );
  }
  if (template === "heatmap_events_by_team") {
    return (
      "select team_name, x, y from viz_match_events_with_match " +
      "where " +
      teamFilter
    );
  }
  if (template === "shot_map_by_player") {
    return (
      "select player_name, x, y from viz_match_events_with_match " +
      "where event_name in ('Shoot','Shoot Location','Penalty') and player_name ilike '%" +
      String(v.player || "").replace(/'/g, "''") +
      "%'"
    );
  }
  if (template === "shots_conceded_by_team") {
    return (
      "select x, y, team_name from viz_match_events_with_match " +
      "where event_name in ('Shoot','Shoot Location','Penalty') " +
      "and team_name is not null " +
      "and team_name not ilike '%" +
      String(v.team || "").replace(/'/g, "''") +
      "%' " +
      "and (home_team_name ilike '%" +
      String(v.team || "").replace(/'/g, "''") +
      "%' or away_team_name ilike '%" +
      String(v.team || "").replace(/'/g, "''") +
      "%')"
    );
  }
  if (template === "shots_conceded_last_n_matches") {
    const lastN = Number(v.last_n || 5);
    return (
      "with recent as (" +
      "select m.id from matches m " +
      "join teams th on m.home_team_id = th.id " +
      "join teams ta on m.away_team_id = ta.id " +
      "where th.name ilike '%" +
      String(v.team || "").replace(/'/g, "''") +
      "%' or ta.name ilike '%" +
      String(v.team || "").replace(/'/g, "''") +
      "%' " +
      "order by m.date_time desc limit " +
      lastN +
      ") " +
      "select e.x, e.y, e.team_name from viz_match_events_with_match e " +
      "where e.match_id in (select id from recent) " +
      "and e.event_name in ('Shoot','Shoot Location','Penalty') " +
      "and e.team_name is not null " +
      "and e.team_name not ilike '%" +
      String(v.team || "").replace(/'/g, "''") +
      "%'"
    );
  }
  if (template === "pass_map_by_player") {
    return (
      "select player_name, x, y from viz_match_events_with_match " +
      "where event_name = 'Pass' and result_name = 'Success' and player_name ilike '%" +
      String(v.player || "").replace(/'/g, "''") +
      "%'"
    );
  }
  if (template === "pass_network_by_team") {
    return null;
  }
  if (template === "heatmap_by_player") {
    return (
      "select player_name, x, y from viz_match_events_with_match " +
      "where player_name ilike '%" +
      String(v.player || "").replace(/'/g, "''") +
      "%'"
    );
  }
  if (template === "metrics_player_summary") {
    return (
      "select '" +
      String(v.player || "").replace(/'/g, "''") +
      "' as player, " +
      "sum(case when result_name = 'Goal' then 1 else 0 end) as goals, " +
      "sum(case when event_name in ('Shoot','Shoot Location','Penalty') then 1 else 0 end) as shots, " +
      "sum(case when event_name = 'Pass' and result_name = 'Success' then 1 else 0 end) as passes, " +
      "sum(case when extra_name = 'Assist' then 1 else 0 end) as assists " +
      "from viz_match_events_with_match where player_name ilike '%" +
      String(v.player || "").replace(/'/g, "''") +
      "%'"
    );
  }
  if (template === "metrics_two_players") {
    return (
      "select player_name, " +
      "sum(case when result_name = 'Goal' then 1 else 0 end) as goals, " +
      "sum(case when event_name in ('Shoot','Shoot Location','Penalty') then 1 else 0 end) as shots, " +
      "sum(case when event_name = 'Pass' and result_name = 'Success' then 1 else 0 end) as passes, " +
      "sum(case when extra_name = 'Assist' then 1 else 0 end) as assists " +
      "from viz_match_events_with_match " +
      "where player_name ilike '%" +
      String(v.player_a || "").replace(/'/g, "''") +
      "%' or player_name ilike '%" +
      String(v.player_b || "").replace(/'/g, "''") +
      "%' group by player_name"
    );
  }
  if (template === "bumpy_top_scorers_by_season") {
    const topN = Number(v.top_n || 5);
    const seasonsN = Number(v.seasons_n || 3);
    return (
      "with ranked as (" +
      "select season_name, player_name, sum(case when result_name='Goal' then 1 else 0 end) as goals " +
      "from viz_match_events_with_match " +
      "where season_name is not null " +
      "group by season_name, player_name" +
      "), seasons as (" +
      "select distinct season_name from ranked order by season_name desc limit " +
      seasonsN +
      "), filtered as (" +
      "select r.* from ranked r join seasons s on r.season_name = s.season_name" +
      "), ranked2 as (" +
      "select season_name as metric, player_name as series_label, " +
      "dense_rank() over (partition by season_name order by goals desc) as value " +
      "from filtered where goals > 0" +
      ") select * from ranked2 where value <= " +
      topN
    );
  }
  return null;
}

function normalizeChartData(chartType, data, params) {
  const rows = Array.isArray(data) ? data : [];
  if (["radar", "pizza"].includes(chartType)) {
    if (params.metrics && params.values) {
      return { metrics: params.metrics, values: params.values, values_compare: params.values_compare };
    }
    if (params.template === "metrics_player_summary") {
      const row = rows[0] || {};
      const metrics = ["Goals", "Shots", "Passes", "Assists"];
      const values = [
        Number(row.goals || 0),
        Number(row.shots || 0),
        Number(row.passes || 0),
        Number(row.assists || 0),
      ];
      return { metrics, values };
    }
    if (params.template === "metrics_two_players") {
      const metrics = ["Goals", "Shots", "Passes", "Assists"];
      const byPlayer = {};
      rows.forEach((row) => {
        byPlayer[row.player_name] = [
          Number(row.goals || 0),
          Number(row.shots || 0),
          Number(row.passes || 0),
          Number(row.assists || 0),
        ];
      });
      const names = Object.keys(byPlayer);
      if (names.length >= 2) {
        return {
          metrics,
          values: byPlayer[names[0]],
          values_compare: byPlayer[names[1]],
        };
      }
    }
    const metrics = [];
    const values = [];
    const valuesCompare = [];
    rows.forEach((row) => {
      if (row.metric != null && row.value != null) {
        metrics.push(row.metric);
        values.push(Number(row.value));
        if (row.value_compare != null) {
          valuesCompare.push(Number(row.value_compare));
        }
      }
    });
    if (!metrics.length || !values.length) {
      throw new Error("Radar/Pizza needs metrics[] and values[] or rows with metric/value.");
    }
    return {
      metrics,
      values,
      values_compare: valuesCompare.length ? valuesCompare : null,
    };
  }

  if (chartType === "bumpy") {
    if (params.series && params.metrics) {
      return { metrics: params.metrics, series: params.series };
    }
    if (params.template === "bumpy_top_scorers_by_season") {
      const metricsSet = new Set();
      const seriesMap = {};
      rows.forEach((row) => {
        const metric = row.metric;
        const label = row.series_label;
        const value = row.value;
        if (metric == null || label == null || value == null) return;
        metricsSet.add(metric);
        if (!seriesMap[label]) seriesMap[label] = [];
        seriesMap[label].push({ metric, value: Number(value) });
      });
      const metrics = Array.from(metricsSet);
      const series = Object.entries(seriesMap).map(([label, values]) => {
        const ordered = metrics.map((m) => {
          const entry = values.find((v) => v.metric === m);
          return entry ? entry.value : null;
        });
        return { label, values: ordered };
      });
      return { metrics, series };
    }
    const metricsSet = new Set();
    const seriesMap = {};
    rows.forEach((row) => {
      const metric = row.metric;
      const label = row.series_label || row.label || row.team_name || "Series";
      const value = row.value;
      if (metric == null || value == null) return;
      metricsSet.add(metric);
      if (!seriesMap[label]) seriesMap[label] = [];
      seriesMap[label].push({ metric, value: Number(value) });
    });
    const metrics = Array.from(metricsSet);
    const series = Object.entries(seriesMap).map(([label, values]) => {
      const ordered = metrics.map((m) => {
        const entry = values.find((v) => v.metric === m);
        return entry ? entry.value : null;
      });
      return { label, values: ordered };
    });
    if (!metrics.length || !series.length) {
      throw new Error("Bumpy needs metrics[] and series[] or rows with metric/series_label/value.");
    }
    return { metrics, series };
  }

  const seriesField = params.series_split_field || "team_name";
  if (params.series && Array.isArray(params.series)) {
    return { series: params.series };
  }
  if (rows.some((row) => row[seriesField] != null)) {
    const grouped = {};
    rows.forEach((row) => {
      const key = row[seriesField];
      if (!grouped[key]) grouped[key] = [];
      grouped[key].push(row);
    });
    const series = Object.entries(grouped).map(([label, dataRows]) => ({
      label,
      data: dataRows,
    }));
    return { series };
  }

  return { data: rows };
}

function getLastUserMessage(messages) {
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    if (messages[i]?.role === "user") return messages[i].content || "";
  }
  return "";
}

function getLastUserMessageIndex(messages) {
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    if (messages[i]?.role === "user") return i;
  }
  return -1;
}

function normalizePrompt(text) {
  return (text || "").trim().replace(/\s+/g, " ").replace(/[?.!]+$/, "");
}

function normalizePrompt(text) {
  return (text || "").trim().replace(/\s+/g, " ").replace(/[?.!]+$/, "");
}

function stripGreetingPreamble(text) {
  if (!text) return "";
  const parts = String(text).split(/[?!.]/).map((p) => p.trim()).filter(Boolean);
  while (parts.length) {
    const head = parts[0].toLowerCase();
    if (
      /^(hi|hello|hey|how are you|how's it going|good morning|good afternoon|good evening|greetings|hi cora|hi kora)$/i.test(
        head
      )
    ) {
      parts.shift();
      continue;
    }
    break;
  }
  return parts.join(". ").trim() || String(text).trim();
}

function extractEntityCandidate(text) {
  const normalized = normalizePrompt(text);
  if (/color|different color|visual|chart|plot|heatmap|shot map|pass map/i.test(normalized)) {
    return null;
  }
  if (/(did he|did she|did they|did him|did her|did them|how many errors|how many blocks|how many|errors did he|blocks did he)/i.test(normalized)) {
    return null;
  }
  const forMatch = normalized.match(/(?:for|of) ([^,]+)$/i);
  if (forMatch) {
    const raw = forMatch[1].trim().replace(/\)$/, "");
    return raw.split(" and ")[0].trim();
  }
  const betweenMatch = normalized.match(/between (.+?) and (.+?)$/i);
  if (betweenMatch) return null;
  const namedMatch = normalized.match(/(?:player|team) ([^,]+)$/i);
  if (namedMatch) return namedMatch[1].trim();
  const nameMatch = normalized.match(/([A-Z][a-z]+\\s+[A-Z][a-z]+(?:\\s+[A-Z][a-z]+)?)/);
  if (nameMatch) return nameMatch[1].trim();
  return null;
}

function buildAliasRegex(key) {
  const escaped = escapeRegex(key);
  const withSpaces = escaped.replace(/\\s+/g, "\\\\s*");
  return new RegExp(`\\b${withSpaces}\\b`, "i");
}

function fuzzyLikePattern(value) {
  const compact = String(value).replace(/\\s+/g, "");
  if (!compact) return "";
  return compact.split("").join("%");
}

function extractCandidatePhrases(text) {
  const normalized = String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9\\s]/g, " ")
    .replace(/\\s+/g, " ")
    .trim();
  if (!normalized) return [];
  const tokens = normalized.split(" ").filter((t) => t.length >= 3);
  const stop = new Set([
    "match",
    "season",
    "shots",
    "shot",
    "passes",
    "pass",
    "blocks",
    "block",
    "goals",
    "goal",
    "assistant",
    "kora",
    "cora",
    "please",
    "show",
    "all",
    "from",
    "versus",
    "against",
    "between",
    "compare",
  ]);
  const phrases = new Set();
  for (let i = 0; i < tokens.length; i += 1) {
    if (!stop.has(tokens[i])) phrases.add(tokens[i]);
    if (i + 1 < tokens.length) {
      const bigram = `${tokens[i]} ${tokens[i + 1]}`;
      if (!stop.has(tokens[i]) && !stop.has(tokens[i + 1])) phrases.add(bigram);
    }
    if (i + 2 < tokens.length) {
      const trigram = `${tokens[i]} ${tokens[i + 1]} ${tokens[i + 2]}`;
      if (
        !stop.has(tokens[i]) &&
        !stop.has(tokens[i + 1]) &&
        !stop.has(tokens[i + 2])
      ) {
        phrases.add(trigram);
      }
    }
  }
  return Array.from(phrases).slice(0, 6);
}

async function findTeamMatch(env, candidate) {
  if (!candidate) return null;
  const safe = candidate.replace(/'/g, "''");
  const fuzzy = fuzzyLikePattern(candidate);
  const query =
    "select name from teams " +
    "where name ilike '%" +
    safe +
    "%' or name ilike '%" +
    fuzzy +
    "%' " +
    "order by length(name) desc limit 1";
  const result = await runSqlRpc(query, env);
  return result.data?.[0]?.name || null;
}

async function findPlayerMatch(env, candidate) {
  if (!candidate) return null;
  const safe = candidate.replace(/'/g, "''");
  const fuzzy = fuzzyLikePattern(candidate);
  const query =
    "select player_name, player_nickname from players " +
    "where player_name ilike '%" +
    safe +
    "%' or player_nickname ilike '%" +
    safe +
    "%' or player_name ilike '%" +
    fuzzy +
    "%' or player_nickname ilike '%" +
    fuzzy +
    "%' " +
    "limit 1";
  const result = await runSqlRpc(query, env);
  return result.data?.[0] || null;
}

async function resolveTranscriptEntities(text, env, memory) {
  let updated = String(text || "");
  const aliases = memory?.aliases || {};
  Object.entries(aliases).forEach(([key, value]) => {
    const re = buildAliasRegex(key);
    if (re.test(updated)) {
      updated = updated.replace(re, value.value || key);
    }
  });

  const candidates = extractCandidatePhrases(updated);
  for (const candidate of candidates) {
    const team = await findTeamMatch(env, candidate);
    if (team) {
      const re = buildAliasRegex(candidate);
      updated = updated.replace(re, team);
      if (!aliases[candidate]) {
        aliases[candidate] = { type: "team_name", value: team };
      }
      continue;
    }
    const player = await findPlayerMatch(env, candidate);
    if (player?.player_name) {
      const re = buildAliasRegex(candidate);
      updated = updated.replace(re, player.player_name);
      if (!aliases[candidate]) {
        aliases[candidate] = { type: "player_name", value: player.player_name };
      }
    } else if (player?.player_nickname) {
      const re = buildAliasRegex(candidate);
      updated = updated.replace(re, player.player_nickname);
      if (!aliases[candidate]) {
        aliases[candidate] = { type: "player_nickname", value: player.player_nickname };
      }
    }
  }

  const updatedMemory = { ...memory, aliases };
  saveMemory(updatedMemory);
  return updated;
}
function seasonLikePattern(value) {
  const cleaned = String(value || "").replace(/[^0-9]/g, "");
  if (!cleaned) return "";
  const first = cleaned.slice(0, 4);
  const second = cleaned.slice(4);
  if (second) return `%${first}%${second}%`;
  return `%${first}%`;
}

function resolveAlias(text, memory) {
  const aliases = memory.aliases || {};
  let resolved = text;
  Object.entries(aliases).forEach(([key, value]) => {
    const re = buildAliasRegex(key);
    if (re.test(resolved)) {
      resolved = resolved.replace(re, (match, offset) => {
        const fullValue = String(value.value || "");
        const slice = resolved.slice(offset, offset + fullValue.length);
        if (slice.toLowerCase() === fullValue.toLowerCase()) {
          return match;
        }
        if (value.type === "team_name") {
          return fullValue;
        }
        if (value.type === "player_name") {
          return fullValue;
        }
        if (value.type === "player_nickname") {
          return fullValue;
        }
        return match;
      });
    }
  });
  return resolved;
}

function findKnownTeam(text, memory) {
  const aliases = memory.aliases || {};
  for (const [key, value] of Object.entries(aliases)) {
    if (value.type !== "team_name") continue;
    const reKey = buildAliasRegex(key);
    if (reKey.test(text)) return value.value || key;
    const reValue = buildAliasRegex(value.value || "");
    if (reValue.test(text)) return value.value || key;
  }
  return null;
}

function hasKnownAlias(text, memory) {
  const aliases = memory.aliases || {};
  return Object.keys(aliases).some((key) => buildAliasRegex(key).test(text));
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function looksLikeNewQuestion(text) {
  const normalized = normalizePrompt(text).toLowerCase();
  return /(show|compare|shot map|pass map|heatmap|pitch plot|pass network|radar|pizza|bumpy|draw|visualize|plot|how many|what|which|who|when|where)/.test(
    normalized
  );
}

function handlePendingClarification(lastQuestion) {
  const pending = getPending();
  if (!pending) return null;
  const answer = normalizePrompt(lastQuestion);
  if (!answer) return null;
  if (looksLikeNewQuestion(answer)) {
    savePending(null);
    return null;
  }

  const memory = getMemory();
  if (pending.kind === "alias") {
    const key = pending.key;
    const cleanKey = key.split(" and ")[0].trim();
    if (/team/i.test(answer)) {
      memory.aliases[cleanKey] = { type: "team_name", value: cleanKey };
    } else if (/nickname|nick/i.test(answer)) {
      memory.aliases[cleanKey] = { type: "player_nickname", value: cleanKey };
    } else if (/player|name/i.test(answer)) {
      memory.aliases[cleanKey] = { type: "player_name", value: cleanKey };
    } else {
      memory.aliases[cleanKey] = { type: "player_name", value: answer };
    }
    saveMemory(memory);
    savePending(null);
    return { resolved: resolveAlias(pending.original, memory) };
  }

  if (pending.kind === "scope") {
    const scope = memory.scopes || {};
    scope[pending.scopeKey] = scope[pending.scopeKey] || {};
    if (/season/i.test(pending.asking)) {
      scope[pending.scopeKey].season = answer;
    } else if (/opponent|home|away/i.test(pending.asking)) {
      scope[pending.scopeKey].opponent = answer;
    }
    memory.scopes = scope;
    saveMemory(memory);

    if (pending.remaining > 1) {
      const next = {
        kind: "scope",
        scopeKey: pending.scopeKey,
        remaining: pending.remaining - 1,
        asking: "Any opponent or home/away filter?"
      };
      savePending(next);
      return {
        question: "Any opponent or home/away filter? (e.g., vs Pyramids, home, away)"
      };
    }
    savePending(null);
    return { resolved: pending.original };
  }

  return null;
}


function getLastEntity(memory) {
  return memory.scopes?.last_entity || null;
}

function setLastEntity(memory, value) {
  memory.scopes = memory.scopes || {};
  memory.scopes.last_entity = value;
  saveMemory(memory);
}

function parseShotMapComparePrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/show a shot map comparing (.+?) and (.+?)$/i);
  if (!match) return null;
  return { team_a: match[1].trim(), team_b: match[2].trim() };
}

function parseHeatmapComparePrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/show a heatmap comparing (.+?) and (.+?)$/i);
  if (!match) return null;
  return { team_a: match[1].trim(), team_b: match[2].trim() };
}

function parsePassMapComparePrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/show a pass map comparing (.+?) and (.+?)$/i);
  if (!match) return null;
  return { team_a: match[1].trim(), team_b: match[2].trim() };
}

function stripTeamPrefix(text) {
  return String(text || "")
    .replace(/^(the\\s+)?last\\s+\\d+\\s+shots?\\s+(taken\\s+by|by|for)\\s+/i, "")
    .replace(/^(the\\s+)?shots?\\s+(taken\\s+by|by|for)\\s+/i, "")
    .replace(/^the\\s+/i, "")
    .trim();
}

function parseLastShotsComparePrompt(text) {
  const normalized = normalizePrompt(text);
  const limitMatch = normalized.match(/last\\s+(\\d+)\\s+shots?/i);
  if (!limitMatch) return null;
  const limit = Number(limitMatch[1]);
  const vsMatch = normalized.match(/(.+?)\\s+(?:vs\\b|versus|against)\\s+(.+)$/i);
  if (!vsMatch) return null;
  const left = cleanEntityName(vsMatch[1]);
  const right = cleanEntityName(vsMatch[2]);
  if (!left || !right) return null;
  return { team_a: left, team_b: right, limit };
}

function parseShotMapPlayerPrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/shot map (?:for|of) (.+?)$/i);
  if (!match) return null;
  return { player: match[1].trim() };
}

function parsePassMapPlayerPrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/pass map (?:for|of) (.+?)$/i);
  if (!match) return null;
  return { player: match[1].trim() };
}

function parseHeatmapPlayerPrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/heatmap (?:for|of) (.+?)$/i);
  if (!match) return null;
  return { player: match[1].trim() };
}

function parseRandomMatchPassesPrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/random match for (.+?) and draw (?:me )?all of their successful passes/i);
  if (!match) return null;
  return { team: match[1].trim() };
}

function parseRandomMatchShotsPrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/random match for (.+?) and draw (?:me )?all of their shots/i);
  if (!match) return null;
  return { team: match[1].trim() };
}

function parseRandomShotMapPrompt(text) {
  const normalized = normalizePrompt(text);
  if (!/shot map/i.test(normalized)) return null;
  if (!/random match/i.test(normalized)) return null;
  const teamMatch = normalized.match(/shot map(?:\s+for|\s+of)?\s+(.+?)\s+in\s+(?:a\s+)?random match/i);
  const seasonMatch = normalized.match(/(?:season|from)\s+(\d{4}\/\d{4})/i);
  if (!teamMatch) return null;
  return { team: teamMatch[1].trim(), season: seasonMatch?.[1]?.trim() || null };
}

function parseCarryMapPrompt(text) {
  const normalized = normalizePrompt(text);
  if (!/(carry|carries)/i.test(normalized)) return null;
  if (!/(shot map|pitch plot|map)/i.test(normalized)) return null;
  const playerMatch = normalized.match(/carries?\s+that\s+(.+?)\s+made/i) ||
    normalized.match(/carries?\s+by\s+(.+?)(?:\s+in\s+the\s+|\s+in\s+|\s+from\s+|$)/i);
  const seasonMatch = normalized.match(/(\d{4}\/\d{4})/);
  if (!playerMatch) return null;
  return { player: playerMatch[1].trim(), season: seasonMatch?.[1]?.trim() || null };
}

function parseTeamBlockMapPrompt(text) {
  const normalized = normalizePrompt(text);
  if (!/(block|blocks)/i.test(normalized)) return null;
  if (!/(shot map|pitch plot|map)/i.test(normalized)) return null;
  const teamMatch = normalized.match(/blocks?\s+that\s+(.+?)\s+made/i) ||
    normalized.match(/blocks?\s+by\s+(.+?)(?:\s+in\s+the\s+|\s+in\s+|\s+from\s+|$)/i) ||
    normalized.match(/map\s+of\s+all\s+the\s+blocks?\s+that\s+(.+?)\s+made/i);
  const seasonMatch = normalized.match(/(\d{4}\/\d{4})/);
  if (!teamMatch) return null;
  return { team: teamMatch[1].trim(), season: seasonMatch?.[1]?.trim() || null };
}

function parseGoalsConcededPrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/how many goals did (.+?) concede in the (\d{4}\/\d{4}) season/i);
  if (!match) return null;
  return { team: match[1].trim(), season: match[2].trim() };
}

function parsePlayerCompare(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/(?:compare|vs|versus)\s+(.+?)\s+(?:vs|versus)\s+(.+)/i);
  if (!match) return null;
  return { player_a: match[1].trim(), player_b: match[2].trim() };
}

function parseSinglePlayer(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/for\s+([A-Za-z\s]+)$/i);
  if (!match) return null;
  return { player: match[1].trim() };
}

function parsePizzaPrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/pizza chart.*?for\s+(.+?)(?:\s+with|$)/i);
  if (!match) return null;
  return { player: match[1].trim() };
}

function parseRadarPrompt(text) {
  const normalized = normalizePrompt(text);
  const match = normalized.match(/radar chart.*?(.+?)\s+vs\s+(.+?)(?:\s+|$)/i);
  if (!match) return null;
  const left = match[1].replace(/^comparing\s+/i, "").trim();
  return { player_a: left, player_b: match[2].trim() };
}

function parseBumpyPrompt(text) {
  const normalized = normalizePrompt(text);
  if (!/bumpy chart/i.test(normalized)) return null;
  const topMatch = normalized.match(/top\s+(\d+)/i);
  const seasonsMatch = normalized.match(/last\s+(\d+)\s+seasons/i);
  return {
    top_n: topMatch ? Number(topMatch[1]) : 5,
    seasons_n: seasonsMatch ? Number(seasonsMatch[1]) : 3
  };
}

function parseOrientation(text) {
  if (/vertical/i.test(text)) return "vertical";
  if (/horizontal/i.test(text)) return "horizontal";
  return "horizontal";
}

function parseHalfPitch(text) {
  return /(half[-\\s]?pitch|half pitch|half)/i.test(text);
}

function parseShotMapTeamPrompt(text) {
  const normalized = normalizePrompt(text);
  if (!/shot map/i.test(normalized)) return null;
  if (/comparing|between|vs\\b|against\\b/i.test(normalized)) return null;
  const match = normalized.match(/shot map(?:\\s+for|\\s+of)?\\s+(.+?)$/i);
  if (!match) return null;
  let team = match[1].trim();
  team = team.replace(/\\b(team)\\b/i, "").trim();
  team = team.replace(/\\b(shots?)\\b.*$/i, "").trim();
  team = team.replace(/\\b(in|from)\\b.*\\b(season|random match)\\b.*$/i, "").trim();
  if (!team) return null;
  return { team, orientation: parseOrientation(text), half: parseHalfPitch(text) };
}

function parseShotsComparePrompt(text) {
  const normalized = normalizePrompt(text);
  if (!/shots?/i.test(normalized)) return null;
  const match =
    normalized.match(/(?:between|comparing) (.+?) and (.+?)$/i) ||
    normalized.match(/(.+?)\\s+(?:vs\\.?|versus|against)\\s+(.+)$/i);
  if (!match) return null;
  const teamA = cleanEntityName(match[1]);
  const teamB = cleanEntityName(match[2]);
  if (!teamA || !teamB) return null;
  const matchSpecific = /match/i.test(normalized);
  return { team_a: teamA, team_b: teamB, match_specific: matchSpecific };
}

function parsePitchPlotTeamPrompt(text) {
  const normalized = normalizePrompt(text);
  if (!/pitch plot/i.test(normalized)) return null;
  const match = normalized.match(/pitch plot(?:\\s+of|\\s+for)?\\s+(.+?)$/i);
  if (!match) return null;
  let team = match[1].trim();
  team = team.replace(/\\b(team)\\b/i, "").trim();
  team = team.replace(/\\b(successful\\s+passes?|passes?|shots?)\\b.*$/i, "").trim();
  if (!team) return null;
  const kind = /pass/i.test(normalized) ? "pass" : /shot/i.test(normalized) ? "shot" : "pass";
  return { team, kind, orientation: parseOrientation(text), half: parseHalfPitch(text) };
}

function needsScopeForVisual(text) {
  const normalized = normalizePrompt(text).toLowerCase();
  const visual = /(shot map|pass map|heatmap|pass network|pitch plot)/.test(normalized);
  const hasSeason = /(20\d{2}\/20\d{2}|season|2023\/2024)/.test(normalized);
  const hasMatch = /(match\s*id|vs\s|against\s|opponent|home|away)/.test(normalized);
  if (visual && !hasSeason && !hasMatch) {
    return true;
  }
  return false;
}

function isVisualizationPrompt(text) {
  const normalized = normalizePrompt(text).toLowerCase();
  return /(shot map|pass map|heatmap|pass network|pitch plot|pitch|visual|chart)/.test(normalized);
}

async function callWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    return response;
  } finally {
    clearTimeout(timeout);
  }
}

async function callOpenRouter(payload, apiKey, retries = 2, fallbackModel = "moonshotai/kimi-k2:free") {
  try {
    const response = await callWithTimeout(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
          "HTTP-Referer": `http://localhost:${PORT}`,
          "X-Title": "OpenRouter Studio",
        },
        body: JSON.stringify(payload),
      },
      120000
    );

    if (!response.ok) {
      const err = await response.json().catch(() => ({}));
      const message = err?.error?.message || err?.message || response.statusText;
      const shouldFallback = response.status >= 500 || response.status === 429;
      if (shouldFallback && fallbackModel && payload?.model !== fallbackModel) {
        const fallbackPayload = { ...payload, model: fallbackModel };
        try {
          return await callOpenRouter(fallbackPayload, apiKey, retries, null);
        } catch (fallbackError) {
          throw new Error(message);
        }
      }
      throw new Error(message);
    }

    return response.json();
  } catch (error) {
    if (retries > 0 && error?.name === "AbortError") {
      return callOpenRouter(payload, apiKey, retries - 1);
    }
    throw error;
  }
}

function buildAnalysisPrompt(userPrompt, dataPreview, rowCount) {
  const fields =
    dataPreview && dataPreview.length ? Object.keys(dataPreview[0]) : [];
  let dataText = JSON.stringify(dataPreview || []);
  let truncated = false;
  if (dataText.length > 6000) {
    dataText = dataText.slice(0, 6000);
    truncated = true;
  }
  const sampleNote =
    rowCount && dataPreview && rowCount > dataPreview.length
      ? `Sample of ${dataPreview.length} rows from ${rowCount} total rows.`
      : `Rows provided: ${dataPreview?.length || 0}.`;
  return [
    `User request: ${userPrompt}`,
    `Available fields: ${fields.join(", ") || "none"}`,
    sampleNote,
    truncated ? "Data preview is truncated." : "Data preview below.",
    `Data: ${dataText}`,
  ].join("\n");
}

function sanitizeAnalysisText(text) {
  if (!text) return null;
  let cleaned = String(text).trim();
  // Strip markdown image tags and any embedded data URIs.
  cleaned = cleaned.replace(/!\[[^\]]*\]\([^\)]*\)/g, "");
  const dataUriIndex = cleaned.toLowerCase().indexOf("data:image");
  if (dataUriIndex >= 0) {
    cleaned = cleaned.slice(0, dataUriIndex).trim();
  }
  cleaned = cleaned.replace(/```[\s\S]*?```/g, "").trim();
  if (cleaned.length > 800) {
    cleaned = `${cleaned.slice(0, 800).trim()}…`;
  }
  return cleaned || null;
}

function filterAnalysisByFields(text, fields) {
  if (!text) return null;
  const fieldSet = new Set((fields || []).map((f) => String(f).toLowerCase()));
  const allowMinute =
    fieldSet.has("minute") ||
    fieldSet.has("time") ||
    fieldSet.has("date_time");
  let cleaned = String(text).trim();
  const sentences = cleaned
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
  const filtered = allowMinute
    ? sentences
    : sentences.filter(
        (s) =>
          !/\bminute\b/i.test(s) &&
          !/\b\d{1,3}['′]/.test(s) &&
          !/\bhalf\b/i.test(s)
      );
  cleaned = filtered.join(" ").trim();
  return cleaned || null;
}

function wantsDeepAnalysis(prompt) {
  if (!prompt) return false;
  return /(deep|detailed|detail|break down|full analysis|in-depth|advanced)/i.test(prompt);
}

function enforceAnalysisStyle(text, prompt, fields) {
  if (!text) return null;
  let cleaned = filterAnalysisByFields(text, fields) || String(text).trim();
  if (!/chart above|image above|figure above|visual above/i.test(cleaned)) {
    cleaned = `As shown in the chart above, ${cleaned}`;
  }
  const sentences = cleaned
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
  if (!wantsDeepAnalysis(prompt) && sentences.length > 4) {
    cleaned = sentences.slice(0, 4).join(" ");
  }
  return cleaned;
}

async function analyzeVisualization(userPrompt, image, apiKey) {
  if (!apiKey || !image?.data_preview?.length) return null;
  const prompt = buildAnalysisPrompt(
    userPrompt,
    image.data_preview,
    image.row_count || image.data_preview.length
  );
  const fields =
    image.data_fields ||
    (image.data_preview?.length ? Object.keys(image.data_preview[0]) : []);
  const models = [
    "moonshotai/kimi-k2:free",
    "openai/gpt-oss-120b:free",
    "deepseek/deepseek-r1-0528:free",
  ];
  for (const model of models) {
    try {
      const response = await callOpenRouter(
        {
          model,
          temperature: 0.3,
          max_tokens: 350,
          stream: false,
          messages: [
            {
              role: "system",
              content:
                "You are a football analytics assistant. Provide a concise analysis tied to the user's request. Highlight key patterns or anomalies. Use exactly 3–4 sentences by default unless the user explicitly asks for deeper analysis. Always reference the visualization by saying \"chart above\" in your response. Do NOT include images, data URIs, code blocks, or raw data dumps.",
            },
            { role: "user", content: prompt },
          ],
        },
        apiKey,
        1,
        null
      );
      const text = enforceAnalysisStyle(
        sanitizeAnalysisText(response?.choices?.[0]?.message?.content),
        userPrompt,
        fields
      );
      if (text) return text;
    } catch (error) {
      // Try next fallback model
    }
  }
  return null;
}

async function fetchSchema(env) {
  const now = Date.now();
  if (schemaCache.data && now - schemaCache.loadedAt < SCHEMA_TTL_MS) {
    return schemaCache.data;
  }

  const response = await fetch(`${env.SUPABASE_URL}/rest/v1/`, {
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      Accept: "application/openapi+json",
    },
  });

  if (!response.ok) {
    throw new Error("Failed to fetch schema from Supabase.");
  }

  const spec = await response.json();
  const definitions = spec.definitions || {};
  const tables = Object.entries(definitions).map(([name, def]) => {
    const props = def?.properties || {};
    const columns = Object.entries(props).map(([col, meta]) => ({
      name: col,
      type: meta?.type || "unknown",
      format: meta?.format,
    }));
    return { name, columns };
  });

  const schema = { tables };
  schemaCache.data = schema;
  schemaCache.loadedAt = now;
  return schema;
}

async function refreshSchemaCacheOnStartup() {
  try {
    const env = parseEnvFile();
    if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) return;
    const schema = await fetchSchema(env);
    const semantic = loadSemanticHints() || {};
    semantic.schema_cache = {
      updated_at: new Date().toISOString(),
      tables: schema.tables || [],
    };
    semantic.schema_snapshot = schema.tables || [];
    saveJson(SEMANTIC_PATH, semantic);
  } catch (error) {
    console.warn("Schema cache refresh failed:", error?.message || error);
  }
}

function findTable(schema, table) {
  return schema.tables.find((t) => t.name === table);
}

function validateTableName(table) {
  return table && !/[^a-zA-Z0-9_]/.test(table);
}

function getTableColumns(schema, table) {
  const entry = findTable(schema, table);
  return entry ? entry.columns.map((col) => col.name) : [];
}

function validateColumnName(name) {
  return name && !/[^a-zA-Z0-9_]/.test(name);
}

function parseWhereClause(whereClause, columns) {
  if (!whereClause) return {};
  const filters = {};
  const parts = whereClause.split(/\s+and\s+/i);
  for (const part of parts) {
    let match = part.match(/^([a-zA-Z0-9_]+)\s+ilike\s+'([^']*)'$/i);
    if (match) {
      const col = match[1];
      if (columns.includes(col)) {
        filters[col] = { op: "ilike", value: match[2] };
      }
      continue;
    }

    match = part.match(/^([a-zA-Z0-9_]+)\s*=\s*'([^']*)'$/i);
    if (match) {
      const col = match[1];
      if (columns.includes(col)) {
        filters[col] = { op: "eq", value: match[2] };
      }
      continue;
    }

    match = part.match(/^([a-zA-Z0-9_]+)\s+in\s*\\(([^)]*)\\)$/i);
    if (match) {
      const col = match[1];
      if (columns.includes(col)) {
        const raw = match[2]
          .split(",")
          .map((value) => value.trim().replace(/^'|'$/g, ""))
          .filter(Boolean);
        filters[col] = { op: "in", value: raw };
      }
    }
  }
  return filters;
}

function buildFilterParams(filters, columns) {
  const params = new URLSearchParams();
  if (!filters || typeof filters !== "object") {
    return params;
  }
  Object.entries(filters).forEach(([key, raw]) => {
    if (!validateColumnName(key) || !columns.includes(key)) return;
    let op = "eq";
    let value = raw;
    if (raw && typeof raw === "object" && "op" in raw && "value" in raw) {
      op = String(raw.op || "eq").toLowerCase();
      value = raw.value;
    }
    const allowed = ["eq", "ilike", "like", "gt", "gte", "lt", "lte", "in"];
    if (!allowed.includes(op)) return;
    if (op === "in") {
      const list = Array.isArray(value) ? value : [value];
      const encoded = list.map((v) => `${v}`).join(",");
      params.set(key, `in.(${encoded})`);
      return;
    }
    params.set(key, `${op}.${value}`);
  });
  return params;
}

function parseSelect(select, columns) {
  if (!select || select === "*") {
    return "*";
  }
  const parts = select
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (!parts.length) {
    throw new Error("Invalid select list.");
  }
  for (const part of parts) {
    if (!validateColumnName(part)) {
      throw new Error(
        "Invalid select list. Use comma-separated column names only (no SQL keywords)."
      );
    }
    if (!columns.includes(part)) {
      throw new Error(`Unknown column in select: ${part}`);
    }
  }
  return parts.join(",");
}

async function executeSqlQuery(query, env, schema) {
  const normalized = query.trim().replace(/\s+/g, " ");
  const match = normalized.match(
    /^select (.+) from ([a-zA-Z0-9_]+)(?: where (.+?))?(?: group by ([a-zA-Z0-9_]+))?(?: limit (\\d+))?$/i
  );
  if (!match) {
    throw new Error(
      "SQL not supported. Use: SELECT ... FROM table [WHERE ...] [GROUP BY col] [LIMIT n]."
    );
  }

  const selectClause = match[1];
  const table = match[2];
  const whereClause = match[3];
  const groupBy = match[4];
  const limit = match[5] ? Number(match[5]) : undefined;

  if (!validateTableName(table)) {
    throw new Error("Invalid table name.");
  }
  const schemaTable = findTable(schema, table);
  if (!schemaTable) {
    throw new Error(`Unknown table: ${table}`);
  }
  const columns = getTableColumns(schema, table);

  const selectParts = selectClause.split(",").map((part) => part.trim());
  const aggregates = [];
  const plainColumns = [];
  for (const part of selectParts) {
    const countMatch = part.match(/^count\\(\\*\\)$/i);
    if (countMatch) {
      aggregates.push({ op: "count", column: null, alias: "count" });
      continue;
    }
    const sumMatch = part.match(/^sum\\(([a-zA-Z0-9_]+)\\)$/i);
    if (sumMatch) {
      const col = sumMatch[1];
      if (!columns.includes(col)) throw new Error(`Unknown column: ${col}`);
      aggregates.push({ op: "sum", column: col, alias: `sum_${col}` });
      continue;
    }
    if (!columns.includes(part)) {
      throw new Error(`Unknown column: ${part}`);
    }
    plainColumns.push(part);
  }

  const filters = parseWhereClause(whereClause, columns);

  if (!groupBy && aggregates.length === 0) {
    const select = plainColumns.length ? plainColumns.join(",") : "*";
    return queryPublicTable({ table, select, filters, limit }, env, schema);
  }

  if (!groupBy && aggregates.length === 1 && aggregates[0].op === "count") {
    return countPublicTable({ table, filters }, env, schema);
  }

  if (!groupBy && aggregates.length === 1 && aggregates[0].op === "sum") {
    return aggregatePublicTable(
      { table, column: aggregates[0].column, operation: "sum", filters, limit },
      env,
      schema
    );
  }

  if (!groupBy) {
    throw new Error("Group by column required for multiple aggregates.");
  }

  if (!columns.includes(groupBy)) {
    throw new Error(`Unknown group by column: ${groupBy}`);
  }

  const selectCols = [groupBy];
  aggregates.forEach((agg) => {
    if (agg.column && !selectCols.includes(agg.column)) {
      selectCols.push(agg.column);
    }
  });
  if (!aggregates.length) {
    throw new Error("Group by requires at least one aggregate.");
  }

  const rowsResponse = await queryPublicTable(
    { table, select: selectCols.join(","), filters, limit },
    env,
    schema
  );
  const rows = rowsResponse.data || [];
  const grouped = {};

  rows.forEach((row) => {
    const key = row[groupBy] ?? "Unknown";
    if (!grouped[key]) {
      grouped[key] = { [groupBy]: key };
      aggregates.forEach((agg) => {
        grouped[key][agg.alias] = 0;
      });
    }
    aggregates.forEach((agg) => {
      if (agg.op === "count") {
        grouped[key][agg.alias] += 1;
      } else if (agg.op === "sum") {
        const value = Number(row[agg.column]);
        if (!Number.isNaN(value)) {
          grouped[key][agg.alias] += value;
        }
      }
    });
  });

  return { data: Object.values(grouped) };
}

function rewriteSqlOnError(query, errorMsg) {
  const lower = (errorMsg || "").toLowerCase();
  let rewritten = query;
  if (lower.includes("syntax error at or near \"limit\"")) {
    rewritten = rewritten.replace(/\blimit\b[\s\S]*$/i, "").trim();
    rewritten = `${rewritten} limit 5000`;
  }
  if (lower.includes("statement timeout")) {
    if (!/\\blimit\\b/i.test(rewritten)) {
      rewritten = `${rewritten} limit 5000`;
    }
  }
  if (lower.includes("column") && lower.includes("end_x")) {
    rewritten = rewritten.replace(/,\\s*end_x\\s*/gi, ", ");
    rewritten = rewritten.replace(/end_x\\s*,\\s*/gi, "");
    rewritten = rewritten.replace(/end_x\\b/gi, "");
  }
  if (lower.includes("column") && lower.includes("end_y")) {
    rewritten = rewritten.replace(/,\\s*end_y\\s*/gi, ", ");
    rewritten = rewritten.replace(/end_y\\s*,\\s*/gi, "");
    rewritten = rewritten.replace(/end_y\\b/gi, "");
  }
  return rewritten.trim();
}

function validateColumnsInSql(query, schema) {
  const tableMatch = query.match(/from\\s+([a-zA-Z0-9_]+)/i);
  if (!tableMatch) return true;
  const table = tableMatch[1];
  const tableInfo = findTable(schema, table);
  if (!tableInfo) return true;
  const columns = getTableColumns(schema, table);
  const selectMatch = query.match(/select\\s+(.+?)\\s+from/i);
  if (!selectMatch) return true;
  const selectPart = selectMatch[1];
  const parts = selectPart.split(",").map((p) => p.trim());
  for (const part of parts) {
    const col = part.replace(/\\b(count|sum|avg|min|max)\\s*\\(|\\)|\\sas\\s+.+$/gi, "").trim();
    if (!col || col === "*" || /\\d/.test(col)) continue;
    if (!columns.includes(col) && !/\\s+as\\s+/i.test(part)) {
      return false;
    }
  }
  return true;
}

async function runSqlRpc(query, env, attempts = 2) {
  if (!query || typeof query !== "string") {
    throw new Error("Query is required.");
  }
  let cleaned = query.trim();
  cleaned = cleaned.replace(/;+$/g, "");
  if (cleaned.includes(";")) {
    throw new Error("Multiple statements are not allowed");
  }
  const schema = await fetchSchema(env);
  if (!validateColumnsInSql(cleaned, schema)) {
    throw new Error("Query references unknown columns. Check schema.");
  }
  const response = await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/run_sql_readonly`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query: cleaned }),
  });

  if (!response.ok) {
    const err = await response.text();
    if (attempts > 0) {
      const rewritten = rewriteSqlOnError(cleaned, err);
      if (rewritten && rewritten !== cleaned) {
        return runSqlRpc(rewritten, env, attempts - 1);
      }
    }
    throw new Error(err || "SQL RPC failed.");
  }

  const data = await response.json().catch(() => []);
  return { data };
}

async function renderMplSoccer(params, env) {
  const query = params?.query;
  let chartType = params?.chart_type || "shot_map";
  if (chartType.endsWith("_chart")) {
    chartType = chartType.replace("_chart", "");
  }
  const title = params?.title || "";
  const subtitle = params?.subtitle || "";
  const xField = params?.x_field || "x";
  const yField = params?.y_field || "y";
  const endXField = params?.end_x_field || "end_x";
  const endYField = params?.end_y_field || "end_y";

  if (!env.MPLSOCCER_URL) {
    throw new Error("MPLSOCCER_URL not set.");
  }

  let data = params?.data || [];
  const templateQuery = buildTemplateQuery(params?.template, params?.template_vars);
  if (params?.template && !templateQuery) {
    return { error: "Template not supported for this dataset." };
  }
  let effectiveQuery = query || templateQuery;
  if (chartType === "pass_map" && effectiveQuery) {
    const lower = effectiveQuery.toLowerCase();
    if (!lower.includes("end_x") || !lower.includes("end_y")) {
      if (lower.includes("viz_match_events_with_match") || lower.includes("viz_match_events")) {
        effectiveQuery = ensureSelectIncludes(effectiveQuery, "end_x");
        effectiveQuery = ensureSelectIncludes(effectiveQuery, "end_y");
      }
    }
  }
  if (params?.series_split_field === "team_name" && effectiveQuery) {
    effectiveQuery = ensureTeamNameInQuery(effectiveQuery);
  }
  effectiveQuery = enforceLastNLimit(
    effectiveQuery,
    params?.prompt_text || "",
    params?.series_split_field
  );
  if (!effectiveQuery && !["radar", "pizza", "bumpy"].includes(chartType)) {
    return { error: "Query is required." };
  }
  if (!data.length && effectiveQuery) {
    const result = await runSqlRpc(effectiveQuery, env);
    data = result.data || [];
  }

  const dataPreview = Array.isArray(data) ? data.slice(0, 200) : [];
  const normalized = normalizeChartData(chartType, data, params);

  const response = await fetch(env.MPLSOCCER_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chart_type: chartType,
      title,
      subtitle,
      x_field: xField,
      y_field: yField,
      end_x_field: endXField,
      end_y_field: endYField,
      data: normalized.data || [],
      series: normalized.series || null,
      metrics: normalized.metrics || null,
      values: normalized.values || null,
      values_compare: normalized.values_compare || null,
      orientation: params?.orientation || "horizontal",
      half: params?.half || false,
      series_label: params?.series_label || null,
      marker_rules: params?.marker_rules || null,
      highlight_rules: params?.highlight_rules || null
    }),
  });

  if (!response.ok) {
    const err = await response.text();
    return { error: err || "Visualization server error." };
  }

  const payload = await response.json();
  return {
    image_base64: payload.image_base64,
    mime: payload.mime || "image/png",
    row_count: data.length,
    data_preview: dataPreview,
    data_fields: dataPreview.length ? Object.keys(dataPreview[0]) : [],
  };
}

async function queryPublicTable(args, env, schema) {
  const { table, select, filters, limit, order } = args;
  if (!validateTableName(table)) {
    throw new Error("Invalid table name.");
  }
  if (!findTable(schema, table)) {
    throw new Error(`Unknown table: ${table}`);
  }

  const columns = getTableColumns(schema, table);
  const params = buildFilterParams(filters, columns);
  params.set("select", parseSelect(select, columns));
  if (limit) params.set("limit", String(limit));
  if (order?.column) {
    if (!validateColumnName(order.column)) {
      throw new Error("Invalid order column.");
    }
    if (!columns.includes(order.column)) {
      throw new Error(`Unknown order column: ${order.column}`);
    }
    params.set("order", `${order.column}.${order.ascending === false ? "desc" : "asc"}`);
  }


  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${params.toString()}`;
  const response = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
    },
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(err || "Supabase query failed.");
  }

  const data = await response.json();
  return { data };
}

async function countPublicTable(args, env, schema) {
  const { table, filters } = args;
  if (!validateTableName(table)) {
    throw new Error("Invalid table name.");
  }
  if (!findTable(schema, table)) {
    throw new Error(`Unknown table: ${table}`);
  }

  const columns = getTableColumns(schema, table);
  const params = buildFilterParams(filters, columns);
  params.set("select", "*");

  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${params.toString()}`;
  const response = await fetch(url, {
    method: "HEAD",
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: "count=exact",
    },
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(err || "Supabase count failed.");
  }

  const contentRange = response.headers.get("content-range") || "";
  const count = Number(contentRange.split("/").pop());
  if (Number.isNaN(count)) {
    throw new Error("Unable to read count from Supabase.");
  }

  return { count };
}

async function aggregatePublicTable(args, env, schema) {
  const { table, column, operation, filters, limit } = args;
  if (!validateTableName(table) || !validateColumnName(column)) {
    throw new Error("Invalid table or column name.");
  }
  if (!findTable(schema, table)) {
    throw new Error(`Unknown table: ${table}`);
  }

  const columns = getTableColumns(schema, table);
  if (!columns.includes(column)) {
    throw new Error(`Unknown column: ${column}`);
  }
  const params = buildFilterParams(filters, columns);
  params.set("select", column);
  const effectiveLimit = limit || 5000;
  params.set("limit", String(effectiveLimit));

  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${params.toString()}`;
  const response = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: "count=exact",
    },
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(err || "Supabase aggregate failed.");
  }

  const rows = await response.json();
  const values = rows.map((row) => Number(row[column])).filter((v) => Number.isFinite(v));
  if (!values.length) {
    return { result: 0, sampled: rows.length };
  }

  let result = 0;
  const op = (operation || "sum").toLowerCase();
  if (op === "sum") {
    result = values.reduce((acc, v) => acc + v, 0);
  } else if (op === "avg") {
    result = values.reduce((acc, v) => acc + v, 0) / values.length;
  } else if (op === "min") {
    result = Math.min(...values);
  } else if (op === "max") {
    result = Math.max(...values);
  } else {
    throw new Error("Unsupported operation. Use sum, avg, min, or max.");
  }

  const contentRange = response.headers.get("content-range") || "";
  const total = Number(contentRange.split("/").pop());

  return {
    result,
    sampled: values.length,
    total: Number.isNaN(total) ? undefined : total,
  };
}

async function answerGoalsQuestion(question, env) {
  const match = question.match(/how many goals did (.+?) score\\??/i);
  if (!match) return null;
  const playerQuery = match[1].trim();
  if (!playerQuery) return null;

  const searchTerms = [playerQuery];
  const tokens = playerQuery.split(/\s+/).filter(Boolean);
  if (tokens.length > 1) {
    searchTerms.push(tokens[0], tokens[tokens.length - 1]);
  }

  let player = null;
  for (const term of searchTerms) {
    const playerParams = new URLSearchParams({
      select: "id,name",
      name: `ilike.*${term}*`,
      limit: "5",
    });

    const playerResponse = await fetch(
      `${env.SUPABASE_URL}/rest/v1/players?${playerParams.toString()}`,
      {
        headers: {
          apikey: env.SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        },
      }
    );

    if (!playerResponse.ok) {
      return null;
    }

    const players = await playerResponse.json();
    if (players[0]) {
      player = players[0];
      break;
    }
  }

  if (!player) {
    return `I couldn't find a player matching \"${playerQuery}\" in the database.`;
  }

  const countParams = new URLSearchParams({
    select: "id",
    player_id: `eq.${player.id}`,
    result_name: "eq.Goal",
  });

  const countResponse = await fetch(
    `${env.SUPABASE_URL}/rest/v1/viz_match_events?${countParams.toString()}`,
    {
      method: "HEAD",
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        Prefer: "count=exact",
      },
    }
  );

  if (!countResponse.ok) {
    return null;
  }

  const contentRange = countResponse.headers.get("content-range") || "";
  const count = Number(contentRange.split("/").pop());
  if (Number.isNaN(count)) {
    return null;
  }

  return `${player.name} has scored ${count} goals in the database.`;
}

async function answerPassComparison(question, env) {
  const match = question.match(/average number of successful passes per match between (.+?) and (.+?)\\??/i);
  if (!match) return null;
  const teamA = match[1].trim();
  const teamB = match[2].trim();
  if (!teamA || !teamB) return null;

  const safeTeamA = teamA.replace(/'/g, "''");
  const safeTeamB = teamB.replace(/'/g, "''");
  const query =
    "select team_name, avg(pass_count) as avg_successful_passes from (" +
    "select match_id, team_name, count(*) as pass_count " +
    "from viz_match_events_with_match " +
    "where event_name = 'Pass' and result_name = 'Success' " +
    `and team_name in ('${safeTeamA}', '${safeTeamB}') ` +
    "group by match_id, team_name" +
    ") t group by team_name";

  const result = await runSqlRpc(query, env);
  const rows = result.data || [];
  if (!rows.length) {
    return `I couldn't find any successful pass data for ${teamA} or ${teamB} in the database.`;
  }

  const formatRow = (row) =>
    `${row.team_name}: ${Number(row.avg_successful_passes).toFixed(2)}`;
  return `Average successful passes per match\\n- ${rows.map(formatRow).join("\\n- ")}`;
}

function buildToolsSchema() {
  return [
    {
      type: "function",
      function: {
        name: "get_schema",
        description:
          "List tables in the public schema, or describe a specific table when provided.",
        parameters: {
          type: "object",
          properties: {
            table: { type: "string", description: "Optional table name to describe." },
            limit: { type: "integer", description: "Optional max number of tables." },
          },
        },
      },
    },
    {
      type: "function",
      function: {
        name: "get_semantic_hints",
        description:
          "Get domain-specific hints: table descriptions, relationships, and domain rules.",
        parameters: { type: "object", properties: {} },
      },
    },
    {
      type: "function",
      function: {
        name: "query_public_table",
        description: "Read data from a public Supabase table (read-only).",
        parameters: {
          type: "object",
          properties: {
            table: { type: "string", description: "Public table name." },
            select: { type: "string", description: "Columns to select, e.g. 'id,name' or '*'" },
            filters: {
              type: "object",
              description:
                "Filters map: { column: value } for eq, or { column: { op: 'eq|ilike|like|gt|gte|lt|lte|in', value: any } }",
              additionalProperties: true,
            },
            limit: { type: "integer", description: "Max rows to return." },
            order: {
              type: "object",
              properties: {
                column: { type: "string" },
                ascending: { type: "boolean" },
              },
            },
          },
          required: ["table"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "count_public_table",
        description: "Count rows in a public Supabase table with optional filters.",
        parameters: {
          type: "object",
          properties: {
            table: { type: "string" },
            filters: {
              type: "object",
              description:
                "Filters map: { column: value } for eq, or { column: { op: 'eq|ilike|like|gt|gte|lt|lte|in', value: any } }",
              additionalProperties: true,
            },
          },
          required: ["table"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "aggregate_public_table",
        description: "Aggregate a numeric column in a public table (sum, avg, min, max).",
        parameters: {
          type: "object",
          properties: {
            table: { type: "string" },
            column: { type: "string" },
            operation: { type: "string", enum: ["sum", "avg", "min", "max"] },
            filters: {
              type: "object",
              description:
                "Filters map: { column: value } for eq, or { column: { op: 'eq|ilike|like|gt|gte|lt|lte|in', value: any } }",
              additionalProperties: true,
            },
            limit: { type: "integer" },
          },
          required: ["table", "column", "operation"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "run_sql",
        description:
          "Run a restricted read-only SQL query. Supported: SELECT columns or COUNT(*) or SUM(col) FROM table [WHERE col = 'value' AND col ILIKE 'pattern'] [GROUP BY col] [LIMIT n].",
        parameters: {
          type: "object",
          properties: {
            query: { type: "string" },
          },
          required: ["query"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "run_sql_rpc",
        description:
          "Run a full read-only SQL query via Supabase RPC (supports joins/group-bys).",
        parameters: {
          type: "object",
          properties: {
            query: { type: "string" },
          },
          required: ["query"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "render_mplsoccer",
        description:
          "Generate a football visualization (shot_map, pass_map, pitch_plot, heatmap, pass_network, radar, pizza, bumpy).",
        parameters: {
          type: "object",
          properties: {
            chart_type: {
              type: "string",
              enum: [
                "shot_map",
                "pass_map",
                "pitch_plot",
                "heatmap",
                "pass_network",
                "radar",
                "pizza",
                "bumpy"
              ]
            },
            title: { type: "string" },
            subtitle: { type: "string" },
            query: { type: "string", description: "SQL query returning x/y fields." },
            x_field: { type: "string" },
            y_field: { type: "string" },
            end_x_field: { type: "string" },
            end_y_field: { type: "string" },
            orientation: { type: "string", enum: ["horizontal", "vertical"] },
            half: { type: "boolean" },
            metrics: { type: "array", items: { type: "string" } },
            values: { type: "array", items: { type: "number" } },
            values_compare: { type: "array", items: { type: "number" } },
            series: {
              type: "array",
              items: { type: "object" },
              description:
                "For stacked pitch charts: [{label, color, data:[{x,y,end_x,end_y,...}]}]"
            },
            series_split_field: { type: "string", description: "Column to group into series." },
            marker_rules: {
              type: "array",
              items: { type: "object" },
              description:
                "Optional marker overrides, e.g. [{target:'shot', marker:'s'}] or [{target:'pass', marker:'^'}]."
            },
            highlight_rules: {
              type: "array",
              items: { type: "object" },
              description:
                "Optional highlight rules, e.g. [{type:'penalty_area', color:'#ff0000'}]."
            },
            template: {
              type: "string",
              description: "SQL template key (shots_by_team, passes_success_by_team, heatmap_events_by_team, shot_map_by_player, pass_map_by_player, pass_network_by_team, heatmap_by_player, metrics_player_summary, metrics_two_players, bumpy_top_scorers_by_season, shots_conceded_by_team, shots_conceded_last_n_matches)."
            },
            template_vars: { type: "object", description: "Variables for SQL template." }
          },
          required: ["chart_type"],
        },
      },
    },
  ];
}

function getSchemaResult(args, schema) {
  const tableName = args?.table;
  if (tableName) {
    const table = findTable(schema, tableName);
    if (!table) {
      return { error: `Unknown table: ${tableName}` };
    }
    return { table: table.name, columns: table.columns };
  }
  const limit = Number.isFinite(args?.limit) ? Number(args.limit) : 40;
  const tables = schema.tables
    .slice(0, Math.max(1, Math.min(limit, schema.tables.length)))
    .map((t) => t.name);
  return { tables };
}

function serveFile(req, res) {
  const safePath = path.normalize(req.url.split("?")[0]).replace(/^\/+/, "");
  const filePath = path.join(ROOT, safePath || "index.html");
  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  if (path.basename(filePath) === ".env") {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const typeMap = {
      ".html": "text/html",
      ".js": "text/javascript",
      ".css": "text/css",
      ".json": "application/json",
    };

    res.writeHead(200, { "Content-Type": typeMap[ext] || "application/octet-stream" });
    res.end(data);
  });
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 5000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    return response;
  } finally {
    clearTimeout(id);
  }
}

async function handleHealth(req, res) {
  const env = parseEnvFile();
  const results = {
    openrouter: { ok: false },
    supabase: { ok: false },
    mplsoccer: { ok: false },
  };

  if (env.OPENROUTER_API_KEY) {
    try {
      const response = await fetchWithTimeout("https://openrouter.ai/api/v1/models", {
        headers: { Authorization: `Bearer ${env.OPENROUTER_API_KEY}` },
      });
      results.openrouter.ok = response.ok;
      results.openrouter.status = response.status;
    } catch (error) {
      results.openrouter.error = error.message || String(error);
    }
  } else {
    results.openrouter.error = "Missing OPENROUTER_API_KEY";
  }

  if (env.SUPABASE_URL && env.SUPABASE_SERVICE_ROLE_KEY) {
    try {
      const response = await fetchWithTimeout(`${env.SUPABASE_URL}/rest/v1/`, {
        headers: {
          apikey: env.SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
          Accept: "application/openapi+json",
        },
      });
      results.supabase.ok = response.ok;
      results.supabase.status = response.status;
    } catch (error) {
      results.supabase.error = error.message || String(error);
    }
  } else {
    results.supabase.error = "Missing Supabase credentials";
  }

  if (env.MPLSOCCER_URL) {
    try {
      const baseUrl = env.MPLSOCCER_URL.replace(/\/render\/?$/, "");
      const response = await fetchWithTimeout(`${baseUrl}/health`);
      results.mplsoccer.ok = response.ok;
      results.mplsoccer.status = response.status;
    } catch (error) {
      results.mplsoccer.error = error.message || String(error);
    }
  } else {
    results.mplsoccer.error = "Missing MPLSOCCER_URL";
  }

  const allOk = results.openrouter.ok && results.supabase.ok && results.mplsoccer.ok;
  sendJson(res, allOk ? 200 : 503, { status: allOk ? "ok" : "degraded", services: results });
}

async function handleVoiceTool(req, res) {
  const env = parseEnvFile();
  const sharedSecret = env.ELEVENLABS_TOOL_SECRET;
  if (sharedSecret) {
    const provided = req.headers["x-pss-tool-secret"];
    if (!provided || provided !== sharedSecret) {
      sendJson(res, 401, { error: "Unauthorized tool call." });
      return;
    }
  }

  let body = "";
  req.on("data", (chunk) => {
    body += chunk;
  });

  req.on("end", async () => {
    let payload;
    try {
      payload = JSON.parse(body || "{}");
    } catch (error) {
      sendJson(res, 400, { error: "Invalid JSON" });
      return;
    }

    const query = String(payload.query || payload.text || "").trim();
    if (!query) {
      sendJson(res, 400, { error: "Missing query." });
      return;
    }

    const jobId = randomUUID();
    broadcastVoiceEvent({ type: "voice_start", id: jobId, query });

    try {
      const response = await callWithTimeout(
        `http://127.0.0.1:${PORT}/api/chat`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model: "openai/gpt-oss-120b:free",
            source: "voice",
            messages: [{ role: "user", content: query }],
          }),
        },
        120000
      );
      const data = await response.json().catch(() => ({}));
      let text = data?.choices?.[0]?.message?.content || "(no response)";
      if (data?.image?.image_base64) {
        text += " A visualization was generated in the dashboard.";
      }
      broadcastVoiceEvent({
        type: "voice_result",
        id: jobId,
        content: data?.choices?.[0]?.message?.content || "(no response)",
        image: data?.image || null,
      });
      sendJson(res, 200, { result: text });
    } catch (error) {
      broadcastVoiceEvent({
        type: "voice_error",
        id: jobId,
        error: error.message || "Voice tool failed.",
      });
      sendJson(res, 500, { error: error.message || "Tool error" });
    }
  });
}

async function handleElevenLabsToken(req, res) {
  const env = parseEnvFile();
  const apiKey = env.ELEVENLABS_API_KEY;
  const agentId = env.ELEVENLABS_AGENT_ID;
  if (!apiKey || !agentId) {
    sendJson(res, 500, { error: "Missing ELEVENLABS_API_KEY or ELEVENLABS_AGENT_ID." });
    return;
  }

  try {
    const response = await fetch(
      `https://api.elevenlabs.io/v1/convai/conversation/token?agent_id=${agentId}`,
      {
        headers: {
          "xi-api-key": apiKey,
          "Content-Type": "application/json",
        },
      }
    );
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      sendJson(res, response.status, { error: payload?.detail || payload?.error || "Token failed." });
      return;
    }
    sendJson(res, 200, { token: payload?.token });
  } catch (error) {
    sendJson(res, 500, { error: error.message || "Token failed." });
  }
}

async function proxyChat(req, res) {
  const env = parseEnvFile();
  const apiKey = env.OPENROUTER_API_KEY;
  const supabaseUrl = env.SUPABASE_URL;
  const serviceKey = env.SUPABASE_SERVICE_ROLE_KEY;

  if (!apiKey) {
    sendJson(res, 500, { error: "Missing OPENROUTER_API_KEY in .env" });
    return;
  }
  if (!supabaseUrl || !serviceKey) {
    sendJson(res, 500, { error: "Missing Supabase credentials in .env" });
    return;
  }

  let body = "";
  req.on("data", (chunk) => {
    body += chunk;
  });

  req.on("end", async () => {
    let payload;
    try {
      payload = JSON.parse(body || "{}");
    } catch (error) {
      sendJson(res, 400, { error: "Invalid JSON" });
      return;
    }

    try {
      const memory = getMemory();
      const source = payload?.source || "chat";
      if (source === "voice" && Array.isArray(payload.messages)) {
        const lastUserIndex = getLastUserMessageIndex(payload.messages);
        if (lastUserIndex >= 0) {
          const rawVoice = payload.messages[lastUserIndex]?.content || "";
          if (rawVoice) {
            try {
              const resolvedVoice = await resolveTranscriptEntities(rawVoice, env, memory);
              if (resolvedVoice && resolvedVoice !== rawVoice) {
                payload.messages[lastUserIndex] = {
                  ...payload.messages[lastUserIndex],
                  content: resolvedVoice,
                };
              }
            } catch (error) {
              if (process.env.DEBUG_TOOLS === "true") {
                console.log("Voice resolver failed:", error.message || error);
              }
            }
          }
        }
      }
      const tools = buildToolsSchema();
      const mcpSystem = {
        role: "system",
        content:
          "SQL-first mode enforced: for any database question, use run_sql_rpc to answer. For visualization requests, use render_mplsoccer with either a SQL template (template + template_vars) or a direct SQL query (query). Prefer templates when available. IMPORTANT: for pitch charts (shot_map/pass_map/heatmap/pitch_plot), do not call render_mplsoccer without a query or template. For pass_map, always include end_x and end_y so arrows can be drawn; if missing, consult schema and add them. If comparing two teams/players, ensure the result includes a label column (team_name/player_name) and pass series_split_field so each entity is a separate series and color. Start by calling get_semantic_hints to learn domain rules and table meanings, then use get_schema to confirm table/column names, then run_sql_rpc or render_mplsoccer for the final answer. If you have any doubt about a column or table, call get_schema before querying. Only use non-SQL tools if the question is NOT about the database. Use ILIKE for name matches. Prefer viz_match_events or viz_match_events_with_match for event-based stats (goals, shots, cards, assists). Repair rules: if you reference end_x/end_y and the query fails, drop those columns and proceed with x/y only. If a query may be large, add a LIMIT (e.g., 5000) to avoid timeouts. Use shot synonyms: treat shots as event_name in ('Shoot','Shoot Location','Penalty'). For shots conceded by a team, select shots by the opponent in matches where the team appears as home or away (team_name != target AND (home_team_name ILIKE target OR away_team_name ILIKE target)), or use templates shots_conceded_by_team / shots_conceded_last_n_matches. For 'last N matches', use matches.date_time or viz_match_events_with_match.date_time ordered DESC with LIMIT N, then filter events by those match_ids. If you generate a correct SQL query for a new visualization, call render_mplsoccer with the query; the system will store it as a learned template for future exact matches. Strip trailing semicolons from SQL.",
      };
      const lastQuestionRaw = getLastUserMessage(payload.messages || []);
      const cleanedQuestion = stripGreetingPreamble(lastQuestionRaw);
      const pendingResult = handlePendingClarification(cleanedQuestion);
      if (pendingResult?.question) {
        sendAssistantReply(res, pendingResult.question);
        return;
      }
      const resolvedQuestion = pendingResult?.resolved
        ? pendingResult.resolved
        : resolveAlias(cleanedQuestion, memory);
      const lastQuestion = resolvedQuestion;

      const baseParams = extractParamsFromPrompt(lastQuestionRaw, memory);
      if (baseParams?.team_a && baseParams?.team_b) {
        setLastTeams(memory, baseParams.team_a, baseParams.team_b);
      }

      const actionPlan = buildActionPlan(lastQuestionRaw, memory);
      if (actionPlan?.clarification) {
        sendAssistantReply(res, actionPlan.clarification);
        return;
      }

      const arrowsRequested = /arrow|arrows|trajectory|trajector|direction/i.test(lastQuestionRaw);
      if (arrowsRequested) {
        const lastPass = getLastPassMap(memory);
        if (lastPass?.team && lastPass?.match_id) {
          const safeTeam = lastPass.team.replace(/'/g, "''");
          const passQuery =
            "select x, y, end_x, end_y from viz_match_events_with_match " +
            `where match_id = ${lastPass.match_id} ` +
            `and team_name ilike '%${safeTeam}%' ` +
            "and event_name = 'Pass' and result_name = 'Success'";
          const image = await renderMplSoccerAndLearn(
            {
              chart_type: "pass_map",
              query: passQuery,
              title: `${lastPass.team} successful passes (arrows)`,
              subtitle: `Match ID ${lastPass.match_id}`
            },
            env,
            lastQuestionRaw,
            memory
          );
          sendAssistantReply(
            res,
            `Here are all successful passes for ${lastPass.team} in match ${lastPass.match_id} with arrows.`,
            image
          );
          return;
        }
      }

      const earlyLastShotsCompare = parseLastShotsComparePrompt(lastQuestion);
      if (earlyLastShotsCompare) {
        const safeA = earlyLastShotsCompare.team_a.replace(/'/g, "''");
        const safeB = earlyLastShotsCompare.team_b.replace(/'/g, "''");
        const limit = Number(earlyLastShotsCompare.limit) || 100;
        const shotQuery =
          "with ranked as (" +
          "select team_name, x, y, event_name, date_time, " +
          "row_number() over (partition by team_name order by date_time desc) as rn " +
          "from viz_match_events_with_match " +
          "where event_name in ('Shoot','Shoot Location','Penalty') " +
          "and (team_name ilike '%" +
          safeA +
          "%' or team_name ilike '%" +
          safeB +
          "%')" +
          ") " +
          "select team_name, x, y, event_name from ranked where rn <= " +
          limit;
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "shot_map",
            query: shotQuery,
            series_split_field: "team_name",
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Shot map comparing last ${limit} shots for ${earlyLastShotsCompare.team_a} vs ${earlyLastShotsCompare.team_b}.`,
          image
        );
        return;
      }

      const shotsCompare = parseShotsComparePrompt(lastQuestion);
      if (shotsCompare) {
        setLastTeams(memory, shotsCompare.team_a, shotsCompare.team_b);
        if (shotsCompare.match_specific) {
          const safeA = shotsCompare.team_a.replace(/'/g, "''");
          const safeB = shotsCompare.team_b.replace(/'/g, "''");
          const pickMatchQuery =
            "select m.id from matches m " +
            "join teams th on m.home_team_id = th.id " +
            "join teams ta on m.away_team_id = ta.id " +
            "where (th.name ilike '%" +
            safeA +
            "%' and ta.name ilike '%" +
            safeB +
            "%') or (th.name ilike '%" +
            safeB +
            "%' and ta.name ilike '%" +
            safeA +
            "%') " +
            "order by m.date_time desc limit 1";
          const pickResult = await runSqlRpc(pickMatchQuery, env);
          const matchId = pickResult.data?.[0]?.id || null;
          if (!matchId) {
            sendAssistantReply(
              res,
              `I couldn't find a recent match between ${shotsCompare.team_a} and ${shotsCompare.team_b}.`
            );
            return;
          }
          const shotQuery =
            "select team_name, x, y from viz_match_events_with_match " +
            `where match_id = ${matchId} ` +
            "and event_name in ('Shoot','Shoot Location','Penalty')";
          const image = await renderMplSoccerAndLearn(
            {
              chart_type: "shot_map",
              query: shotQuery,
              series_split_field: "team_name",
              title: `All shots - ${shotsCompare.team_a} vs ${shotsCompare.team_b}`,
              subtitle: `Match ID ${matchId}`
            },
            env,
            lastQuestionRaw,
            memory
          );
          sendAssistantReply(
            res,
            `All shots for ${shotsCompare.team_a} vs ${shotsCompare.team_b} (match ${matchId}).`,
            image
          );
          setLastMatchContext(memory, {
            match_id: matchId,
            teams: { team_a: shotsCompare.team_a, team_b: shotsCompare.team_b },
          });
          return;
        }
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "shot_map",
            template: "shots_by_team",
            template_vars: { team_a: shotsCompare.team_a, team_b: shotsCompare.team_b },
            series_split_field: "team_name",
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Shot map comparing ${shotsCompare.team_a} vs ${shotsCompare.team_b}.`,
          image
        );
        return;
      }

      const blockedShotsPlayers = parseBlockedShotsPlayersPrompt(lastQuestionRaw);
      if (blockedShotsPlayers) {
        const lastMatch = getLastMatchContext(memory);
        let matchId = lastMatch?.match_id || null;
        let teams = lastMatch?.teams || getLastTeams(memory);
        if (!matchId && teams?.team_a && teams?.team_b) {
          const safeA = teams.team_a.replace(/'/g, "''");
          const safeB = teams.team_b.replace(/'/g, "''");
          const pickMatchQuery =
            "select m.id from matches m " +
            "join teams th on m.home_team_id = th.id " +
            "join teams ta on m.away_team_id = ta.id " +
            "where (th.name ilike '%" +
            safeA +
            "%' and ta.name ilike '%" +
            safeB +
            "%') or (th.name ilike '%" +
            safeB +
            "%' and ta.name ilike '%" +
            safeA +
            "%') " +
            "order by m.date_time desc limit 1";
          const pickResult = await runSqlRpc(pickMatchQuery, env);
          matchId = pickResult.data?.[0]?.id || null;
        }
        if (!matchId) {
          sendAssistantReply(
            res,
            "Which match should I use? You can share a match ID or specify the teams/season."
          );
          return;
        }
        const blockQuery =
          "select player_name, count(*) as blocks from viz_match_events_with_match " +
          `where match_id = ${matchId} ` +
          "and (event_name ilike '%block%' or category_name ilike '%block%') " +
          "and player_name is not null " +
          "group by player_name " +
          "order by blocks desc";
        const blockResult = await runSqlRpc(blockQuery, env);
        const rows = blockResult.data || [];
        if (!rows.length) {
          sendAssistantReply(res, "I couldn’t find any block events for that match.");
          return;
        }
        const lines = rows
          .slice(0, 20)
          .map((row) => `- ${row.player_name}: ${row.blocks}`);
        sendAssistantReply(
          res,
          `Players who blocked shots in match ${matchId}:\n${lines.join("\n")}`
        );
        setLastMatchContext(memory, { match_id: matchId, teams });
        return;
      }

      const randomShotMap = parseRandomShotMapPrompt(lastQuestion);
      if (randomShotMap) {
        const safeTeam = randomShotMap.team.replace(/'/g, "''");
        const seasonClause = randomShotMap.season
          ? `and season_name ilike '${seasonLikePattern(randomShotMap.season)}' `
          : "";
        const pickMatchQuery =
          "select match_id from viz_match_events_with_match " +
          `where team_name ilike '%${safeTeam}%' ` +
          "and event_name in ('Shoot','Shoot Location','Penalty') " +
          seasonClause +
          "group by match_id order by random() limit 1";
        const pickResult = await runSqlRpc(pickMatchQuery, env);
        const matchId = pickResult.data?.[0]?.match_id;
        if (!matchId) {
          sendAssistantReply(res, `I couldn't find any matches for ${randomShotMap.team}.`);
          return;
        }
        const shotQuery =
          "select x, y from viz_match_events_with_match " +
          `where match_id = ${matchId} ` +
          `and team_name ilike '%${safeTeam}%' ` +
          "and event_name in ('Shoot','Shoot Location','Penalty')";
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "shot_map",
            query: shotQuery,
            title: `${randomShotMap.team} shots`,
            subtitle: `Match ID ${matchId}`
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Here are all shots for ${randomShotMap.team} in match ${matchId}.`,
          image
        );
        setLastMatchContext(memory, { match_id: matchId, teams: { team_a: randomShotMap.team } });
        return;
      }

      const semanticHints = loadSemanticHints();
      const learnedTemplates = semanticHints?.learned_templates || [];
      const learnedExact = learnedTemplates.find(
        (t) => normalizePromptKey(t.source_prompt || "") === normalizePromptKey(lastQuestionRaw)
      );
      if (learnedExact?.name && learnedExact?.chart_type && isLearnedTemplateCompatible(lastQuestionRaw, learnedExact, memory)) {
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: learnedExact.chart_type,
            template: learnedExact.name,
            orientation: parseOrientation(lastQuestionRaw),
            half: parseHalfPitch(lastQuestionRaw)
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(res, `Visualization ready. See the image above.`, image);
        return;
      }

      const learnedGeneral = learnedTemplates.find((t) => {
        if (!t?.intent_keywords?.length) return false;
        const normalized = normalizePromptKey(lastQuestionRaw);
        return t.intent_keywords.every((k) => normalized.includes(k));
      });
      if (learnedGeneral?.query_template && learnedGeneral?.chart_type && isLearnedTemplateCompatible(lastQuestionRaw, learnedGeneral, memory)) {
        const params = extractParamsFromPrompt(lastQuestionRaw, memory);
        if (!learnedGeneral.params?.some((p) => params[p] == null)) {
          const query = fillQueryTemplate(learnedGeneral.query_template, params);
          const image = await renderMplSoccerAndLearn(
            {
              chart_type: learnedGeneral.chart_type,
              query,
              orientation: parseOrientation(lastQuestionRaw),
              half: parseHalfPitch(lastQuestionRaw)
            },
            env,
            lastQuestionRaw,
            memory
          );
          if (image?.image_base64) {
            sendAssistantReply(res, `Visualization ready. See the image above.`, image);
            return;
          }
        }
      }

      const prePitchPlotQuestion = resolveAlias(lastQuestionRaw, memory);
      if (/pitch plot/i.test(prePitchPlotQuestion) && !/conceded|concede/i.test(prePitchPlotQuestion) && !/last\\s+\\d+\\s+matches?/i.test(prePitchPlotQuestion)) {
        const parsedPitch = parsePitchPlotTeamPrompt(prePitchPlotQuestion);
        const team =
          findKnownTeam(parsedPitch?.team || prePitchPlotQuestion, memory) ||
          parsedPitch?.team;
        if (team) {
          const kind = parsedPitch?.kind || (/shot/i.test(prePitchPlotQuestion) ? "shot" : "pass");
          const template = kind === "shot" ? "shots_by_team" : "passes_success_by_team";
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "pitch_plot",
            template,
            template_vars: { team_a: team, team_b: team },
            orientation: parsedPitch?.orientation || parseOrientation(prePitchPlotQuestion),
            half: parsedPitch?.half ?? parseHalfPitch(prePitchPlotQuestion)
          },
          env,
          lastQuestionRaw,
          memory
        );
          sendAssistantReply(res, `Pitch plot for ${team}.`, image);
          return;
        }
      }

      const preShotQuestion = resolveAlias(lastQuestionRaw, memory);
      if (
        /shot map/i.test(preShotQuestion) &&
        !/comparing|between|vs\\b|against\\b/i.test(preShotQuestion) &&
        !/conceded|concede/i.test(preShotQuestion) &&
        !/last\\s+\\d+\\s+matches?/i.test(preShotQuestion) &&
        !/random match/i.test(preShotQuestion) &&
        !/\d{4}\/\d{4}/.test(preShotQuestion)
      ) {
        const parsedShot = parseShotMapTeamPrompt(preShotQuestion);
        const team =
          findKnownTeam(parsedShot?.team || preShotQuestion, memory) ||
          parsedShot?.team;
        if (team) {
          const image = await renderMplSoccerAndLearn(
            {
              chart_type: "shot_map",
              template: "shots_by_team",
              template_vars: { team_a: team, team_b: team },
              orientation: parsedShot?.orientation || parseOrientation(preShotQuestion),
              half: parsedShot?.half ?? parseHalfPitch(preShotQuestion)
            },
            env,
            lastQuestionRaw,
            memory
          );
          sendAssistantReply(res, `Shot map for ${team}.`, image);
          return;
        }
      }

      // If we resolved a pending clarification, continue with resolved question and skip new alias probing.
      const skipAliasPrompt = Boolean(pendingResult?.resolved) || hasKnownAlias(lastQuestionRaw, memory);
      const lastEntity = getLastEntity(memory);
      const pronounRef = /(did he|did she|did they|he|she|they|him|her|them)/i.test(lastQuestionRaw);

      if (needsScopeForVisual(lastQuestionRaw)) {
        const scopeKey = lastQuestionRaw.toLowerCase();
        const pending = {
          kind: "scope",
          scopeKey,
          original: lastQuestionRaw,
          remaining: 2,
          asking: "Which season should I use?"
        };
        savePending(pending);
        sendAssistantReply(res, "Which season should I use? (e.g., 2023/2024)");
        return;
      }

      const carryMap = parseCarryMapPrompt(lastQuestion);
      if (carryMap) {
        const safePlayer = carryMap.player.replace(/'/g, "''");
        const fuzzyPlayer = fuzzyLikePattern(carryMap.player);
        const seasonClause = carryMap.season
          ? `and m.season_name ilike '${seasonLikePattern(carryMap.season)}' `
          : "";
        const carryQuery =
          "select e.x, e.y from v_events_full e " +
          "join matches m on e.match_id = m.id " +
          `where (e.player_name ilike '%${safePlayer}%' or e.player_nickname ilike '%${safePlayer}%' ` +
          `or e.player_name ilike '%${fuzzyPlayer}%' or e.player_nickname ilike '%${fuzzyPlayer}%') ` +
          seasonClause +
          "and (e.event_name ilike '%carry%' or e.category_name ilike '%carry%' or e.event_name ilike '%dribble%' or e.category_name ilike '%dribble%')";
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "shot_map",
            query: carryQuery,
            title: `${carryMap.player} carries`,
            subtitle: carryMap.season ? `Season ${carryMap.season}` : undefined
          },
          env,
          lastQuestionRaw,
          memory
        );
        if (image?.error) {
          sendAssistantReply(
            res,
            `I couldn't find carry events for ${carryMap.player}${carryMap.season ? ` in ${carryMap.season}` : ""}.`
          );
          return;
        }
        sendAssistantReply(res, `Carry map for ${carryMap.player}.`, image);
        return;
      }

      const blockMap = parseTeamBlockMapPrompt(lastQuestion);
      if (blockMap) {
        const safeTeam = blockMap.team.replace(/'/g, "''");
        const fuzzyTeam = fuzzyLikePattern(blockMap.team);
        const seasonClause = blockMap.season
          ? `and m.season_name ilike '${seasonLikePattern(blockMap.season)}' `
          : "";
        const blockQuery =
          "select x, y from viz_match_events_with_match " +
          `where (team_name ilike '%${safeTeam}%' or team_name ilike '%${fuzzyTeam}%') ` +
          (blockMap.season ? `and season_name ilike '${seasonLikePattern(blockMap.season)}' ` : "") +
          "and (event_name ilike '%block%' or category_name ilike '%block%') " +
          "limit 5000";
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "shot_map",
            query: blockQuery,
            title: `${blockMap.team} blocks`,
            subtitle: blockMap.season ? `Season ${blockMap.season}` : undefined
          },
          env,
          lastQuestionRaw,
          memory
        );
        if (image?.error) {
          sendAssistantReply(
            res,
            `I couldn't find block events for ${blockMap.team}${blockMap.season ? ` in ${blockMap.season}` : ""}.`
          );
          return;
        }
        sendAssistantReply(res, `Block map for ${blockMap.team}.`, image);
        return;
      }

      const randomPasses = parseRandomMatchPassesPrompt(lastQuestion);
      if (randomPasses) {
        const safeTeam = randomPasses.team.replace(/'/g, "''");
        const pickMatchQuery =
          "select match_id from viz_match_events_with_match " +
          `where team_name ilike '%${safeTeam}%' ` +
          "and event_name in ('Shoot','Shoot Location','Penalty') " +
          "group by match_id order by random() limit 1";
        const pickResult = await runSqlRpc(pickMatchQuery, env);
        const matchId = pickResult.data?.[0]?.match_id;
        if (!matchId) {
          sendAssistantReply(res, `I couldn't find any matches for ${randomPasses.team}.`);
          return;
        }
        const passQuery =
          "select x, y, end_x, end_y from viz_match_events_with_match " +
          `where match_id = ${matchId} ` +
          `and team_name ilike '%${safeTeam}%' ` +
          "and event_name = 'Pass' and result_name = 'Success'";
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "pass_map",
            query: passQuery,
            title: `${randomPasses.team} successful passes`,
            subtitle: `Match ID ${matchId}`
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Here are all successful passes for ${randomPasses.team} in match ${matchId}.`,
          image
        );
        rememberLastPassMap(memory, { team: randomPasses.team, match_id: matchId });
        setLastMatchContext(memory, { match_id: matchId, teams: { team_a: randomPasses.team } });
        return;
      }

      const randomShots = parseRandomMatchShotsPrompt(lastQuestion);
      if (randomShots) {
        const safeTeam = randomShots.team.replace(/'/g, "''");
        const pickMatchQuery =
          "select match_id from viz_match_events_with_match " +
          `where team_name ilike '%${safeTeam}%' ` +
          "and event_name in ('Shoot','Shoot Location','Penalty') " +
          "group by match_id order by random() limit 1";
        const pickResult = await runSqlRpc(pickMatchQuery, env);
        const matchId = pickResult.data?.[0]?.match_id;
        if (!matchId) {
          sendAssistantReply(res, `I couldn't find any matches for ${randomShots.team}.`);
          return;
        }
        const shotQuery =
          "select x, y from viz_match_events_with_match " +
          `where match_id = ${matchId} ` +
          `and team_name ilike '%${safeTeam}%' ` +
          "and event_name in ('Shoot','Shoot Location','Penalty')";
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "shot_map",
            query: shotQuery,
            title: `${randomShots.team} shots`,
            subtitle: `Match ID ${matchId}`
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Here are all shots for ${randomShots.team} in match ${matchId}.`,
          image
        );
        setLastMatchContext(memory, { match_id: matchId, teams: { team_a: randomShots.team } });
        return;
      }

      const lastShotsCompare = parseLastShotsComparePrompt(lastQuestion);
      if (lastShotsCompare) {
        const safeA = lastShotsCompare.team_a.replace(/'/g, "''");
        const safeB = lastShotsCompare.team_b.replace(/'/g, "''");
        const limit = Number(lastShotsCompare.limit) || 100;
        const shotQuery =
          "with ranked as (" +
          "select team_name, x, y, event_name, date_time, " +
          "row_number() over (partition by team_name order by date_time desc) as rn " +
          "from viz_match_events_with_match " +
          "where event_name in ('Shoot','Shoot Location','Penalty') " +
          "and (team_name ilike '%" +
          safeA +
          "%' or team_name ilike '%" +
          safeB +
          "%')" +
          ") " +
          "select team_name, x, y, event_name from ranked where rn <= " +
          limit;
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "shot_map",
            query: shotQuery,
            series_split_field: "team_name",
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Shot map comparing last ${limit} shots for ${lastShotsCompare.team_a} vs ${lastShotsCompare.team_b}.`,
          image
        );
        return;
      }

      const shotCompare = parseShotMapComparePrompt(lastQuestion);
      if (shotCompare) {
        const image = await renderMplSoccerAndLearn(
          { chart_type: "shot_map", template: "shots_by_team", template_vars: shotCompare },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Shot map comparing ${shotCompare.team_a} and ${shotCompare.team_b}.`,
          image
        );
        return;
      }

      const heatCompare = parseHeatmapComparePrompt(lastQuestion);
      if (heatCompare) {
        const image = await renderMplSoccerAndLearn(
          { chart_type: "heatmap", template: "heatmap_events_by_team", template_vars: heatCompare },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Heatmap comparing ${heatCompare.team_a} and ${heatCompare.team_b}.`,
          image
        );
        return;
      }

      const passCompare = parsePassMapComparePrompt(lastQuestion);
      if (passCompare) {
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "pass_map",
            template: "passes_success_by_team",
            template_vars: passCompare
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Pass map comparing ${passCompare.team_a} and ${passCompare.team_b}.`,
          image
        );
        return;
      }

      const shotTeam = parseShotMapTeamPrompt(lastQuestion);
      if (shotTeam) {
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "shot_map",
            template: "shots_by_team",
            template_vars: { team_a: shotTeam.team, team_b: shotTeam.team },
            orientation: shotTeam.orientation,
            half: shotTeam.half
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(res, `Shot map for ${shotTeam.team}.`, image);
        return;
      }

      const pitchPlotTeam = parsePitchPlotTeamPrompt(lastQuestion);
      if (pitchPlotTeam || /pitch plot/i.test(lastQuestion)) {
        const team =
          findKnownTeam(pitchPlotTeam?.team || lastQuestion, memory) ||
          pitchPlotTeam?.team ||
          findKnownTeam(lastQuestion, memory);
        if (team) {
          const kind =
            pitchPlotTeam?.kind ||
            (/shot/i.test(lastQuestion) ? "shot" : "pass");
          const template = kind === "shot" ? "shots_by_team" : "passes_success_by_team";
          const image = await renderMplSoccerAndLearn(
            {
              chart_type: "pitch_plot",
              template,
              template_vars: { team_a: team, team_b: team },
              orientation: pitchPlotTeam?.orientation || parseOrientation(lastQuestion),
              half: pitchPlotTeam?.half ?? parseHalfPitch(lastQuestion)
            },
            env,
            lastQuestionRaw,
            memory
          );
          sendAssistantReply(res, `Pitch plot for ${team}.`, image);
          return;
        }
      }

      const pizzaReq = parsePizzaPrompt(lastQuestion);
      if (pizzaReq) {
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "pizza",
            template: "metrics_player_summary",
            template_vars: pizzaReq
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(res, `Pizza chart for ${pizzaReq.player}.`, image);
        return;
      }

      const radarReq = parseRadarPrompt(lastQuestion);
      if (radarReq) {
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "radar",
            template: "metrics_two_players",
            template_vars: radarReq
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(
          res,
          `Radar chart comparing ${radarReq.player_a} vs ${radarReq.player_b}.`,
          image
        );
        return;
      }

      const bumpyReq = parseBumpyPrompt(lastQuestion);
      if (bumpyReq) {
        const image = await renderMplSoccerAndLearn(
          {
            chart_type: "bumpy",
            template: "bumpy_top_scorers_by_season",
            template_vars: {}
          },
          env,
          lastQuestionRaw,
          memory
        );
        sendAssistantReply(res, "Bumpy chart of top scorers by season.", image);
        return;
      }

      const conceded = parseGoalsConcededPrompt(lastQuestion);
      if (conceded) {
        const safeTeam = conceded.team.replace(/'/g, "''");
        const safeSeason = conceded.season.replace(/'/g, "''");
        const baseQuery =
          "select sum(case when m.home_team_id = t.id then m.away_score else m.home_score end) as conceded " +
          "from matches m join teams t on t.id = m.home_team_id or t.id = m.away_team_id " +
          "where m.season_name = '" +
          safeSeason +
          "' and t.name ilike '%" +
          safeTeam +
          "%'";
        let result = await runSqlRpc(baseQuery, env);
        let concededGoals = result.data?.[0]?.conceded;
        if (concededGoals == null) {
          const looseTeam = safeTeam.replace(/\s+/g, "%");
          const fallbackQuery =
            "select sum(case when m.home_team_id = t.id then m.away_score else m.home_score end) as conceded " +
            "from matches m join teams t on t.id = m.home_team_id or t.id = m.away_team_id " +
            "where m.season_name = '" +
            safeSeason +
            "' and t.name ilike '%" +
            looseTeam +
            "%'";
          result = await runSqlRpc(fallbackQuery, env);
          concededGoals = result.data?.[0]?.conceded;
        }
        if (concededGoals == null) {
          const fuzzy = fuzzyLikePattern(safeTeam);
          const fuzzyQuery =
            "select sum(case when m.home_team_id = t.id then m.away_score else m.home_score end) as conceded " +
            "from matches m join teams t on t.id = m.home_team_id or t.id = m.away_team_id " +
            "where m.season_name = '" +
            safeSeason +
            "' and t.name ilike '%" +
            fuzzy +
            "%'";
          result = await runSqlRpc(fuzzyQuery, env);
          concededGoals = result.data?.[0]?.conceded;
        }
        const concededFinal = concededGoals == null ? 0 : concededGoals;
        sendAssistantReply(
          res,
          `${conceded.team} conceded ${concededFinal} goals in the ${conceded.season} season.`
        );
        return;
      }

      const entity = extractEntityCandidate(lastQuestionRaw);
      if (entity) {
        setLastEntity(memory, entity);
      }
      if (pronounRef && lastEntity && !entity) {
        setLastEntity(memory, lastEntity);
      }
      if (!skipAliasPrompt && entity && !memory.aliases?.[entity]) {
        savePending({ kind: "alias", key: entity, original: lastQuestionRaw });
        sendAssistantReply(
          res,
          `When you say \"${entity}\", is that a team, a player's name, or a nickname?`
        );
        return;
      }
      const directAnswer =
        (await answerPassComparison(lastQuestion, env)) ||
        (await answerGoalsQuestion(lastQuestion, env));
      if (directAnswer) {
        sendAssistantReply(res, directAnswer);
        return;
      }
      let messages = [mcpSystem, ...(payload.messages || [])];
      if (actionPlan?.summary) {
        messages = [
          ...messages,
          {
            role: "system",
            content: `ACTION PLAN: ${actionPlan.summary}. If intent is database, you must call run_sql_rpc. If intent is visual, you must call render_mplsoccer.`,
          },
        ];
      }
      const forceToolName = actionPlan?.forceTool || null;
      const forceVisualizationTool = forceToolName === "render_mplsoccer";

      let response = await callOpenRouter(
        {
          model: payload.model,
          temperature: payload.temperature,
          max_tokens: payload.max_tokens,
          stream: false,
          messages,
          tools,
          tool_choice: forceToolName
            ? { type: "function", function: { name: forceToolName } }
            : "auto",
        },
        apiKey
      );

      let imageAttachment = null;
      let visualizationHandled = false;
      let safetyCounter = 0;
      while (response?.choices?.[0]?.message?.tool_calls && safetyCounter < 6) {
        const toolCalls = response.choices[0].message.tool_calls;
        messages = [...messages, response.choices[0].message];

        const schema = await fetchSchema(env);

        for (const call of toolCalls) {
          let toolResult = { error: "Unsupported tool." };
          let args = {};
          try {
            args = JSON.parse(call.function?.arguments || "{}");
          } catch (error) {
            args = {};
          }

          if (call.function?.name === "get_schema") {
            if (DEBUG_TOOLS) console.log("[tools] get_schema", args?.table || "all");
            toolResult = getSchemaResult(args, schema);
          } else if (call.function?.name === "get_semantic_hints") {
            if (DEBUG_TOOLS) console.log("[tools] get_semantic_hints");
            toolResult = await getSemanticHints(env);
          } else if (call.function?.name === "query_public_table") {
            toolResult = await queryPublicTable(args, env, schema);
          } else if (call.function?.name === "count_public_table") {
            toolResult = await countPublicTable(args, env, schema);
          } else if (call.function?.name === "aggregate_public_table") {
            toolResult = await aggregatePublicTable(args, env, schema);
          } else if (call.function?.name === "run_sql") {
            toolResult = await executeSqlQuery(args?.query || "", env, schema);
          } else if (call.function?.name === "run_sql_rpc") {
            toolResult = await runSqlRpc(args?.query || "", env);
          } else if (call.function?.name === "render_mplsoccer") {
            if (!args.template && !args.query) {
              const shotCompare = parseShotMapComparePrompt(lastQuestion);
              const heatCompare = parseHeatmapComparePrompt(lastQuestion);
              const passCompare = parsePassMapComparePrompt(lastQuestion);
              const shotPlayer = parseShotMapPlayerPrompt(lastQuestion);
              const passPlayer = parsePassMapPlayerPrompt(lastQuestion);
              const heatPlayer = parseHeatmapPlayerPrompt(lastQuestion);
              const comparePlayers = parsePlayerCompare(lastQuestion);
              const singlePlayer = parseSinglePlayer(lastQuestion);

              if (shotCompare) {
                args.template = "shots_by_team";
                args.template_vars = shotCompare;
              } else if (passCompare) {
                args.template = "passes_success_by_team";
                args.template_vars = passCompare;
              } else if (heatCompare) {
                args.template = "heatmap_events_by_team";
                args.template_vars = heatCompare;
              } else if (shotPlayer) {
                args.template = "shot_map_by_player";
                args.template_vars = shotPlayer;
              } else if (passPlayer) {
                args.template = "pass_map_by_player";
                args.template_vars = passPlayer;
              } else if (heatPlayer) {
                args.template = "heatmap_by_player";
                args.template_vars = heatPlayer;
              } else if (args.chart_type === "pizza" && singlePlayer) {
                args.template = "metrics_player_summary";
                args.template_vars = singlePlayer;
              } else if (args.chart_type === "radar" && comparePlayers) {
                args.template = "metrics_two_players";
                args.template_vars = comparePlayers;
              } else if (args.chart_type === "bumpy") {
                args.template = "bumpy_top_scorers_by_season";
                args.template_vars = {};
              }
            }
            if (!args.template && args.query && /conceded|concede/i.test(lastQuestionRaw)) {
              const params = extractParamsFromPrompt(lastQuestionRaw, memory);
              args.query = ensureConcededQuery(args.query, params.team);
            }
            if (!args.series_split_field && detectMultiTeamPrompt(lastQuestionRaw || "")) {
              args.series_split_field = "team_name";
            }
            if (args.series_split_field === "team_name" && args.query) {
              args.query = ensureTeamNameInQuery(args.query);
            }
            if (args.query) {
              args.query = ensureSeasonNameQuery(args.query);
            }
            applyVisualOverrides(args, lastQuestionRaw);
            toolResult = await renderMplSoccer(args, env);
            if (toolResult?.error) {
              sendAssistantReply(res, `Visualization failed: ${toolResult.error}`);
              return;
            }
            if (toolResult?.image_base64) {
              const analysis = await analyzeVisualization(lastQuestionRaw, toolResult, apiKey);
              if (analysis) {
                toolResult.analysis_text = analysis;
              }
              imageAttachment = toolResult;
              visualizationHandled = true;
              maybeLearnTemplateFromArgs(args, lastQuestionRaw, memory);
            }
          }

          messages.push({
            role: "tool",
            tool_call_id: call.id,
            content: JSON.stringify(toolResult),
          });
        }

        if (visualizationHandled) {
          const rowsText = imageAttachment?.row_count
            ? `Used ${imageAttachment.row_count} rows for the plot.`
            : "Plot generated from query results.";
          sendAssistantReply(
            res,
            `Visualization ready. ${rowsText} See the image above.`,
            imageAttachment
          );
          return;
        }

        response = await callOpenRouter(
          {
            model: payload.model,
            temperature: payload.temperature,
            max_tokens: payload.max_tokens,
            stream: false,
            messages,
            tools,
            tool_choice: forceVisualizationTool
              ? { type: "function", function: { name: "render_mplsoccer" } }
              : "auto",
          },
          apiKey
        );

        const toolCallsNext = response?.choices?.[0]?.message?.tool_calls;
        if (!toolCallsNext?.length && !visualizationHandled && isVisualizationPrompt(lastQuestionRaw)) {
          response = await callOpenRouter(
            {
              model: payload.model,
              temperature: payload.temperature,
              max_tokens: payload.max_tokens,
              stream: false,
              messages,
              tools,
              tool_choice: { type: "function", function: { name: "render_mplsoccer" } }
            },
            apiKey
          );
        }

        safetyCounter += 1;
      }

      const finalResponse = imageAttachment ? { ...response, image: imageAttachment } : response;
      sendJson(res, 200, finalResponse);
    } catch (error) {
      sendJson(res, 500, { error: error.message || "Proxy error" });
    }
  });
}

const server = http.createServer((req, res) => {
  if (req.url.startsWith("/api/health")) {
    handleHealth(req, res).catch((error) =>
      sendJson(res, 500, { status: "error", error: error.message || String(error) })
    );
    return;
  }

  if (req.url.startsWith("/api/elevenlabs/conversation-token")) {
    handleElevenLabsToken(req, res);
    return;
  }

  if (req.url.startsWith("/api/voice_tool")) {
    if (req.method !== "POST") {
      sendJson(res, 405, { error: "Method not allowed" });
      return;
    }
    handleVoiceTool(req, res);
    return;
  }

  if (req.url.startsWith("/api/voice_events")) {
    if (req.method !== "GET") {
      sendJson(res, 405, { error: "Method not allowed" });
      return;
    }
    handleVoiceEvents(req, res);
    return;
  }

  if (req.url.startsWith("/api/schema")) {
    const env = parseEnvFile();
    fetchSchema(env)
      .then((schema) => sendJson(res, 200, schema))
      .catch((error) => sendJson(res, 500, { error: error.message || "Schema error" }));
    return;
  }

  if (req.url.startsWith("/api/chat")) {
    if (req.method !== "POST") {
      sendJson(res, 405, { error: "Method not allowed" });
      return;
    }
    proxyChat(req, res);
    return;
  }

  serveFile(req, res);
});

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
  refreshSchemaCacheOnStartup();
});
