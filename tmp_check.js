const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '.env');
const text = fs.readFileSync(envPath, 'utf8');
const env = {};
for (const raw of text.split('\n')) {
  const line = raw.trim();
  if (!line || line.startsWith('#') || !line.includes('=')) continue;
  const [key, ...rest] = line.split('=');
  env[key.trim()] = rest.join('=').trim().replace(/^"|"$/g, '');
}
const safeA = "Al Ahly".replace(/'/g, "''");
const safeB = "Pyramids".replace(/'/g, "''");
const limit = 100;
const query =
  "with ranked as (" +
  "select team_name, x, y, event_name, date_time, " +
  "row_number() over (partition by team_name order by date_time desc) as rn " +
  "from viz_match_events_with_match " +
  "where event_name in ('Shoot','Shoot Location','Penalty') " +
  "and (team_name ilike '%" + safeA + "%' or team_name ilike '%" + safeB + "%')" +
  ") " +
  "select team_name, x, y, event_name from ranked where rn <= " + limit;

fetch(env.SUPABASE_URL + "/rest/v1/rpc/run_sql_readonly", {
  method: 'POST',
  headers: {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({ query })
})
  .then(async (r) => {
    const d = await r.json().catch(() => null);
    console.log('ok', r.ok, 'rows', Array.isArray(d) ? d.length : d);
    if (Array.isArray(d)) {
      const teams = new Set();
      d.forEach((row) => teams.add(row.team_name));
      console.log('teams', Array.from(teams));
      console.log('sample', d.slice(0, 3));
    } else {
      console.log('payload', d);
    }
  })
  .catch((e) => console.error(e));
