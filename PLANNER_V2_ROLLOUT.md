# Planner V2 Rollout

## Feature flags

- `PLANNER_V2_ENABLED=true|false`
- `PLANNER_V2_SHADOW=true|false`
- `PLANNER_V2_ROLLOUT_PERCENT=0..100`
- `PLANNER_V2_MIN_REQUESTS_FOR_GUARD=20`
- `PLANNER_V2_MAX_FAIL_RATE=0.25`

## Endpoints

- Health: `GET /api/health`
- Planner status: `GET /api/planner_v2/status`
- Trace summary: `GET /api/trace?limit=50`

## Canary progression

1. **Shadow only**
   - `PLANNER_V2_ENABLED=false`
   - `PLANNER_V2_SHADOW=true`
2. **5%**
   - `PLANNER_V2_ENABLED=true`
   - `PLANNER_V2_SHADOW=true`
   - `PLANNER_V2_ROLLOUT_PERCENT=5`
3. **25%**
   - increase `PLANNER_V2_ROLLOUT_PERCENT=25`
4. **50%**
   - increase `PLANNER_V2_ROLLOUT_PERCENT=50`
5. **100%**
   - increase `PLANNER_V2_ROLLOUT_PERCENT=100`
   - optionally disable shadow.

## Auto-fallback behavior

Planner v2 automatically falls back to legacy orchestration when:

- plan JSON invalid,
- planner generation errors,
- resolver/probe failures in execution path.

Additionally, guardrail auto-disables planner rollout (in-process override) if observed fail rate exceeds:

- `failed/total > PLANNER_V2_MAX_FAIL_RATE`
- after `PLANNER_V2_MIN_REQUESTS_FOR_GUARD` requests.

Check current guard state using:

```bash
curl -s http://127.0.0.1:8080/api/planner_v2/status | jq
```
