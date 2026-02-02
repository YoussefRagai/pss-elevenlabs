#!/usr/bin/env bash
set -euo pipefail

MPLSOCCER_PORT="${MPLSOCCER_PORT:-8001}"
if [[ "${PORT:-}" == "${MPLSOCCER_PORT}" ]]; then
  MPLSOCCER_PORT="8002"
fi

export MPLSOCCER_URL="${MPLSOCCER_URL:-http://127.0.0.1:${MPLSOCCER_PORT}/render}"

python3 -m uvicorn viz_server:app --host 0.0.0.0 --port "${MPLSOCCER_PORT}" &
exec node server.js
