#!/usr/bin/env bash
set -euo pipefail

python3 -m uvicorn viz_server:app --host 0.0.0.0 --port 8001 &
node server.js
