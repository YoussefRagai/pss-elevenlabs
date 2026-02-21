#!/usr/bin/env python3
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TRACE = ROOT / "trace.log"
OUT = ROOT / "training" / "out" / "planner_v2_shadow_report.json"


def parse_trace_line(line: str):
    # format: "<id> <ts> <stage> <json-details>"
    parts = line.strip().split(" ", 3)
    if len(parts) < 4:
        return None
    req_id, ts, stage, details_raw = parts
    try:
        details = json.loads(details_raw)
    except Exception:
        details = {}
    return req_id, ts, stage, details


def main():
    per_req = defaultdict(list)
    if TRACE.exists():
        for line in TRACE.read_text(encoding="utf-8", errors="ignore").splitlines():
            parsed = parse_trace_line(line)
            if not parsed:
                continue
            req_id, ts, stage, details = parsed
            per_req[req_id].append({"ts": ts, "stage": stage, "details": details})

    total = 0
    planner_attempts = 0
    planner_success = 0
    planner_fallback = 0
    avg_tool_calls = []
    avg_resolution_conf = []
    for req_id, events in per_req.items():
        total += 1
        by_stage = {e["stage"]: e for e in events}
        if "planner_v2_compiled" in by_stage:
            planner_attempts += 1
            avg_tool_calls.append(by_stage["planner_v2_compiled"]["details"].get("tool_calls", 0))
        if "planner_v2_response_sent" in by_stage:
            planner_success += 1
        if "planner_v2_fallback" in by_stage:
            planner_fallback += 1
        if "entity_resolution_v2" in by_stage:
            avg_resolution_conf.append(by_stage["entity_resolution_v2"]["details"].get("confidence", 0))

    report = {
        "requests_seen": total,
        "planner_attempts": planner_attempts,
        "planner_success": planner_success,
        "planner_fallback": planner_fallback,
        "planner_success_rate": (planner_success / planner_attempts) if planner_attempts else 0,
        "avg_tool_calls": (sum(avg_tool_calls) / len(avg_tool_calls)) if avg_tool_calls else 0,
        "avg_resolution_confidence": (sum(avg_resolution_conf) / len(avg_resolution_conf))
        if avg_resolution_conf
        else 0,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
