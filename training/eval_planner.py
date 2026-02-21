#!/usr/bin/env python3
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "training" / "out" / "planner_val.jsonl"
OUT = ROOT / "training" / "out" / "planner_eval_report.json"


def parse_assistant_json(messages):
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            try:
                return json.loads(msg.get("content", ""))
            except Exception:
                return None
    return None


def main():
    rows = []
    with INPUT.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    required = {
        "version",
        "intent",
        "context_intent",
        "entities",
        "metrics",
        "tool_sequence",
        "vis_recommendation",
        "confidence",
        "assumptions",
        "fallback_if_empty",
    }
    valid = 0
    tool_budget_ok = 0
    for row in rows:
        plan = parse_assistant_json(row.get("messages", []))
        if not isinstance(plan, dict):
            continue
        if required.issubset(set(plan.keys())) and plan.get("version") == "plan_v1":
            valid += 1
        steps = plan.get("tool_sequence", [])
        if isinstance(steps, list) and len(steps) <= 4:
            tool_budget_ok += 1

    total = max(1, len(rows))
    report = {
        "total": len(rows),
        "valid_plan_json_rate": valid / total,
        "tool_budget_rate": tool_budget_ok / total,
    }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
