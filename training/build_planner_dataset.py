#!/usr/bin/env python3
import json
from pathlib import Path
from random import Random

ROOT = Path(__file__).resolve().parents[1]
PLAYBOOK = ROOT / "context_playbook.json"
OUT_DIR = ROOT / "training" / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SYSTEM = (
    "You are planner v1 for football analytics. "
    "Output strict JSON only with version=plan_v1."
)


def mk_row(user_prompt: str, context_intent: str, chart_type: str, metrics: list[str], entity_key="team"):
    plan = {
        "version": "plan_v1",
        "intent": "database",
        "context_intent": context_intent,
        "entities": {
            "team": "Team X" if entity_key == "team" else None,
            "team_a": "Team A" if entity_key == "compare" else None,
            "team_b": "Team B" if entity_key == "compare" else None,
            "player": None,
            "season": "2023/2024",
            "match_scope": "last_n",
        },
        "metrics": metrics,
        "tool_sequence": [
            {"tool": "run_sql_rpc", "purpose": "retrieve core metrics", "args_template": {"query": "SELECT ..."}},
            {
                "tool": "render_mplsoccer",
                "purpose": "render recommendation",
                "args_template": {"chart_type": chart_type},
            },
        ],
        "vis_recommendation": {"chart_type": chart_type, "reason": "Best fit for tactical pattern visibility."},
        "confidence": 0.9,
        "assumptions": ["season defaults to 2023/2024"],
        "fallback_if_empty": "relax filters and retry with last_n=10",
    }
    return {
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user_prompt},
            {"role": "assistant", "content": json.dumps(plan, ensure_ascii=False)},
        ]
    }


def main():
    pb = json.loads(PLAYBOOK.read_text(encoding="utf-8"))
    rows = []
    prompts = {
        "weakness_profile": [
            "Show me the weaknesses of Team X in their last 5 matches.",
            "Where is Team X vulnerable defensively this season?",
        ],
        "pressing_profile": [
            "Analyze Team X pressing pattern and suggest visualization.",
            "How effective is Team X pressing in the final third?",
        ],
        "transition_defense": [
            "How does Team X defend transitions?",
            "Show transition defense issues for Team X.",
        ],
    }
    for intent, meta in pb.get("intents", {}).items():
        for p in prompts.get(intent, []):
            rows.append(mk_row(p, intent, meta.get("default_chart_type", "heatmap"), meta.get("metrics", [])))

    rng = Random(42)
    rng.shuffle(rows)
    split = max(1, int(len(rows) * 0.8))
    train, val = rows[:split], rows[split:]

    for path, data in ((OUT_DIR / "planner_train.jsonl", train), (OUT_DIR / "planner_val.jsonl", val)):
        with path.open("w", encoding="utf-8") as f:
            for row in data:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(json.dumps({"train": len(train), "val": len(val)}, indent=2))


if __name__ == "__main__":
    main()
