#!/usr/bin/env python3
import json
import re
from pathlib import Path
from random import Random

ROOT = Path(__file__).resolve().parents[1]
TRACE = ROOT / "trace.log"
PLAYBOOK = ROOT / "context_playbook.json"
OUT_DIR = ROOT / "training" / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SYSTEM = (
    "You are planner v1 for football analytics. "
    "Output strict JSON only with version=plan_v1."
)


def infer_context_intent(prompt: str) -> str:
    p = prompt.lower()
    if any(k in p for k in ["weakness", "vulnerable", "concede", "exploit"]):
        return "weakness_profile"
    if any(k in p for k in ["press", "pressing", "counter-press"]):
        return "pressing_profile"
    if any(k in p for k in ["transition", "counter", "turnover"]):
        return "transition_defense"
    return "weakness_profile"


def infer_intent(prompt: str) -> str:
    p = prompt.lower()
    if any(k in p for k in ["map", "heatmap", "chart", "plot", "visual", "radar", "pizza"]):
        return "visual"
    if any(k in p for k in ["show", "compare", "analyze", "weakness", "pressing", "xg", "shots"]):
        return "database"
    return "general"


def build_plan(prompt: str, playbook: dict) -> dict:
    context_intent = infer_context_intent(prompt)
    cfg = playbook.get("intents", {}).get(context_intent, {})
    intent = infer_intent(prompt)
    chart = cfg.get("default_chart_type", "heatmap")
    metrics = cfg.get("metrics", [])
    match_scope = "last_n" if re.search(r"\blast\s+\d+\b", prompt.lower()) else "season"
    plan = {
        "version": "plan_v1",
        "intent": intent,
        "context_intent": context_intent,
        "entities": {
            "team": "Team X",
            "team_a": None,
            "team_b": None,
            "player": None,
            "season": "2023/2024",
            "match_scope": match_scope,
        },
        "metrics": metrics,
        "tool_sequence": [
            {"tool": "run_sql_rpc", "purpose": "retrieve tactical metrics", "args_template": {"query": "SELECT ..."}},
            {
                "tool": "render_mplsoccer",
                "purpose": "render tactical visualization",
                "args_template": {"chart_type": chart},
            },
        ],
        "vis_recommendation": {
            "chart_type": chart,
            "reason": "Most informative view for this contextual intent.",
        },
        "confidence": 0.88,
        "assumptions": ["season defaults to 2023/2024 when not specified"],
        "fallback_if_empty": "widen scope to last_n=10",
    }
    if "compare" in prompt.lower() or "vs" in prompt.lower():
        plan["entities"]["team"] = None
        plan["entities"]["team_a"] = "Team A"
        plan["entities"]["team_b"] = "Team B"
    return plan


def parse_trace_prompts(trace_path: Path) -> list[str]:
    if not trace_path.exists():
        return []
    prompts = []
    pat = re.compile(r"request_start|prompt_received")
    for line in trace_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not pat.search(line):
            continue
        if "prompt_received" not in line:
            continue
        try:
            payload = json.loads(line.split(" ", 3)[-1])
            prompt = str(payload.get("prompt", "")).strip()
            if prompt:
                prompts.append(prompt)
        except Exception:
            continue
    dedup = []
    seen = set()
    for p in prompts:
        k = p.lower().strip()
        if k in seen:
            continue
        seen.add(k)
        dedup.append(p)
    return dedup


def main():
    playbook = json.loads(PLAYBOOK.read_text(encoding="utf-8"))
    prompts = parse_trace_prompts(TRACE)
    if not prompts:
        prompts = [
            "Show me the weaknesses of Team X in the last 5 matches.",
            "Compare pressing profile of Team A vs Team B this season.",
            "Where does Team X concede dangerous chances?",
            "Analyze Team X transition defense and suggest a chart.",
        ]
    rows = []
    for prompt in prompts:
        plan = build_plan(prompt, playbook)
        rows.append(
            {
                "messages": [
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": json.dumps(plan, ensure_ascii=False)},
                ]
            }
        )

    rng = Random(42)
    rng.shuffle(rows)
    split = max(1, int(0.85 * len(rows)))
    train, val = rows[:split], rows[split:]
    for path, data in ((OUT_DIR / "planner_train.jsonl", train), (OUT_DIR / "planner_val.jsonl", val)):
        with path.open("w", encoding="utf-8") as f:
            for row in data:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(json.dumps({"train": len(train), "val": len(val), "source_prompts": len(prompts)}, indent=2))


if __name__ == "__main__":
    main()
