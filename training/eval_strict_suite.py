#!/usr/bin/env python3
import json
import os
import re
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "training" / "out" / "strict_eval_report.json"
API_URL = os.getenv("EVAL_CHAT_URL", "http://127.0.0.1:8080/api/chat")

CASES = [
    {"prompt": "Show me the weaknesses of Team X in the last 5 matches.", "expect_context": "weakness"},
    {"prompt": "Compare pressing profile of Team A vs Team B.", "expect_context": "press"},
    {"prompt": "How does Team X defend transitions?", "expect_context": "transition"},
    {"prompt": "Give me a visual for Team X chance creation.", "expect_context": "chance"},
]


def post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def has_vis(text: str) -> bool:
    return "VIS_RECOMMENDATION:" in (text or "")


def main():
    results = []
    for case in CASES:
        payload = {"model": "openai/gpt-4o-mini", "messages": [{"role": "user", "content": case["prompt"]}]}
        try:
            data = post_json(API_URL, payload)
            text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            tool_calls = re.findall(r"tool_", json.dumps(data))
            results.append(
                {
                    "prompt": case["prompt"],
                    "ok": True,
                    "has_vis": has_vis(text),
                    "has_image": bool(data.get("image", {}).get("image_base64")),
                    "tool_calls_detected": len(tool_calls),
                    "response": text[:400],
                }
            )
        except Exception as error:
            results.append({"prompt": case["prompt"], "ok": False, "error": str(error)})

    total = len(results)
    ok = [r for r in results if r.get("ok")]
    report = {
        "total": total,
        "ok_rate": (len(ok) / total) if total else 0,
        "vis_rate": (sum(1 for r in ok if r.get("has_vis")) / len(ok)) if ok else 0,
        "image_rate": (sum(1 for r in ok if r.get("has_image")) / len(ok)) if ok else 0,
        "results": results,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
