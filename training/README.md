# Planner V2 Fine-tuning Assets

This folder contains scaffolding to train and evaluate planner-focused fine-tunes (`plan_v1` output).

## Files

- `build_planner_dataset.py`: builds planner training/validation JSONL from prompts + canonical playbook mappings.
- `build_planner_dataset_from_trace.py`: builds planner JSONL from `trace.log` prompts + canonical playbook mappings.
- `eval_planner.py`: evaluates `plan_v1` validity, intent/context intent accuracy, and tool-count policy.
- `eval_strict_suite.py`: sends benchmark prompts to live `/api/chat` and records strict runtime metrics.
- `run_ab_finetune.sh`: runs A/B training jobs (fresh adapter and continued adapter).
- `../scripts/planner_v2_shadow_report.py`: summarizes shadow-mode outcomes from `trace.log`.

## Expected outputs

- `training/out/planner_train.jsonl`
- `training/out/planner_val.jsonl`
- `training/out/planner_eval_report.json`
- `training/out/strict_eval_report.json`
- `training/out/planner_v2_shadow_report.json`

## Suggested flow

1. Enable shadow mode in API:
   - `PLANNER_V2_ENABLED=false`
   - `PLANNER_V2_SHADOW=true`
2. Collect traffic and build dataset from trace:
   - `python training/build_planner_dataset_from_trace.py`
3. Run A/B:
   - `bash training/run_ab_finetune.sh`
4. Run strict runtime suite:
   - `python training/eval_strict_suite.py`
5. Build shadow report:
   - `python scripts/planner_v2_shadow_report.py`
