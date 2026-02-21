#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TRAIN_JSONL="$ROOT/training/out/planner_train.jsonl"
VAL_JSONL="$ROOT/training/out/planner_val.jsonl"
MODEL_NAME="${MODEL_NAME:-Qwen/Qwen3-8B-Base}"
TINKER_SCRIPT="${TINKER_SCRIPT:-/Users/youssefragai/Desktop/TrainingDB/tinker_train_football.py}"
RESUME_CHECKPOINT="${RESUME_CHECKPOINT:-}"

echo "[1/3] Build planner dataset"
python "$ROOT/training/build_planner_dataset_from_trace.py"

echo "[2/4] Run A (fresh adapter)"
python "$TINKER_SCRIPT" \
  model_name="$MODEL_NAME" \
  train_file="$TRAIN_JSONL" \
  val_file="$VAL_JSONL" \
  learning_rate=2e-4 \
  batch_size=16 \
  lora_rank=64 \
  num_epochs=1 \
  eval_every=100 \
  save_every=100 \
  behavior_if_log_dir_exists=delete \
  wandb_project=pss_planner_v2_ab || true

if [[ -n "$RESUME_CHECKPOINT" ]]; then
  echo "[3/4] Run B (continue adapter)"
  python "$TINKER_SCRIPT" \
    model_name="$MODEL_NAME" \
    train_file="$TRAIN_JSONL" \
    val_file="$VAL_JSONL" \
    load_checkpoint_path="$RESUME_CHECKPOINT" \
    learning_rate=1e-4 \
    batch_size=16 \
    lora_rank=64 \
    num_epochs=1 \
    eval_every=100 \
    save_every=100 \
    behavior_if_log_dir_exists=delete \
    wandb_project=pss_planner_v2_ab || true
else
  echo "[3/4] Skipping Run B (set RESUME_CHECKPOINT to enable continued training)"
fi

echo "[4/4] Evaluate planner outputs"
python "$ROOT/training/eval_planner.py"
