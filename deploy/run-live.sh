#!/usr/bin/env bash
set -euo pipefail
task_repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$task_repo_dir"
: "${LIGHTER_ACCOUNT_INDEX:?Set the account index}"
: "${LIGHTER_API_KEY_INDEX:?Set the API key index}"
: "${LIGHTER_SIGNING_KEY_FILE:?Set a protected local signing-key file path}"
: "${APEX_REPORT:?Set the real evaluation report path}"
: "${APEX_MODEL:?Set the corresponding model path}"
: "${APEX_LATENCY:?Set the measured latency path}"
task_config="${APEX_CONFIG:-config/default.toml}"
task_stage="${APEX_STAGE:-production}"
task_registry="${APEX_REGISTRY:-artifacts/registry.json}"
.venv/bin/apex --config "$task_config" auth-check
.venv/bin/apex --config "$task_config" recover --timeout 120
exec .venv/bin/apex --config "$task_config" live --report "$APEX_REPORT" \
  --model "$APEX_MODEL" --latency "$APEX_LATENCY" --stage "$task_stage" --registry "$task_registry"
