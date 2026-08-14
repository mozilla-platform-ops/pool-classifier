#!/usr/bin/env bash
# Run read-only classification previews for the sampled non-macOS task runs.
#
# These tasks were previously labelled tc-task-payload-invalid-missing-value;
# that incidental message is no longer a classification rule. The previews
# help identify more useful patterns without writing classifier data.

set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

task_ids=(
  fvZ8DioSQwW8oeLeRLGRxg
  LMmUi5iWQFGNdOqTO1Hqrw
  ARS1Rh2GRk-S2brU_50HQA
  Wfi83l4xTgeFuEmQnLwGeg
  HONvLuwsQjWKvn3sZcudFw
  OnN0rqR0QA6i0swSmij7Bw
  Y_77JcVUSgaIL7rzx0Wpsw
  dNW88YxvTaOOQd66n9ZE8g
  NPaeaN9BSpSL14djeX33jw
  KpULqSZIQkWn-CwSGHhPqw
)

exit_code=0
for task_id in "${task_ids[@]}"; do
  printf '\n===== %s run 0 =====\n' "$task_id"
  if ! uv run python pool_classifier.py --preview-task "$task_id" --preview-run 0; then
    exit_code=1
  fi
done

exit "$exit_code"
