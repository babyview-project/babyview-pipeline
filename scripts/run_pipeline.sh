#!/bin/bash
set -euo pipefail

SKIP=$(curl -sf "http://metadata.google.internal/computeMetadata/v1/instance/attributes/skip_job" \
  -H "Metadata-Flavor: Google" 2>/dev/null || true)
if [ "${SKIP:-}" = "true" ]; then
  echo "skip_job=true, exiting startup script."
  exit 0
fi

LOG=/var/log/pipeline_run_$(date +%Y%m%d_%H%M%S).log
exec >"$LOG" 2>&1

RUN_USER=ezhang61
PROJECT_DIR=/home/${RUN_USER}/babyview-pipeline

echo "=== Pipeline started at $(date) ==="
echo "Orchestrator: $(whoami), pipeline user: ${RUN_USER}, project: ${PROJECT_DIR}"

if [ ! -f "${PROJECT_DIR}/main.py" ]; then
  echo "ERROR: ${PROJECT_DIR}/main.py not found"
  exit 1
fi

VENV_ACTIVATE="${PROJECT_DIR}/.venv/bin/activate"
if [ ! -f "${VENV_ACTIVATE}" ]; then
  VENV_ACTIVATE="${PROJECT_DIR}/venv/bin/activate"
fi
if [ ! -f "${VENV_ACTIVATE}" ]; then
  echo "ERROR: no venv at ${PROJECT_DIR}/.venv or ${PROJECT_DIR}/venv"
  exit 1
fi

# Run as ezhang61 so tmux/logs live under /home/ezhang61 (not root / OS Login home)
# Attach while running: sudo -u ezhang61 tmux attach -t pipeline
sudo -u "${RUN_USER}" bash -c "
  set -euo pipefail
  cd '${PROJECT_DIR}'
  source '${VENV_ACTIVATE}'
  tmux kill-session -t pipeline 2>/dev/null || true
  tmux new-session -d -s pipeline \
    \"cd '${PROJECT_DIR}' && source '${VENV_ACTIVATE}' && python main.py; tmux wait-for -S done\"
  tmux wait-for done
"

echo "=== Pipeline finished at $(date) ==="
shutdown -h now
