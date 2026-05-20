#!/bin/bash
SKIP=$(curl -sf "http://metadata.google.internal/computeMetadata/v1/instance/attributes/skip_job" -H "Metadata-Flavor: Google" 2>/dev/null)
if [ "$SKIP" = "true" ]; then
  echo "skip_job=true, exiting startup script."
  exit 0
fi

LOG=/var/log/pipeline_run_$(date +%Y%m%d_%H%M%S).log
exec > "$LOG" 2>&1

echo "=== Pipeline started at $(date) ==="

cd babyview-pipeline/
source venv/bin/activate

tmux new-session -d -s pipeline "source venv/bin/activate && python main.py; tmux wait-for -S done"
tmux wait-for done

echo "=== Pipeline finished at $(date) ==="

shutdown -h now