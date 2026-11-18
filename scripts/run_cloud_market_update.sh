#!/usr/bin/env bash
set -euo pipefail

if [[ "${DRY_RUN:-true}" == "true" ]]; then
  exec python manual-system-f1-th-ver2.py --dry-run
fi

python manual-system-f1-th-ver2.py
python scripts/check_gsheets_dates.py --market-only --wait-seconds 180 --poll-seconds 30
