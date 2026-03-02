#!/usr/bin/env bash
set -euo pipefail

DEFAULT_CMD=(python /app/app.py)

if [[ $# -eq 0 ]]; then
  set -- "${DEFAULT_CMD[@]}"
fi

exec "$@"
