#!/bin/sh
set -eu

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

PID_FILE="data/uvicorn.pid"

if [ -f "$PID_FILE" ]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
else
  PID=""
fi

if [ -n "${PID:-}" ] && kill -0 "$PID" 2>/dev/null; then
  echo "RUNNING pid=$PID"
else
  echo "NOT RUNNING"
fi

if command -v curl >/dev/null 2>&1; then
  curl -sS http://127.0.0.1:8010/api/health || true
  echo
fi
