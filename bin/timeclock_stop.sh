#!/bin/sh
set -eu

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

PID_FILE="data/uvicorn.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "Not running (no pid file)"
  exit 0
fi

PID="$(cat "$PID_FILE" 2>/dev/null || true)"
if [ -z "${PID:-}" ]; then
  rm -f "$PID_FILE"
  echo "Not running (empty pid file)"
  exit 0
fi

if kill -0 "$PID" 2>/dev/null; then
  kill "$PID" 2>/dev/null || true
  sleep 1
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
  fi
fi

rm -f "$PID_FILE"
echo "Stopped"

