#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/tmp/monitor/logs"
LOG_FILE="$LOG_DIR/gateway.log"
PID_FILE="$ROOT_DIR/tmp/monitor/gateway.pid"

mkdir -p "$LOG_DIR" "$ROOT_DIR/tmp/monitor/alerts" "$ROOT_DIR/tmp/monitor/state" "$ROOT_DIR/tmp/monitor/spool"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${PID:-}" ]] && kill -0 "$PID" 2>/dev/null; then
    echo "monitor gateway already running, pid=$PID"
    exit 0
  fi
fi

nohup node "$ROOT_DIR/monitor/gateway/gateway.mjs" >>"$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"

echo "monitor gateway started, pid=$NEW_PID"
echo "log file: $LOG_FILE"
