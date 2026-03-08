#!/bin/bash
# codex2api 重启脚本

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$APP_DIR/.server.pid"
LOG_DIR="$APP_DIR/tmp/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/codex2api.log"
PORT=8066

cd "$APP_DIR"

# ——— 杀掉旧进程 ———
pid=""
if [[ -f "$PID_FILE" ]]; then
  pid=$(cat "$PID_FILE")
fi
if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
  pid=$(lsof -ti :"$PORT" 2>/dev/null || true)
fi

if [[ -n "$pid" ]]; then
  echo "⏹ 停止旧进程 (PID: $pid) ..."
  kill $pid 2>/dev/null || true
  # 等最多 5 秒
  for i in 1 2 3 4 5; do
    kill -0 $pid 2>/dev/null || break
    sleep 1
  done
  # 还没死就强杀
  kill -9 $pid 2>/dev/null || true
  sleep 1
fi
rm -f "$PID_FILE"

# ——— 启动新进程 ———
echo "▶ 启动 codex2api ..."
node server.mjs >> "$LOG_FILE" 2>&1 &
new_pid=$!
echo "$new_pid" > "$PID_FILE"

# 等端口就绪（最多120秒）
echo "⏳ 等待端口 $PORT 就绪 ..."
ready=0
for i in $(seq 1 120); do
  if ! kill -0 "$new_pid" 2>/dev/null; then
    echo "❌ 进程已退出 (PID: $new_pid)"
    tail -20 "$LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
  fi
  if ss -tlnp 2>/dev/null | grep -q ":$PORT "; then
    ready=1
    break
  fi
  # 每10秒打一次进度
  if (( i % 10 == 0 )); then
    echo "  ... 已等待 ${i}s，进程存活，端口未就绪"
  fi
  sleep 1
done

if [[ "$ready" -eq 1 ]]; then
  echo "✅ 服务已启动 (PID: $new_pid, 端口: $PORT, 耗时: ${i}s)"
  tail -5 "$LOG_FILE"
else
  echo "❌ 超时120s端口仍未就绪 (PID: $new_pid)"
  tail -20 "$LOG_FILE"
  rm -f "$PID_FILE"
  exit 1
fi

# ——— 启动监控网关 ———
MONITOR_PID_FILE="$APP_DIR/tmp/monitor/gateway.pid"
if [[ -f "$MONITOR_PID_FILE" ]]; then
  old_mon_pid=$(cat "$MONITOR_PID_FILE" 2>/dev/null || true)
  if [[ -n "$old_mon_pid" ]]; then
    kill "$old_mon_pid" 2>/dev/null || true
    sleep 1
  fi
fi
export MONITOR_GOTIFY_URL="${MONITOR_GOTIFY_URL:-https://notify.example.com}"
export MONITOR_GOTIFY_TOKEN="${MONITOR_GOTIFY_TOKEN:-CHANGE_ME_NOTIFY_TOKEN}"
export MONITOR_SERVICE_HOST="${MONITOR_SERVICE_HOST:-127.0.0.1}"
export MONITOR_SERVICE_PORT="${MONITOR_SERVICE_PORT:-$PORT}"
bash "$APP_DIR/monitor/start.sh" 2>/dev/null && echo "✅ 监控网关已启动" || echo "⚠️ 监控网关启动失败（非致命）"
