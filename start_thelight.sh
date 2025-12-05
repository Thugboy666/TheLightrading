#!/data/data/com.termux/files/usr/bin/sh
# Start TheLightrading backend server

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

LOG_FILE="$SCRIPT_DIR/thelightrading.log"
PID_FILE="$SCRIPT_DIR/thelightrading.pid"

# Prevent duplicate starts
if [ -f "$PID_FILE" ]; then
    PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "${PID:-}" ] && kill -0 "$PID" 2>/dev/null; then
        echo "TheLightrading server already running with PID $PID"
        exit 0
    fi
    rm -f "$PID_FILE"
fi

if [ -d "./venv" ]; then
    . ./venv/bin/activate
elif [ -d "./.venv" ]; then
    . ./.venv/bin/activate
fi

PORT="8090"
nohup python3 api/server.py "$PORT" > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "TheLightrading server started on port $PORT (PID $(cat "$PID_FILE"))"
