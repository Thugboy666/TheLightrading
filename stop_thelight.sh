#!/data/data/com.termux/files/usr/bin/sh
# Stop TheLightrading backend server

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="$SCRIPT_DIR/thelightrading.pid"

if [ -f "$PID_FILE" ]; then
    PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "${PID:-}" ] && kill "$PID" 2>/dev/null; then
        echo "Stopped TheLightrading server (PID $PID)"
    else
        echo "No running TheLightrading process for PID $PID"
    fi
    rm -f "$PID_FILE"
    exit 0
fi

if pkill -f "api/server.py 8090" 2>/dev/null || pkill -f "api/server.py" 2>/dev/null; then
    echo "Stopped TheLightrading server using fallback search"
else
    echo "No TheLightrading server process found"
fi
