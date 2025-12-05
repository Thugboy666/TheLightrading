#!/data/data/com.termux/files/usr/bin/sh

BASEDIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASEDIR"

PID_FILE="thelightrading.pid"

echo "----- 🛑 STOP THELIGHTRADING -----"

if [ -f "thelightrading.pid" ]; then
    PID=$(cat thelightrading.pid)
    kill "$PID" 2>/dev/null
    rm -f thelightrading.pid
    echo "Stopped TheLightrading server"
else
    pkill -f "api/server.py 8090"
fi
