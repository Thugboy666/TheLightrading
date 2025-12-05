#!/data/data/com.termux/files/usr/bin/sh

BASEDIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASEDIR"

PID_FILE_API="thelightrading_api.pid"
PID_FILE_LLM="thelightrading_llm.pid"

echo "----- 🛑 STOP THELIGHTRADING -----"
echo "BASEDIR = $BASEDIR"

# Stop API
if [ -f "$PID_FILE_API" ]; then
    PID_API="$(cat "$PID_FILE_API" 2>/dev/null)"
    if [ -n "$PID_API" ] && kill "$PID_API" 2>/dev/null; then
        echo "✅ Arrestata API TheLightrading (PID $PID_API)."
    else
        echo "⚠ PID API ($PID_API) non valido o processo già morto."
    fi
    rm -f "$PID_FILE_API"
else
    echo "ℹ Nessun file PID API trovato, provo pkill su api.server 8090..."
    pkill -f "api.server 8090" 2>/dev/null && echo "✅ Arrestata API via pkill."
fi

# Stop LLM
if [ -f "$PID_FILE_LLM" ]; then
    PID_LLM="$(cat "$PID_FILE_LLM" 2>/dev/null)"
    if [ -n "$PID_LLM" ] && kill "$PID_LLM" 2>/dev/null; then
        echo "✅ Arrestato LLM (PID $PID_LLM)."
    else
        echo "⚠ PID LLM ($PID_LLM) non valido o processo già morto."
    fi
    rm -f "$PID_FILE_LLM"
else
    echo "ℹ Nessun file PID LLM trovato, provo pkill su llama-server..."
    pkill -f "llama-server" 2>/dev/null && echo "✅ Arrestato LLM via pkill."
fi
