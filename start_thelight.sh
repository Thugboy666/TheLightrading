#!/data/data/com.termux/files/usr/bin/sh

BASEDIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASEDIR"

PID_FILE_API="thelightrading_api.pid"
PID_FILE_LLM="thelightrading_llm.pid"
LOGDIR="$BASEDIR/logs"
LOG_API="$LOGDIR/gui.log"
LOG_LLM="$LOGDIR/llm.log"

echo "----- 🚀 START THELIGHTRADING -----"
echo "BASEDIR = $BASEDIR"

mkdir -p "$LOGDIR"

# Se esiste già un PID API, controlla se è vivo
if [ -f "$PID_FILE_API" ]; then
    OLD_PID_API="$(cat "$PID_FILE_API" 2>/dev/null)"
    if [ -n "$OLD_PID_API" ] && kill -0 "$OLD_PID_API" 2>/dev/null; then
        echo "⚠ TheLightrading API sembra già in esecuzione (PID $OLD_PID_API)."
        echo "   Se non è così, lancia ./stop_theight.sh e poi riprova."
        exit 0
    else
        echo "ℹ Rimosso vecchio PID API non valido ($OLD_PID_API)."
        rm -f "$PID_FILE_API"
    fi
fi

# Attiva virtualenv locale, se presente
if [ -d "./venv" ]; then
    . ./venv/bin/activate
elif [ -d "./.venv" ]; then
    . ./.venv/bin/activate
else
    echo "⚠ Nessuna virtualenv trovata (./venv o ./.venv). Uso python3 di sistema."
fi

# ───────────── LLM (llama-server) su 8081 ─────────────

LLM_BIN="$BASEDIR/llm/llama.cpp/build/bin/llama-server"
MODEL_PATH="$BASEDIR/llm/models/Phi-3-mini-4k-instruct-q4.gguf"

if [ -x "$LLM_BIN" ] && [ -f "$MODEL_PATH" ]; then
    echo "🧠 Avvio LLM: $LLM_BIN"
    echo "   Modello:  $MODEL_PATH"

    # Se esiste vecchio PID LLM, prova a chiuderlo
    if [ -f "$PID_FILE_LLM" ]; then
        OLD_PID_LLM="$(cat "$PID_FILE_LLM" 2>/dev/null)"
        if [ -n "$OLD_PID_LLM" ] && kill -0 "$OLD_PID_LLM" 2>/dev/null; then
            echo "ℹ Arresto vecchio LLM (PID $OLD_PID_LLM)..."
            kill "$OLD_PID_LLM" 2>/dev/null
            sleep 1
        fi
        rm -f "$PID_FILE_LLM"
    fi

    nohup "$LLM_BIN" \
        --model "$MODEL_PATH" \
        --host 127.0.0.1 \
        --port 8081 \
        > "$LOG_LLM" 2>&1 &

    NEW_PID_LLM=$!
    echo "$NEW_PID_LLM" > "$PID_FILE_LLM"
    echo "✅ LLM avviato (PID $NEW_PID_LLM) – log: $LOG_LLM"
else
    echo "⚠ LLM non avviato: binario o modello mancanti."
    echo "   Atteso binario: $LLM_BIN"
    echo "   Atteso modello: $MODEL_PATH"
fi

# ───────────── API + GUI su 8090 ─────────────

export LLM_BACKEND_URL="http://127.0.0.1:8081/completion"

echo "Avvio API+GUI (python -m api.server) su 0.0.0.0:8090..."
nohup python3 -m api.server 8090 > "$LOG_API" 2>&1 &

NEW_PID_API=$!
echo "$NEW_PID_API" > "$PID_FILE_API"

echo "✅ TheLightrading API avviata (PID $NEW_PID_API) – log: $LOG_API"
echo "   LLM_BACKEND_URL=$LLM_BACKEND_URL"
echo "   URL GUI: http://127.0.0.1:8090"
