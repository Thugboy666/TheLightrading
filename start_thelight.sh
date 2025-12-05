#!/data/data/com.termux/files/usr/bin/sh

BASEDIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASEDIR"

if [ "$(basename "$BASEDIR")" != "TheLightrading" ]; then
    echo "❌ Questo script è riservato al progetto TheLightrading. Directory corrente: $BASEDIR"
    exit 1
fi

PID_FILE="thelightrading.pid"
LOG_FILE="thelightrading.log"

echo "----- 🚀 START THELIGHTRADING -----"
echo "BASEDIR = $BASEDIR"

# Se esiste già un PID, verifica se il processo è vivo
if [ -f "$PID_FILE" ]; then
    OLD_PID="$(cat "$PID_FILE" 2>/dev/null)"
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "⚠ TheLightrading sembra già in esecuzione (PID $OLD_PID)."
        echo "   Se non è così, lancia ./stop_theight.sh e poi riprova."
        exit 0
    else
        echo "ℹ Rimosso vecchio PID non valido ($OLD_PID)."
        rm -f "$PID_FILE"
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

# Avvia il backend su 8090
echo "Avvio api/server.py sulla porta 8090..."
nohup python3 api/server.py 8090 > "$LOG_FILE" 2>&1 &

NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"
echo "✅ TheLightrading avviato (PID $NEW_PID) – log: $LOG_FILE"
