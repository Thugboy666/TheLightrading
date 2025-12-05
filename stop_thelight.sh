#!/data/data/com.termux/files/usr/bin/sh

BASEDIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASEDIR"

if [ "$(basename "$BASEDIR")" != "TheLightrading" ]; then
    echo "❌ Questo script è riservato al progetto TheLightrading. Directory corrente: $BASEDIR"
    exit 1
fi

PID_FILE="thelightrading.pid"

echo "----- 🛑 STOP THELIGHTRADING -----"
echo "BASEDIR = $BASEDIR"

if [ -f "$PID_FILE" ]; then
    PID="$(cat "$PID_FILE" 2>/dev/null)"

    if [ -n "$PID" ] && kill "$PID" 2>/dev/null; then
        echo "✅ Arrestato TheLightrading (PID $PID)."
    else
        echo "⚠ PID nel file ($PID) non valido o processo già morto."
    fi

    rm -f "$PID_FILE"
    exit 0
fi

echo "ℹ Nessun file PID trovato, provo a fermare per pattern di comando..."
pkill -f "api/server.py 8090" 2>/dev/null && {
    echo "✅ Arrestato TheLightrading tramite pkill su api/server.py 8090."
    exit 0
}

echo "ℹ Nessun processo TheLightrading trovato da fermare."
