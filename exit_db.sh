#!/data/data/com.termux/files/usr/bin/bash

PROJECT_DIR="$HOME/TheLight24"
PID_FILE="$PROJECT_DIR/.db_pid"

if [ ! -f "$PID_FILE" ]; then
    echo "[WARN] Nessun PID salvato. Probabile che sqlite-web non sia attivo."
    exit 0
fi

PID=$(cat "$PID_FILE")

if ps -p $PID > /dev/null 2>&1; then
    echo "[INFO] Chiudo sqlite-web (PID $PID)..."
    kill -9 $PID
else
    echo "[WARN] Il processo (PID $PID) non esiste più."
fi

rm "$PID_FILE"
echo "[OK] Pannello DB chiuso."
