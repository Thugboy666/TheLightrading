#!/data/data/com.termux/files/usr/bin/bash

PROJECT_DIR="$HOME/TheLight24"
DB_PATH="$PROJECT_DIR/data/db/thelight_universe.db"
PORT=8082
PID_FILE="$PROJECT_DIR/.db_pid"

# Controlla sqlite_web
if ! command -v sqlite_web >/dev/null 2>&1; then
    echo "[INFO] sqlite_web non trovato, lo installo..."
    pip install sqlite-web
fi

# Kill vecchia istanza sulla stessa porta
OLD_PID=$(lsof -t -i:$PORT)
if [ ! -z "$OLD_PID" ]; then
    echo "[INFO] Trovato sqlite_web già attivo (PID $OLD_PID), lo chiudo..."
    kill -9 $OLD_PID
fi

# Avvia sqlite_web
echo "[INFO] Avvio pannello DB su http://127.0.0.1:$PORT"
sqlite_web "$DB_PATH" --host 0.0.0.0 --port $PORT &
NEW_PID=$!
echo $NEW_PID > "$PID_FILE"

sleep 1

# Apri browser del telefono
termux-open-url "http://127.0.0.1:$PORT"

echo "[OK] sqlite-web avviato (PID $NEW_PID). Browser aperto."
