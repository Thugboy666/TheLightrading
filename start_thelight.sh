#!/usr/bin/env sh
# Avvio ordinato: llama.cpp server + API+GUI aiohttp
# Versione "parla chiaro", niente morti silenziose
set -u

BASEDIR="$(cd "$(dirname "$0")" && pwd)"
RUN_DIR="$BASEDIR/run"
LOG_DIR="$BASEDIR/logs"
UI_INDEX="$BASEDIR/gui/index.html"

mkdir -p "$RUN_DIR" "$LOG_DIR"

if [ ! -f "$UI_INDEX" ]; then
  echo "❌ File GUI mancante: $UI_INDEX"
  echo "Assicurati che gui/index.html esista prima di avviare TheLight24."
  exit 1
fi

echo "----- 🚀 START THELIGHT24 -----"
echo "BASEDIR = $BASEDIR"

# Attiva venv se esiste
if [ -d "$BASEDIR/.venv" ]; then
  echo "→ Attivo virtualenv .venv"
  . "$BASEDIR/.venv/bin/activate"
else
  echo "⚠️  Nessuna .venv trovata in $BASEDIR (continuo con Python globale)"
fi

LLM_BIN="$BASEDIR/llm/llama.cpp/build/bin/llama-server"

# Possibili percorsi modello
MODEL_A="$BASEDIR/llm/models/Phi-3-mini-4k-instruct-q4.gguf"
MODEL_B="$BASEDIR/llm/models/Phi-3-mini-4k-instruct-q4_k_m.gguf"
MODELS_DIR1="$BASEDIR/llm/models"
MODELS_DIR2="$BASEDIR/llm/llama.cpp/build/bin"

LLM_HOST="0.0.0.0"
LLM_PORT="8081"
LLM_THREADS="${LLM_THREADS:-6}"
LLM_CTX="${LLM_CTX:-1024}"

API_PORT="8080"

stop_dead_pidfile() {
  PF="$1"
  if [ -f "$PF" ]; then
    PID="$(cat "$PF" 2>/dev/null || true)"
    if [ -n "${PID:-}" ] && ! kill -0 "$PID" 2>/dev/null; then
      echo "ℹ️  Rimuovo PID file orfano: $PF"
      rm -f "$PF" 2>/dev/null || true
    fi
  fi
}

mkdir -p "$RUN_DIR"

stop_dead_pidfile "$RUN_DIR/llm.pid"
stop_dead_pidfile "$RUN_DIR/gui.pid"

# === CHECK BINARIO LLM ===
if [ ! -x "$LLM_BIN" ]; then
  echo "❌ Binario llama-server non trovato o non eseguibile:"
  echo "   $LLM_BIN"
  echo "Controlla di aver compilato llama.cpp e che il path sia corretto."
  exit 1
fi
echo "✔️  Trovato llama-server: $LLM_BIN"

# === SCELTA MODELLO ===
MODEL=""

if [ -n "${LLM_MODEL_PATH:-}" ] && [ -f "$LLM_MODEL_PATH" ]; then
  MODEL="$LLM_MODEL_PATH"
elif [ -f "$MODEL_A" ]; then
  MODEL="$MODEL_A"
elif [ -f "$MODEL_B" ]; then
  MODEL="$MODEL_B"
else
  # uso find ma senza far esplodere lo script se una cartella non esiste
  CANDIDATE=""
  if [ -d "$MODELS_DIR1" ] || [ -d "$MODELS_DIR2" ]; then
    CANDIDATE="$(find "$MODELS_DIR1" "$MODELS_DIR2" -maxdepth 2 -type f -name '*.gguf' 2>/dev/null | head -n 1 || true)"
  fi
  if [ -n "$CANDIDATE" ] && [ -f "$CANDIDATE" ]; then
    MODEL="$CANDIDATE"
  fi
fi

if [ -z "$MODEL" ]; then
  echo "❌ Nessun modello GGUF trovato.
Ho cercato qui:
  - $MODEL_A
  - $MODEL_B
  - $MODELS_DIR1 (tutti i .gguf)
  - $MODELS_DIR2 (tutti i .gguf)

Sposta il tuo modello .gguf in una di queste cartelle
(o lancia con:  LLM_MODEL_PATH=/percorso/modello.gguf ./start_thelight.sh )"
  exit 1
fi

echo "🧠 Userò il modello: $MODEL"

# === AVVIO LLM ===
if [ -f "$RUN_DIR/llm.pid" ] && kill -0 "$(cat "$RUN_DIR/llm.pid")" 2>/dev/null; then
  echo "ℹ️  LLM già avviato (PID $(cat "$RUN_DIR/llm.pid"))."
else
  echo "▶️  Avvio LLM: $LLM_BIN"
  (cd "$(dirname "$LLM_BIN")" && \
    nohup "$LLM_BIN" \
      -m "$MODEL" \
      --host "$LLM_HOST" \
      --port "$LLM_PORT" \
      --threads "$LLM_THREADS" \
      --ctx-size "$LLM_CTX" \
      > "$LOG_DIR/llm.log" 2>&1 & echo $! > "$RUN_DIR/llm.pid")
  sleep 2
fi

# === CHECK LLM RAPIDO ===
printf "⏳ Check LLM su 127.0.0.1:%s ..." "$LLM_PORT"
if command -v curl >/dev/null 2>&1; then
  for i in 1 2 3 4 5; do
    if curl -s -m 3 -H "Content-Type: application/json" \
      -d '{"prompt":"ping","n_predict":8}' \
      "http://127.0.0.1:${LLM_PORT}/completion" >/dev/null 2>&1; then
      echo " ok"
      break
    fi
    sleep 1
    [ "$i" -eq 5 ] && echo " FAIL (controlla logs/llm.log)"
  done
else
  echo " (curl non disponibile, salto check)"
fi

# === AVVIO API+GUI (aiohttp + index.html) ===
if [ -f "$RUN_DIR/gui.pid" ] && kill -0 "$(cat "$RUN_DIR/gui.pid")" 2>/dev/null; then
  echo "ℹ️  API+GUI già avviate (PID $(cat "$RUN_DIR/gui.pid"))."
else
  echo "▶️  Avvio API+GUI (python api/server.py) su porta $API_PORT"
  (cd "$BASEDIR" && \
    THELIGHT_UI_INDEX="$UI_INDEX" \
    LLM_BACKEND_URL="http://127.0.0.1:${LLM_PORT}/completion" \
    nohup python api/server.py > "$LOG_DIR/gui.log" 2>&1 & echo $! > "$RUN_DIR/gui.pid")
  sleep 2
fi

echo "✅ Avviato."
echo "- LLM:      PID $(cat "$RUN_DIR/llm.pid" 2>/dev/null || echo '?')  | log: $LOG_DIR/llm.log  | http://127.0.0.1:$LLM_PORT"
echo "- API+GUI:  PID $(cat "$RUN_DIR/gui.pid" 2>/dev/null || echo '?') | log: $LOG_DIR/gui.log | http://127.0.0.1:$API_PORT"
echo "------------------------------------"
