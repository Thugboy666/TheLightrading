#!/usr/bin/env bash
# Wrapper per importare gli ordini mensili dal gestionale.
# Configura il percorso del file CSV/XLSX come primo argomento oppure modifica
# la variabile DEFAULT_INPUT nello script Python.
# Esempio cron (tutti i giorni alle 2):
# 0 2 * * * /percorso/progetto/scripts/import_orders.sh /percorso/export/ordini.xlsx >> /percorso/logs/import_orders.log 2>&1

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INPUT_FILE="${1:-$PROJECT_ROOT/data/orders_latest.csv}"

# Usa il Python della virtualenv se esiste, altrimenti fallback su python3 di sistema
if [ -x "$PROJECT_ROOT/.venv/bin/python" ]; then
  PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

"$PYTHON_BIN" "$PROJECT_ROOT/scripts/import_orders.py" --input "$INPUT_FILE" "${@:2}"
