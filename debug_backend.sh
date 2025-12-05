#!/data/data/com.termux/files/usr/bin/sh
cd "$(dirname "$0")"
. .venv/bin/activate
export PYTHONPATH="$PWD"
API_HOST=0.0.0.0 API_PORT=8080 python -m api.server
