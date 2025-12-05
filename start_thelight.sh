#!/data/data/com.termux/files/usr/bin/sh

BASEDIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASEDIR"

echo "----- 🚀 START THELIGHTRADING -----"

if [ -d "./venv" ]; then
    . ./venv/bin/activate
elif [ -d "./.venv" ]; then
    . ./.venv/bin/activate
fi

nohup python3 api/server.py 8090 > thelightrading.log 2>&1 &
echo $! > thelightrading.pid
