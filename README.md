# TheLightrading – Seed AI universe

TheLightrading is a Seed AI playground: a 3D universe of thinking nodes connected to a mock LLM adapter. Each node stores its own chat history and log stream so that reasoning paths stay transparent and isolated.

## Prerequisites
- Python 3.10+
- `python3 -m venv` available

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the local server
```bash
source .venv/bin/activate
python api/server.py
```

The server binds to `0.0.0.0` on port `8080` by default. Override with environment variables:
```bash
HOST=0.0.0.0 PORT=9000 python api/server.py
```

## Access the UI
- Local browser: http://127.0.0.1:8080
- LAN browser (phone, tablet): http://<vm-ip>:8080

## What changed vs the legacy e-commerce clone
- All Ormanet/B2B commerce logic was removed.
- New node model with chat messages and per-node logs persisted in SQLite (`data/db/thelightrading.db`).
- REST endpoints for creating nodes, chatting, exporting conversations, and inspecting logs.
- 3D UI re-skinned as a dark Seed AI universe; clicking a star opens the node panel and chat.
- Mock LLM adapter ready for a real model integration.

## API quick reference
- `GET /api/nodes` – list nodes
- `POST /api/nodes` – create node (`title`, optional `parent_hash`, `position`, `cluster_hash`, `meta`)
- `GET /api/nodes/{hash}` – node details
- `GET /api/nodes/{hash}/messages` – list chat messages
- `POST /api/nodes/{hash}/messages` – add a user message, mock LLM responds
- `POST /api/nodes/{hash}/reset` – clear chat history
- `GET /api/nodes/{hash}/export` – export node and messages as JSON
- `GET /api/nodes/{hash}/logs` – list per-node logs
- `POST /api/nodes/{hash}/logs` – append a log entry (info/warning/error)

## Seed AI mindset
The system enforces a simulation-only universe. Nodes organize conversations and thoughts; no real-world trading or uncontrolled actions are performed. Logs surface errors and reasoning steps for transparency.
