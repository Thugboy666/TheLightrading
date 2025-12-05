import json
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict

from aiohttp import web

from api import db
from core import node_logger
from core.nodes import Node, generate_node_hash
from llm.adapter import run_llm_for_node

# ================== CONFIG BASE ==================

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "api.log"

logger = logging.getLogger("thelightrading.api")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=2 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

logger.info("TheLightrading API avviata (server.py caricato)")


def _resolve_ui_index() -> Path:
    candidate = os.environ.get("THELIGHT_UI_INDEX")
    if candidate:
        ui_path = Path(candidate).expanduser().resolve()
    else:
        ui_path = BASE_DIR / "gui" / "index.html"

    if not ui_path.is_file():
        raise FileNotFoundError(f"GUI index non trovato: {ui_path}")

    return ui_path


UI_INDEX = _resolve_ui_index()

db.init_db()

# ================== HELPERS ==================


def json_response(payload: Dict[str, Any], status: int = 200) -> web.Response:
    return web.json_response(payload, status=status)


async def _ensure_node(node_hash: str) -> Dict[str, Any]:
    node = db.get_node_by_hash(node_hash)
    if not node:
        raise web.HTTPNotFound(text="Nodo non trovato")
    return node


# ================== ROUTES ==================


async def serve_index(_request: web.Request) -> web.StreamResponse:
    return web.FileResponse(UI_INDEX)


async def list_nodes(request: web.Request) -> web.Response:
    parent_hash = request.query.get("parent_hash")
    cluster_hash = request.query.get("cluster_hash")
    search = request.query.get("search")
    nodes = db.list_nodes(parent_hash=parent_hash, cluster_hash=cluster_hash, search=search)
    return json_response({"nodes": nodes})


async def create_node(request: web.Request) -> web.Response:
    data = await request.json()
    title = (data.get("title") or "Nodo").strip()
    if not title:
        raise web.HTTPBadRequest(text="Titolo del nodo obbligatorio")

    node_hash = generate_node_hash(title)
    node = Node(
        hash=node_hash,
        title=title,
        parent_hash=data.get("parent_hash"),
        cluster_hash=data.get("cluster_hash"),
        position_x=data.get("position", {}).get("x"),
        position_y=data.get("position", {}).get("y"),
        position_z=data.get("position", {}).get("z"),
        meta=data.get("meta") or {},
    )

    saved = db.insert_node(node.__dict__)
    node_logger.log_node_info(node_hash, "Nodo creato", {"title": title})
    return json_response({"node": saved}, status=201)


async def get_node(request: web.Request) -> web.Response:
    node = await _ensure_node(request.match_info["hash"])
    return json_response({"node": node})


async def get_node_messages(request: web.Request) -> web.Response:
    node_hash = request.match_info["hash"]
    await _ensure_node(node_hash)
    messages = db.list_node_messages(node_hash)
    return json_response({"messages": messages})


async def append_node_message(request: web.Request) -> web.Response:
    node_hash = request.match_info["hash"]
    node = await _ensure_node(node_hash)
    data = await request.json()
    content = (data.get("content") or "").strip()
    role = data.get("role", "user")
    if not content:
        raise web.HTTPBadRequest(text="Contenuto del messaggio obbligatorio")

    user_message = db.add_node_message(node_hash, role, content)
    db.touch_node(node_hash)

    messages = db.list_node_messages(node_hash)
    system_prompt = "Seed AI playground: organizza pensieri in un grafo di nodi."
    reply = await run_llm_for_node(node, messages, system_prompt)
    assistant_message = db.add_node_message(node_hash, "assistant", reply)
    node_logger.log_node_info(node_hash, "Scambio chat", {"user_len": len(content)})

    return json_response({"messages": [user_message, assistant_message]}, status=201)


async def reset_node_messages(request: web.Request) -> web.Response:
    node_hash = request.match_info["hash"]
    await _ensure_node(node_hash)
    db.delete_node_messages(node_hash)
    node_logger.log_node_info(node_hash, "Cronologia ripulita")
    return json_response({"status": "ok"})


async def export_node(request: web.Request) -> web.Response:
    node_hash = request.match_info["hash"]
    node = await _ensure_node(node_hash)
    messages = db.list_node_messages(node_hash)
    payload = {"node": node, "messages": messages}
    return web.Response(
        text=json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
    )


async def get_node_logs(request: web.Request) -> web.Response:
    node_hash = request.match_info["hash"]
    await _ensure_node(node_hash)
    logs = db.list_node_logs(node_hash)
    return json_response({"logs": logs})


async def append_node_log(request: web.Request) -> web.Response:
    node_hash = request.match_info["hash"]
    await _ensure_node(node_hash)
    data = await request.json()
    level = data.get("level", "info")
    message = data.get("message") or ""
    payload = data.get("payload")
    if not message:
        raise web.HTTPBadRequest(text="Messaggio di log obbligatorio")
    if level == "info":
        log_entry = node_logger.log_node_info(node_hash, message, payload)
    elif level == "warning":
        log_entry = node_logger.log_node_warning(node_hash, message, payload)
    else:
        log_entry = node_logger.log_node_error(node_hash, message, payload)
    return json_response({"log": log_entry}, status=201)


# ================== APP FACTORY ==================


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", serve_index)
    app.router.add_static("/assets/", BASE_DIR / "gui" / "assets", show_index=True)

    app.router.add_get("/api/nodes", list_nodes)
    app.router.add_post("/api/nodes", create_node)
    app.router.add_get("/api/nodes/{hash}", get_node)
    app.router.add_get("/api/nodes/{hash}/messages", get_node_messages)
    app.router.add_post("/api/nodes/{hash}/messages", append_node_message)
    app.router.add_post("/api/nodes/{hash}/reset", reset_node_messages)
    app.router.add_get("/api/nodes/{hash}/export", export_node)
    app.router.add_get("/api/nodes/{hash}/logs", get_node_logs)
    app.router.add_post("/api/nodes/{hash}/logs", append_node_log)

    return app


def main() -> None:
    app = create_app()
    port = int(os.environ.get("PORT", "8080"))
    host = os.environ.get("HOST", "0.0.0.0")
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()
