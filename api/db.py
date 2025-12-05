import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "db" / "thelightrading.db"


def ensure_db_dir() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_db() -> sqlite3.Connection:
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    ensure_db_dir()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                parent_hash TEXT,
                cluster_hash TEXT,
                position_x REAL,
                position_y REAL,
                position_z REAL,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS node_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (node_hash) REFERENCES nodes(hash)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS node_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_hash TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                payload_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (node_hash) REFERENCES nodes(hash)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_node_messages_hash ON node_messages(node_hash)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_node_logs_hash ON node_logs(node_hash)"
        )
        conn.commit()


def insert_node(node_data: Dict[str, Any]) -> Dict[str, Any]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO nodes (
                hash, title, parent_hash, cluster_hash, position_x, position_y, position_z, meta_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_data["hash"],
                node_data["title"],
                node_data.get("parent_hash"),
                node_data.get("cluster_hash"),
                node_data.get("position_x"),
                node_data.get("position_y"),
                node_data.get("position_z"),
                json.dumps(node_data.get("meta") or {}),
                node_data.get("created_at", _now_iso()),
                node_data.get("updated_at", _now_iso()),
            ),
        )
        conn.commit()
        node_data["id"] = cur.lastrowid
    return node_data


def list_nodes(
    *,
    parent_hash: Optional[str] = None,
    cluster_hash: Optional[str] = None,
    search: Optional[str] = None,
) -> List[Dict[str, Any]]:
    query = "SELECT * FROM nodes WHERE 1=1"
    params: List[Any] = []
    if parent_hash:
        query += " AND parent_hash = ?"
        params.append(parent_hash)
    if cluster_hash:
        query += " AND cluster_hash = ?"
        params.append(cluster_hash)
    if search:
        query += " AND (title LIKE ? OR hash LIKE ?)"
        like = f"%{search}%"
        params.extend([like, like])
    query += " ORDER BY created_at DESC"

    with get_db() as conn:
        cur = conn.execute(query, params)
        rows = cur.fetchall()
    return [row_to_dict(row) for row in rows]


def get_node_by_hash(node_hash: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM nodes WHERE hash = ?", (node_hash,))
        row = cur.fetchone()
    if not row:
        return None
    return row_to_dict(row)


def touch_node(node_hash: str) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE nodes SET updated_at = ? WHERE hash = ?",
            (_now_iso(), node_hash),
        )
        conn.commit()


def delete_node_messages(node_hash: str) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM node_messages WHERE node_hash = ?", (node_hash,))
        conn.commit()


def add_node_message(node_hash: str, role: str, content: str) -> Dict[str, Any]:
    message = {
        "node_hash": node_hash,
        "role": role,
        "content": content,
        "created_at": _now_iso(),
    }
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO node_messages (node_hash, role, content, created_at) VALUES (?, ?, ?, ?)",
            (
                message["node_hash"],
                message["role"],
                message["content"],
                message["created_at"],
            ),
        )
        conn.commit()
        message["id"] = cur.lastrowid
    return message


def list_node_messages(node_hash: str) -> List[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM node_messages WHERE node_hash = ? ORDER BY created_at ASC",
            (node_hash,),
        )
        rows = cur.fetchall()
    return [row_to_dict(row) for row in rows]


def add_node_log(
    node_hash: str,
    level: str,
    message: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    log_entry = {
        "node_hash": node_hash,
        "level": level,
        "message": message,
        "payload_json": json.dumps(payload) if payload is not None else None,
        "created_at": _now_iso(),
    }
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO node_logs (node_hash, level, message, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                log_entry["node_hash"],
                log_entry["level"],
                log_entry["message"],
                log_entry["payload_json"],
                log_entry["created_at"],
            ),
        )
        conn.commit()
        log_entry["id"] = cur.lastrowid
    return log_entry


def list_node_logs(node_hash: str) -> List[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM node_logs WHERE node_hash = ? ORDER BY created_at DESC",
            (node_hash,),
        )
        rows = cur.fetchall()
    return [row_to_dict(row) for row in rows]

