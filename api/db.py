import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "db" / "thelight_universe.db"


def ensure_db_dir() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_db() -> sqlite3.Connection:
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def init_db() -> None:
    ensure_db_dir()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                name TEXT,
                tier TEXT DEFAULT 'rivenditore10',
                piva TEXT,
                phone TEXT,
                created_at TEXT,
                updated_at TEXT,
                is_admin INTEGER DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ragione_sociale TEXT,
                piva TEXT,
                email TEXT,
                telefono TEXT,
                listino TEXT,
                stato TEXT,
                note TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS discount_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                offer_id TEXT NOT NULL,
                segment TEXT NOT NULL,
                min_amount REAL NOT NULL,
                max_amount REAL,
                discount_percent REAL NOT NULL,
                valid_until TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                sku TEXT PRIMARY KEY,
                name TEXT,
                image_hd TEXT,
                image_thumb TEXT,
                gallery_json TEXT,
                description_html TEXT,
                base_price REAL,
                unit TEXT,
                markup_riv10 REAL,
                markup_riv REAL,
                markup_dist REAL,
                price_riv10 REAL,
                price_riv REAL,
                price_dist REAL,
                extra_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS price_list_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                imported_at TEXT,
                file_name TEXT,
                total_products INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER,
                created_at TEXT,
                expires_at TEXT
            )
            """
        )
        conn.commit()


# ========== USERS / AUTH ========== #

def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM users WHERE email = ?", (email,))
        row = cur.fetchone()
        return row_to_dict(row) if row else None


def create_user(
    email: str,
    password_hash: str,
    name: Optional[str] = None,
    tier: str = "rivenditore10",
    piva: str = "",
    phone: str = "",
    is_admin: int = 0,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO users (email, password_hash, name, tier, piva, phone, created_at, updated_at, is_admin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (email, password_hash, name, tier, piva, phone, now, now, is_admin),
        )
        conn.commit()
        user_id = cur.lastrowid
    return get_user_by_email(email) or {"id": user_id, "email": email, "name": name, "tier": tier}


def create_session(user_id: int, days_valid: int = 30) -> str:
    token = os.urandom(16).hex()
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=days_valid)
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (token, user_id, now.isoformat(), expires.isoformat()),
        )
        conn.commit()
    return token


def get_session(token: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM sessions WHERE token = ?", (token,))
        row = cur.fetchone()
        if not row:
            return None
        data = row_to_dict(row)
        if data.get("expires_at"):
            exp = datetime.fromisoformat(data["expires_at"])
            if exp < datetime.now(timezone.utc):
                conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
                conn.commit()
                return None
        return data


# ========== CLIENTS ========== #

def list_clients() -> List[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM clients ORDER BY id DESC")
        return [row_to_dict(r) for r in cur.fetchall()]


def save_client(data: Dict[str, Any]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        if data.get("id"):
            conn.execute(
                """
                UPDATE clients
                SET ragione_sociale = ?, piva = ?, email = ?, telefono = ?, listino = ?, stato = ?, note = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    data.get("ragione_sociale"),
                    data.get("piva"),
                    data.get("email"),
                    data.get("telefono"),
                    data.get("listino"),
                    data.get("stato"),
                    data.get("note"),
                    now,
                    data.get("id"),
                ),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO clients (ragione_sociale, piva, email, telefono, listino, stato, note, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data.get("ragione_sociale"),
                    data.get("piva"),
                    data.get("email"),
                    data.get("telefono"),
                    data.get("listino"),
                    data.get("stato"),
                    data.get("note"),
                    now,
                    now,
                ),
            )
            data["id"] = cur.lastrowid
        conn.commit()
    if not data.get("id"):
        with get_db() as conn:
            cur = conn.execute("SELECT last_insert_rowid() AS id")
            data["id"] = cur.fetchone()["id"]
    return data


def delete_client(client_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))
        conn.commit()


# ========== DISCOUNT RULES ========== #

def list_discount_rules() -> List[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM discount_rules ORDER BY offer_id, segment, min_amount"
        )
        return [row_to_dict(r) for r in cur.fetchall()]


def clear_discount_rules_for_offer_segment(offer_id: str, segment: str) -> None:
    with get_db() as conn:
        conn.execute(
            "DELETE FROM discount_rules WHERE offer_id = ? AND segment = ?",
            (offer_id, segment),
        )
        conn.commit()


def insert_discount_rule(
    offer_id: str,
    segment: str,
    min_amount: float,
    max_amount: Optional[float],
    discount_percent: float,
    valid_until: Optional[str],
) -> None:
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO discount_rules (offer_id, segment, min_amount, max_amount, discount_percent, valid_until)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (offer_id, segment, min_amount, max_amount, discount_percent, valid_until),
        )
        conn.commit()


# ========== PRODUCTS ========== #

def upsert_product(data: Dict[str, Any]) -> Dict[str, Any]:
    gallery_json = json.dumps(data.get("gallery") or data.get("gallery_json") or [])
    extra_json = json.dumps(data.get("extra") or data.get("extra_json") or {})
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO products (sku, name, image_hd, image_thumb, gallery_json, description_html, base_price, unit, markup_riv10, markup_riv, markup_dist, price_riv10, price_riv, price_dist, extra_json)
            VALUES (:sku, :name, :image_hd, :image_thumb, :gallery_json, :description_html, :base_price, :unit, :markup_riv10, :markup_riv, :markup_dist, :price_riv10, :price_riv, :price_dist, :extra_json)
            ON CONFLICT(sku) DO UPDATE SET
                name=excluded.name,
                image_hd=excluded.image_hd,
                image_thumb=excluded.image_thumb,
                gallery_json=excluded.gallery_json,
                description_html=excluded.description_html,
                base_price=excluded.base_price,
                unit=excluded.unit,
                markup_riv10=excluded.markup_riv10,
                markup_riv=excluded.markup_riv,
                markup_dist=excluded.markup_dist,
                price_riv10=excluded.price_riv10,
                price_riv=excluded.price_riv,
                price_dist=excluded.price_dist,
                extra_json=excluded.extra_json
            """,
            {
                "sku": data.get("sku"),
                "name": data.get("name"),
                "image_hd": data.get("image_hd"),
                "image_thumb": data.get("image_thumb"),
                "gallery_json": gallery_json,
                "description_html": data.get("description_html"),
                "base_price": data.get("base_price"),
                "unit": data.get("unit"),
                "markup_riv10": data.get("markup_riv10"),
                "markup_riv": data.get("markup_riv"),
                "markup_dist": data.get("markup_dist"),
                "price_riv10": data.get("price_riv10"),
                "price_riv": data.get("price_riv"),
                "price_dist": data.get("price_dist"),
                "extra_json": extra_json,
            },
        )
        conn.commit()
    return get_product_by_sku(data.get("sku")) or {"sku": data.get("sku")}


def get_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM products WHERE sku = ?", (sku,))
        row = cur.fetchone()
        if not row:
            return None
        data = row_to_dict(row)
        data["gallery"] = json.loads(data.get("gallery_json") or "[]")
        data["extra"] = json.loads(data.get("extra_json") or "{}")
        data["id"] = data.get("sku")
        return data


def list_products() -> List[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM products ORDER BY name")
        rows = []
        for r in cur.fetchall():
            d = row_to_dict(r)
            d["gallery"] = json.loads(d.get("gallery_json") or "[]")
            d["extra"] = json.loads(d.get("extra_json") or "{}")
            d["id"] = d.get("sku")
            rows.append(d)
        return rows


def save_import_metadata(file_name: str, total_products: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO price_list_imports (imported_at, file_name, total_products) VALUES (?, ?, ?)",
            (now, file_name, total_products),
        )
        conn.commit()


__all__ = [
    "init_db",
    "get_db",
    "get_user_by_email",
    "create_user",
    "create_session",
    "get_session",
    "list_clients",
    "save_client",
    "delete_client",
    "list_discount_rules",
    "clear_discount_rules_for_offer_segment",
    "insert_discount_rule",
    "upsert_product",
    "get_product_by_sku",
    "list_products",
    "save_import_metadata",
]
