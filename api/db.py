import json
import os
import sqlite3
import uuid
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
                promo_enabled INTEGER DEFAULT 0,
                promo_points INTEGER DEFAULT 0,
                promo_ticket_code TEXT,
                promo_last_update TEXT,
                user_id INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        # PROMO NATALE: migrazione colonne
        existing_cols = {
            row["name"] for row in cur.execute("PRAGMA table_info(clients)").fetchall()
        }
        migrations = [
            ("promo_enabled", "ALTER TABLE clients ADD COLUMN promo_enabled INTEGER DEFAULT 0"),
            ("promo_points", "ALTER TABLE clients ADD COLUMN promo_points INTEGER DEFAULT 0"),
            ("promo_ticket_code", "ALTER TABLE clients ADD COLUMN promo_ticket_code TEXT"),
            ("promo_last_update", "ALTER TABLE clients ADD COLUMN promo_last_update TEXT"),
            ("user_id", "ALTER TABLE clients ADD COLUMN user_id INTEGER"),
        ]
        for col, stmt in migrations:
            if col not in existing_cols:
                cur.execute(stmt)
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
                codice TEXT,
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
                extra_json TEXT,
                price_distributore REAL,
                price_rivenditore REAL,
                price_rivenditore10 REAL,
                qty_stock INTEGER DEFAULT 0,
                discount_dist_percent REAL DEFAULT 0,
                discount_riv_percent REAL DEFAULT 0,
                discount_riv10_percent REAL DEFAULT 0,
                status TEXT DEFAULT 'attivo'
            )
            """
        )
        # MIGRAZIONI LISTINO
        product_cols = {
            row["name"] for row in cur.execute("PRAGMA table_info(products)").fetchall()
        }
        migrations_products = [
            ("codice", "ALTER TABLE products ADD COLUMN codice TEXT"),
            ("price_distributore", "ALTER TABLE products ADD COLUMN price_distributore REAL"),
            ("price_rivenditore", "ALTER TABLE products ADD COLUMN price_rivenditore REAL"),
            ("price_rivenditore10", "ALTER TABLE products ADD COLUMN price_rivenditore10 REAL"),
            ("qty_stock", "ALTER TABLE products ADD COLUMN qty_stock INTEGER DEFAULT 0"),
            (
                "discount_dist_percent",
                "ALTER TABLE products ADD COLUMN discount_dist_percent REAL DEFAULT 0",
            ),
            (
                "discount_riv_percent",
                "ALTER TABLE products ADD COLUMN discount_riv_percent REAL DEFAULT 0",
            ),
            (
                "discount_riv10_percent",
                "ALTER TABLE products ADD COLUMN discount_riv10_percent REAL DEFAULT 0",
            ),
            ("status", "ALTER TABLE products ADD COLUMN status TEXT DEFAULT 'attivo'"),
        ]
        for col, stmt in migrations_products:
            if col not in product_cols:
                cur.execute(stmt)
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
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS promo_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                action_code TEXT,
                points INTEGER,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS promo_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                name TEXT,
                start_date TEXT,
                end_date TEXT,
                description TEXT,
                actions_text TEXT,
                actions_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_number TEXT NOT NULL,
                status TEXT,
                cause TEXT,
                customer_name TEXT,
                customer_email TEXT,
                order_date TEXT,
                total_amount REAL,
                external_id TEXT,
                notes TEXT
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_email ON orders(LOWER(customer_email))"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date)"
        )
        # OFFERTA DAILY
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_offer (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                sku TEXT NOT NULL,
                start_at TEXT,
                end_at TEXT,
                discount_dist_percent REAL DEFAULT 0,
                discount_riv_percent REAL DEFAULT 0,
                discount_riv10_percent REAL DEFAULT 0,
                coupon_code TEXT,
                active INTEGER DEFAULT 0,
                min_qty INTEGER NOT NULL DEFAULT 1,
                product_url TEXT
            )
            """
        )
        # Migrazione campo min_qty
        daily_cols = {
            row["name"] for row in cur.execute("PRAGMA table_info(daily_offer)").fetchall()
        }
        if "min_qty" not in daily_cols:
            cur.execute(
                "ALTER TABLE daily_offer ADD COLUMN min_qty INTEGER NOT NULL DEFAULT 1"
            )
        if "product_url" not in daily_cols:
            cur.execute("ALTER TABLE daily_offer ADD COLUMN product_url TEXT")

        # NOTIFICATION SETTINGS
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notification_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL UNIQUE,
                notify_macro_offers INTEGER DEFAULT 1,
                notify_daily_deal INTEGER DEFAULT 1,
                notify_event_offer INTEGER DEFAULT 1,
                notify_order_status INTEGER DEFAULT 1,
                updated_at TEXT
            )
            """
        )
        existing_settings = cur.execute(
            "SELECT * FROM notification_settings WHERE scope = ?", ("global",)
        ).fetchone()
        if not existing_settings:
            now = datetime.now(timezone.utc).isoformat()
            cur.execute(
                """
                INSERT INTO notification_settings (
                    scope,
                    notify_macro_offers,
                    notify_daily_deal,
                    notify_event_offer,
                    notify_order_status,
                    updated_at
                ) VALUES (?, 1, 1, 1, 1, ?)
                """,
                ("global", now),
            )
        conn.commit()


# ========== USERS / AUTH ========== #

def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM users WHERE LOWER(email) = LOWER(?)", (email,)
        )
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


def create_session_with_expiry(user_id: int, expires_delta: timedelta) -> str:
    token = os.urandom(16).hex()
    now = datetime.now(timezone.utc)
    expires = now + expires_delta
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


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        return row_to_dict(row) if row else None


def update_user_password(user_id: int, new_password_hash: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
            (new_password_hash, now, user_id),
        )
        conn.commit()


# ========== CLIENTS ========== #

def list_clients() -> List[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT c.*, u.email AS user_email, u.tier AS user_tier
            FROM clients c
            LEFT JOIN users u ON c.user_id = u.id
            ORDER BY c.id DESC
            """
        )
        return [row_to_dict(r) for r in cur.fetchall()]


def get_client_by_id(client_id: int) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,))
        row = cur.fetchone()
        return row_to_dict(row) if row else None


def get_client_by_email(email: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM clients WHERE LOWER(email) = LOWER(?)", (email,)
        )
        row = cur.fetchone()
        return row_to_dict(row) if row else None


def find_client_by_email_or_piva(email: Optional[str], piva: Optional[str]) -> Optional[Dict[str, Any]]:
    query = "SELECT * FROM clients WHERE 1=1"
    params: list[Any] = []
    conditions: list[str] = []
    if email:
        conditions.append("LOWER(email) = LOWER(?)")
        params.append(email)
    if piva:
        conditions.append("piva = ?")
        params.append(piva)
    if not conditions:
        return None
    query += " AND (" + " OR ".join(conditions) + ")"
    with get_db() as conn:
        cur = conn.execute(query, tuple(params))
        row = cur.fetchone()
        return row_to_dict(row) if row else None


def save_client(data: Dict[str, Any]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    promo_enabled = 1 if str(data.get("promo_enabled", 0)) in ("1", "true", "True") else 0
    promo_points = int(data.get("promo_points", 0) or 0)
    promo_ticket_code = data.get("promo_ticket_code") or None
    promo_last_update = data.get("promo_last_update") or None
    user_id = data.get("user_id")
    with get_db() as conn:
        if data.get("id"):
            existing = get_client_by_id(int(data["id"]))
            promo_ticket_code = promo_ticket_code or (existing or {}).get("promo_ticket_code")
            if existing and not promo_last_update:
                promo_last_update = existing.get("promo_last_update")
            conn.execute(
                """
                UPDATE clients
                SET ragione_sociale = ?, piva = ?, email = ?, telefono = ?, listino = ?, stato = ?, note = ?, promo_enabled = ?, promo_points = ?, promo_ticket_code = ?, promo_last_update = ?, user_id = ?, updated_at = ?
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
                    promo_enabled,
                    promo_points,
                    promo_ticket_code,
                    promo_last_update,
                    user_id,
                    now,
                    data.get("id"),
                ),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO clients (ragione_sociale, piva, email, telefono, listino, stato, note, promo_enabled, promo_points, promo_ticket_code, promo_last_update, user_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data.get("ragione_sociale"),
                    data.get("piva"),
                    data.get("email"),
                    data.get("telefono"),
                    data.get("listino"),
                    data.get("stato"),
                    data.get("note"),
                    promo_enabled,
                    promo_points,
                    promo_ticket_code,
                    promo_last_update,
                    user_id,
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


def link_client_to_user_by_email(
    email: Optional[str],
    *,
    create_missing_client: bool = False,
    default_listino: str = "rivenditore10",
) -> Optional[Dict[str, Any]]:
    # Allinea un client all'utente con stessa email, opzionalmente creandolo
    if not email:
        return None

    user = get_user_by_email(email)
    if not user:
        return None

    client = get_client_by_email(email)
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        if client:
            if client.get("user_id") != user.get("id"):
                conn.execute(
                    "UPDATE clients SET user_id = ?, updated_at = ? WHERE id = ?",
                    (user.get("id"), now, client.get("id")),
                )
                conn.commit()
                client["user_id"] = user.get("id")
            return client

        if not create_missing_client:
            return None

        cur = conn.execute(
            """
            INSERT INTO clients (ragione_sociale, piva, email, telefono, listino, stato, note, promo_enabled, promo_points, promo_ticket_code, promo_last_update, user_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                None,
                None,
                email,
                None,
                user.get("tier") or default_listino,
                "attivo",
                "",
                0,
                0,
                None,
                None,
                user.get("id"),
                now,
                now,
            ),
        )
        conn.commit()
        return get_client_by_id(cur.lastrowid)


def delete_client(client_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))
        conn.commit()


# PROMO NATALE: gestione punti
def _generate_ticket_code() -> str:
    return f"XMAS-{uuid.uuid4().hex[:8].upper()}"


def add_promo_points(client_id: int, action_code: str, points: int) -> Optional[Dict[str, Any]]:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        client = get_client_by_id(client_id)
        if not client:
            return None

        new_points = int(client.get("promo_points") or 0) + int(points)
        ticket_code = client.get("promo_ticket_code")
        if not ticket_code and int(client.get("promo_enabled") or 0) == 1:
            ticket_code = _generate_ticket_code()

        conn.execute(
            """
            UPDATE clients
            SET promo_points = ?, promo_ticket_code = ?, promo_last_update = ?, updated_at = ?
            WHERE id = ?
            """,
            (new_points, ticket_code, now, now, client_id),
        )
        conn.execute(
            """
            INSERT INTO promo_logs (client_id, action_code, points, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (client_id, action_code, points, now),
        )
        conn.commit()
    return get_client_by_id(client_id)


def get_promo_summary(client_id: int) -> Dict[str, Any]:
    client = get_client_by_id(client_id)
    if not client:
        return {}
    with get_db() as conn:
        cur = conn.execute(
            "SELECT action_code, points, created_at FROM promo_logs WHERE client_id = ? ORDER BY id DESC",
            (client_id,),
        )
        actions = [row_to_dict(r) for r in cur.fetchall()]

    points = int(client.get("promo_points") or 0)
    if points >= 1000:
        tier = "max"
    elif points >= 850:
        tier = "tier2"
    elif points >= 350:
        tier = "tier1"
    else:
        tier = "base"

    prizes: list[str] = []
    if tier == "tier1":
        prizes = [
            "1 premio disponibile",
            "Spedizione gratuita prime due settimane di gennaio",
            "Omaggi su prodotti trattati",
        ]
    elif tier == "tier2":
        prizes = [
            "Fino a 2 premi disponibili",
            "Spedizione gratuita prime due settimane di gennaio",
            "Sconto massimo sul fatturato di gennaio",
            "Omaggi prodotti trattati",
            "Sconto minimo 3 mesi su categoria trattata",
        ]
    elif tier == "max":
        prizes = [
            "Biglietto fortunato: tutti i vantaggi (max 3 premi)",
            "Spedizione gratuita prime due settimane di gennaio",
            "Sconto massimo sul fatturato di gennaio",
            "Omaggi prodotti trattati",
            "Sconto minimo 3 mesi su categoria trattata",
        ]

    return {
        "points": points,
        "actions": actions,
        "tier": tier,
        "prizes_available": prizes,
        "promo_enabled": int(client.get("promo_enabled") or 0) == 1,
        "promo_ticket_code": client.get("promo_ticket_code"),
    }


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
    payload = {
        "sku": data.get("sku"),
        "name": data.get("name"),
        "codice": data.get("codice"),
        "image_hd": data.get("image_hd"),
        "image_thumb": data.get("image_thumb"),
        "gallery_json": gallery_json,
        "description_html": data.get("description_html") or data.get("desc_html"),
        "base_price": data.get("base_price"),
        "unit": data.get("unit"),
        "markup_riv10": data.get("markup_riv10"),
        "markup_riv": data.get("markup_riv"),
        "markup_dist": data.get("markup_dist"),
        "price_riv10": data.get("price_riv10") or data.get("price_rivenditore10"),
        "price_riv": data.get("price_riv") or data.get("price_rivenditore"),
        "price_dist": data.get("price_dist") or data.get("price_distributore"),
        "price_distributore": data.get("price_distributore") or data.get("price_dist"),
        "price_rivenditore": data.get("price_rivenditore") or data.get("price_riv"),
        "price_rivenditore10": data.get("price_rivenditore10") or data.get("price_riv10"),
        "qty_stock": data.get("qty_stock", 0),
        "discount_dist_percent": data.get("discount_dist_percent", 0),
        "discount_riv_percent": data.get("discount_riv_percent", 0),
        "discount_riv10_percent": data.get("discount_riv10_percent", 0),
        "status": data.get("status") or "attivo",
        "extra_json": extra_json,
    }
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO products (sku, name, codice, image_hd, image_thumb, gallery_json, description_html, base_price, unit, markup_riv10, markup_riv, markup_dist, price_riv10, price_riv, price_dist, extra_json, price_distributore, price_rivenditore, price_rivenditore10, qty_stock, discount_dist_percent, discount_riv_percent, discount_riv10_percent, status)
            VALUES (:sku, :name, :codice, :image_hd, :image_thumb, :gallery_json, :description_html, :base_price, :unit, :markup_riv10, :markup_riv, :markup_dist, :price_riv10, :price_riv, :price_dist, :extra_json, :price_distributore, :price_rivenditore, :price_rivenditore10, :qty_stock, :discount_dist_percent, :discount_riv_percent, :discount_riv10_percent, :status)
            ON CONFLICT(sku) DO UPDATE SET
                name=excluded.name,
                codice=excluded.codice,
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
                price_distributore=excluded.price_distributore,
                price_rivenditore=excluded.price_rivenditore,
                price_rivenditore10=excluded.price_rivenditore10,
                qty_stock=excluded.qty_stock,
                discount_dist_percent=excluded.discount_dist_percent,
                discount_riv_percent=excluded.discount_riv_percent,
                discount_riv10_percent=excluded.discount_riv10_percent,
                status=excluded.status,
                extra_json=excluded.extra_json
            """,
            payload,
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
        data["desc_html"] = data.get("description_html")
        data.setdefault("price_distributore", data.get("price_dist"))
        data.setdefault("price_rivenditore", data.get("price_riv"))
        data.setdefault("price_rivenditore10", data.get("price_riv10"))
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
            d["desc_html"] = d.get("description_html")
            d.setdefault("price_distributore", d.get("price_dist"))
            d.setdefault("price_rivenditore", d.get("price_riv"))
            d.setdefault("price_rivenditore10", d.get("price_riv10"))
            d["id"] = d.get("sku")
            rows.append(d)
        return rows


def delete_product(sku: str) -> int:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM products WHERE sku = ?", (sku,))
        conn.commit()
        return cur.rowcount


def save_import_metadata(file_name: str, total_products: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO price_list_imports (imported_at, file_name, total_products) VALUES (?, ?, ?)",
            (now, file_name, total_products),
        )
        conn.commit()


def set_meta_value(key: str, value: str) -> None:
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO meta (key, value) VALUES (:key, :value)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            {"key": key, "value": value},
        )
        conn.commit()


def get_meta_value(key: str) -> Optional[str]:
    with get_db() as conn:
        cur = conn.execute("SELECT value FROM meta WHERE key = :key", {"key": key})
        row = cur.fetchone()
        if not row:
            return None
        return row["value"]


def save_clients_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    payload = settings or {}
    set_meta_value("admin_clients_settings", json.dumps(payload, ensure_ascii=False))
    return payload


def save_macro_offers(offers: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    if offers:
        payload = [dict(item) for item in offers]
    set_meta_value("admin_macro_offers", json.dumps(payload, ensure_ascii=False))
    return payload


def save_price_list_config(config: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "listino_attivo": config.get("listino_attivo"),
        "ultima_importazione": config.get("ultima_importazione"),
        "flags": config.get("flags") or config.get("flag") or {},
    }
    set_meta_value("price_list_config", json.dumps(payload, ensure_ascii=False))
    if payload["ultima_importazione"]:
        set_meta_value("price_list_last_import_at", payload["ultima_importazione"])
    if payload["listino_attivo"]:
        set_meta_value("price_list_active", str(payload["listino_attivo"]))
    return payload


def save_promo_config(config: Dict[str, Any]) -> Dict[str, Any]:
    actions = config.get("actions") or config.get("actions_list") or []
    adherents = config.get("adherents") or []
    payload = {
        "name": config.get("name") or "",
        "start_date": config.get("start_date"),
        "end_date": config.get("end_date"),
        "description": config.get("description") or "",
        "actions_text": config.get("actions_text") or "",
        "actions_json": json.dumps(actions, ensure_ascii=False),
    }

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO promo_config (id, name, start_date, end_date, description, actions_text, actions_json)
            VALUES (1, :name, :start_date, :end_date, :description, :actions_text, :actions_json)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                description = excluded.description,
                actions_text = excluded.actions_text,
                actions_json = excluded.actions_json
            """,
            payload,
        )
    set_meta_value("promo_adherents", json.dumps(adherents, ensure_ascii=False))
    set_meta_value("promo_actions_text", payload["actions_text"])
    set_meta_value("promo_gift_badge", payload["name"])
    set_meta_value("promo_points_state_last_update", datetime.now(timezone.utc).isoformat())
    return {**payload, "actions": actions, "adherents": adherents}


def get_promo_config() -> Dict[str, Any]:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT name, start_date, end_date, description, actions_text, actions_json FROM promo_config WHERE id = 1"
        )
        row = cur.fetchone()

    if not row:
        return {}

    config = row_to_dict(row)
    try:
        config["actions"] = json.loads(config.get("actions_json") or "[]")
    except Exception:
        config["actions"] = []
    config.pop("actions_json", None)
    return config


# ========== NOTIFICATION SETTINGS ========== #


def get_notification_settings() -> Dict[str, Any]:
    """Restituisce le impostazioni di notifica, creando un record di default."""

    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM notification_settings WHERE scope = ?", ("global",)
        )
        row = cur.fetchone()
        if not row:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """
                INSERT INTO notification_settings (
                    scope,
                    notify_macro_offers,
                    notify_daily_deal,
                    notify_event_offer,
                    notify_order_status,
                    updated_at
                ) VALUES (?, 1, 1, 1, 1, ?)
                """,
                ("global", now),
            )
            conn.commit()
            return {
                "scope": "global",
                "notify_macro_offers": True,
                "notify_daily_deal": True,
                "notify_event_offer": True,
                "notify_order_status": True,
                "updated_at": now,
            }

        data = row_to_dict(row)
        for key in (
            "notify_macro_offers",
            "notify_daily_deal",
            "notify_event_offer",
            "notify_order_status",
        ):
            data[key] = bool(data.get(key))
        return data


def update_notification_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    notify_macro_offers = 1 if payload.get("notify_macro_offers", True) else 0
    notify_daily_deal = 1 if payload.get("notify_daily_deal", True) else 0
    notify_event_offer = 1 if payload.get("notify_event_offer", True) else 0
    notify_order_status = 1 if payload.get("notify_order_status", True) else 0

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO notification_settings (
                scope,
                notify_macro_offers,
                notify_daily_deal,
                notify_event_offer,
                notify_order_status,
                updated_at
            ) VALUES (:scope, :notify_macro_offers, :notify_daily_deal, :notify_event_offer, :notify_order_status, :updated_at)
            ON CONFLICT(scope) DO UPDATE SET
                notify_macro_offers = excluded.notify_macro_offers,
                notify_daily_deal = excluded.notify_daily_deal,
                notify_event_offer = excluded.notify_event_offer,
                notify_order_status = excluded.notify_order_status,
                updated_at = excluded.updated_at
            """,
            {
                "scope": "global",
                "notify_macro_offers": notify_macro_offers,
                "notify_daily_deal": notify_daily_deal,
                "notify_event_offer": notify_event_offer,
                "notify_order_status": notify_order_status,
                "updated_at": now,
            },
        )
        conn.commit()

    return get_notification_settings()


# ========== DAILY OFFER ========== #


def get_daily_offer() -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM daily_offer WHERE id = 1")
        row = cur.fetchone()
        if not row:
            return None
        data = row_to_dict(row)
        data["active"] = bool(data.get("active"))
        return data


def save_daily_offer(data: Dict[str, Any]) -> Dict[str, Any]:
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO daily_offer (id, sku, start_at, end_at, discount_dist_percent, discount_riv_percent, discount_riv10_percent, coupon_code, active, min_qty, product_url)
            VALUES (1, :sku, :start_at, :end_at, :discount_dist_percent, :discount_riv_percent, :discount_riv10_percent, :coupon_code, :active, :min_qty, :product_url)
            ON CONFLICT(id) DO UPDATE SET
                sku=excluded.sku,
                start_at=excluded.start_at,
                end_at=excluded.end_at,
                discount_dist_percent=excluded.discount_dist_percent,
                discount_riv_percent=excluded.discount_riv_percent,
                discount_riv10_percent=excluded.discount_riv10_percent,
                coupon_code=excluded.coupon_code,
                active=excluded.active,
                min_qty=excluded.min_qty,
                product_url=excluded.product_url
            """,
            {
                "sku": data.get("sku"),
                "start_at": data.get("start_at"),
                "end_at": data.get("end_at"),
                "discount_dist_percent": data.get("discount_dist_percent", 0),
                "discount_riv_percent": data.get("discount_riv_percent", 0),
                "discount_riv10_percent": data.get("discount_riv10_percent", 0),
                "coupon_code": data.get("coupon_code"),
                "active": 1 if data.get("active") else 0,
                "min_qty": data.get("min_qty", 1),
                "product_url": data.get("product_url"),
            },
        )
        conn.commit()
    return get_daily_offer() or {}


def delete_daily_offer() -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM daily_offer WHERE id = 1")
        conn.commit()


# ========== ORDERS ========== #

def delete_orders_older_than(days: int = 31) -> int:
    """Rimuove gli ordini più vecchi di *days* giorni.

    Utile per mantenere lo storico limitato all'ultimo mese come richiesto dal
    gestionale.
    """

    threshold = datetime.now(timezone.utc) - timedelta(days=days)
    with get_db() as conn:
        cur = conn.execute(
            "DELETE FROM orders WHERE date(order_date) < date(?)",
            (threshold.date().isoformat(),),
        )
        conn.commit()
        return cur.rowcount


def bulk_insert_orders(orders: List[Dict[str, Any]]) -> int:
    """Inserisce in blocco gli ordini importati dal gestionale."""

    if not orders:
        return 0

    with get_db() as conn:
        cur = conn.executemany(
            """
            INSERT INTO orders (
                document_number,
                status,
                cause,
                customer_name,
                customer_email,
                order_date,
                total_amount,
                external_id,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    o.get("document_number"),
                    o.get("status"),
                    o.get("cause"),
                    o.get("customer_name"),
                    o.get("customer_email"),
                    o.get("order_date"),
                    o.get("total_amount"),
                    o.get("external_id"),
                    o.get("notes"),
                )
                for o in orders
            ],
        )
        conn.commit()
        return cur.rowcount


def list_orders(
    *,
    customer_email: Optional[str] = None,
    customer_name: Optional[str] = None,
    status: Optional[str] = None,
    cause: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    include_all: bool = False,
) -> List[Dict[str, Any]]:
    """Ritorna gli ordini filtrati per utente o per admin.

    - Se *include_all* è True, ignora i filtri su email/nome (visione admin).
    - Altrimenti filtra per email (case-insensitive) e opzionalmente ragione
      sociale (OR) se fornita.
    """

    query = "SELECT * FROM orders WHERE 1=1"
    params: list[Any] = []
    conditions: list[str] = []

    if not include_all:
        if customer_email:
            email_clause = "LOWER(customer_email) = LOWER(?)"
            params.append(customer_email)
            if customer_name:
                email_clause = "(LOWER(customer_email) = LOWER(?) OR LOWER(customer_name) = LOWER(?))"
                params.append(customer_name)
            conditions.append(email_clause)
        elif customer_name:
            conditions.append("LOWER(customer_name) = LOWER(?)")
            params.append(customer_name)
        else:
            # Nessun riferimento utente, ritorniamo lista vuota
            return []

    if status:
        conditions.append("LOWER(status) = LOWER(?)")
        params.append(status)

    if cause:
        conditions.append("LOWER(cause) = LOWER(?)")
        params.append(cause)

    if date_from:
        conditions.append("date(order_date) >= date(?)")
        params.append(date_from)

    if date_to:
        conditions.append("date(order_date) <= date(?)")
        params.append(date_to)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY datetime(order_date) DESC, document_number DESC"

    with get_db() as conn:
        cur = conn.execute(query, tuple(params))
        return [row_to_dict(r) for r in cur.fetchall()]


__all__ = [
    "init_db",
    "get_db",
    "get_user_by_email",
    "create_user",
    "create_session",
    "get_session",
    "list_clients",
    "get_client_by_id",
    "get_client_by_email",
    "find_client_by_email_or_piva",
    "save_client",
    "link_client_to_user_by_email",
    "delete_client",
    "add_promo_points",
    "get_promo_summary",
    "list_discount_rules",
    "clear_discount_rules_for_offer_segment",
    "insert_discount_rule",
    "upsert_product",
    "get_product_by_sku",
    "list_products",
    "save_import_metadata",
    "set_meta_value",
    "get_meta_value",
    "save_clients_settings",
    "save_macro_offers",
    "save_price_list_config",
    "save_promo_config",
    "get_promo_config",
    "get_notification_settings",
    "update_notification_settings",
    "delete_orders_older_than",
    "bulk_insert_orders",
    "list_orders",
]
