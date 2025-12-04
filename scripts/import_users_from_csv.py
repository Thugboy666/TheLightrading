#!/usr/bin/env python3
import csv
import sqlite3
from datetime import datetime
from pathlib import Path

import bcrypt

from core.logger import get_logger

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "db" / "thelight_universe.db"
CSV_PATH = BASE_DIR / "data" / "users_plain.csv"

logger = get_logger("thelight24.import_users")


def hash_password(password: str) -> str:
    """Applica hashing bcrypt coerente con l'API."""

    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def main():
    if not CSV_PATH.exists():
        print(f"CSV non trovato: {CSV_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
    if not cur.fetchone():
        print("Errore: tabella 'users' non trovata nel database.")
        conn.close()
        return

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("CSV vuoto, niente da importare.")
        conn.close()
        return

    existing_emails = {row[0].lower() for row in cur.execute("SELECT LOWER(email) FROM users")}
    seen_emails = set()
    inserted = 0
    skipped = 0

    now = datetime.utcnow().isoformat(timespec="seconds")

    for row in rows:
        email = (row.get("email") or "").strip().lower()
        pwd_plain = (row.get("password_plain") or "").strip()
        name = (row.get("name") or "").strip() or None
        tier = (row.get("tier") or "").strip() or "rivenditore10"
        piva = (row.get("piva") or "").strip() or None
        phone = (row.get("phone") or "").strip() or None

        if not email or not pwd_plain:
            logger.warning("Riga utente scartata (manca email o password): %s", row)
            skipped += 1
            continue

        if email in existing_emails or email in seen_emails:
            logger.warning("Email duplicata, skip: %s", email)
            skipped += 1
            continue

        pwd_hash = hash_password(pwd_plain)

        try:
            cur.execute(
                """
                INSERT INTO users (
                    email,
                    password_hash,
                    name,
                    tier,
                    piva,
                    phone,
                    created_at,
                    updated_at,
                    is_admin
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (email, pwd_hash, name, tier, piva, phone, now, now),
            )
            inserted += 1
            seen_emails.add(email)
        except sqlite3.IntegrityError as e:
            logger.warning("UTENTE DUPLICATO O ERRORE (%s): %s", email, e)
            skipped += 1

    conn.commit()
    conn.close()

    print(f"Import completato. Inseriti: {inserted}, Skippati: {skipped}")


if __name__ == "__main__":
    main()
