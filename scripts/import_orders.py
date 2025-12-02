#!/usr/bin/env python3
"""Importa gli ordini mensili esportati dal gestionale.

Questo script viene richiamato da ``import_orders.sh`` (cron/manuale) e legge un
file CSV/XLSX con le colonne principali dell'ordine. Configura il percorso del
file con ``--input`` o variabile ``ORDERS_INPUT_FILE``.
"""

import argparse
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional
import sys

# Root del progetto TheLight24 (cartella che contiene "api", "scripts", "data", ecc.)
BASE_DIR = Path(__file__).resolve().parent.parent

# Assicura che la root del progetto sia nel PYTHONPATH così "api" è importabile
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from api.db import bulk_insert_orders, delete_orders_older_than, init_db

try:
    from openpyxl import load_workbook
except Exception:  # pragma: no cover - import facoltativo in ambienti minimal
    load_workbook = None

DEFAULT_INPUT = Path(
    os.environ.get("ORDERS_INPUT_FILE", BASE_DIR / "data" / "orders_latest.csv")
)


def parse_date(value: Optional[str]) -> Optional[str]:
    """Prova a normalizzare la data dell'ordine in ISO (YYYY-MM-DD)."""

    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    text = str(value).strip()
    if not text:
        return None

    patterns = [
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue

    # come fallback tentiamo a interpretare direttamente da datetime.fromisoformat
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except Exception:
        return None


def safe_number(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def load_csv_rows(path: Path) -> Iterable[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def load_xlsx_rows(path: Path) -> Iterable[Dict[str, str]]:
    if load_workbook is None:
        raise RuntimeError("openpyxl non disponibile nell'ambiente corrente")

    wb = load_workbook(path, read_only=True)
    ws = wb.active
    header_row = next(ws.iter_rows(min_row=1, max_row=1))
    headers = [
        (str(cell.value).strip().lower() if cell.value is not None else "")
        for cell in header_row
    ]
    for row in ws.iter_rows(min_row=2, values_only=True):
        yield {headers[i]: (row[i] if i < len(row) else None) for i in range(len(headers))}


def normalize_row(raw: Dict[str, str]) -> Optional[Dict[str, str]]:
    # Mappatura campi con fallback su nomi alternativi
    def pick(*keys: str) -> str:
        for key in keys:
            if key in raw and raw.get(key) not in (None, ""):
                return str(raw.get(key)).strip()
        return ""

    document_number = pick("numero documento", "documento", "doc", "document_number")
    status = pick("stato", "status")
    cause = pick("causale", "cause")
    customer_name = pick("ragione sociale", "cliente", "customer_name")
    customer_email = pick("email", "email cliente", "customer_email")
    order_date = parse_date(pick("data ordine", "data", "order_date"))
    total_amount = safe_number(pick("totale", "importo", "total_amount"))
    external_id = pick("id gestionale", "external_id")
    notes = pick("note", "notes")

    if not document_number or not customer_email:
        return None

    return {
        "document_number": document_number,
        "status": status,
        "cause": cause,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "order_date": order_date,
        "total_amount": total_amount,
        "external_id": external_id,
        "notes": notes,
    }


def load_orders(path: Path) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    loader = load_csv_rows
    if path.suffix.lower() in {".xlsx", ".xlsm"}:
        loader = load_xlsx_rows

    for row in loader(path):
        try:
            normalized = normalize_row({(k or "").lower(): v for k, v in row.items()})
            if not normalized:
                print(f"Riga ignorata (campi obbligatori mancanti): {row}")
                continue
            records.append(normalized)
        except Exception as exc:  # noqa: BLE001
            print(f"Errore parsing riga {row}: {exc}")
            continue
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Import ordini mensili")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Percorso file CSV/XLSX esportato dal gestionale")
    parser.add_argument("--retention-days", type=int, default=31, help="Giorni di storico da mantenere (default: 31)")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERRORE] File di input non trovato: {input_path}")
        print("Configura il percorso nel cron o passa --input /percorso/file.xlsx")
        return

    init_db()

    removed = delete_orders_older_than(args.retention_days)
    print(f"Ordini rimossi perché più vecchi di {args.retention_days} giorni: {removed}")

    try:
        orders = load_orders(input_path)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERRORE] Impossibile leggere il file {input_path}: {exc}")
        return

    inserted = bulk_insert_orders(orders)
    print(f"Ordini importati: {inserted} su {len(orders)} righe valide")
    print("Esegui questo script via cron per aggiornare ogni mese lo storico ordini.")


if __name__ == "__main__":
    main()
