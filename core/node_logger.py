from typing import Any, Dict, Optional

from api import db


def log_node_info(node_hash: str, message: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return db.add_node_log(node_hash, "info", message, payload)


def log_node_warning(node_hash: str, message: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return db.add_node_log(node_hash, "warning", message, payload)


def log_node_error(node_hash: str, message: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return db.add_node_log(node_hash, "error", message, payload)
