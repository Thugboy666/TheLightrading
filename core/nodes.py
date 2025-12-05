from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import hashlib
import os


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_node_hash(title: str) -> str:
    random_bits = os.urandom(16)
    payload = f"{_now_iso()}-{title}-{random_bits.hex()}".encode()
    return hashlib.sha256(payload).hexdigest()


@dataclass
class Node:
    hash: str
    title: str
    parent_hash: Optional[str] = None
    cluster_hash: Optional[str] = None
    position_x: Optional[float] = None
    position_y: Optional[float] = None
    position_z: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)


@dataclass
class NodeMessage:
    node_hash: str
    role: str
    content: str
    created_at: str = field(default_factory=_now_iso)


@dataclass
class NodeLog:
    node_hash: str
    level: str
    message: str
    payload: Optional[Dict[str, Any]] = None
    created_at: str = field(default_factory=_now_iso)
