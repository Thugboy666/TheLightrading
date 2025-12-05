import logging
from typing import List, Optional

from core import node_logger

logger = logging.getLogger(__name__)


async def run_llm_for_node(
    node: dict,
    messages: List[dict],
    system_prompt: Optional[str] = None,
) -> str:
    """Mock LLM adapter.

    Builds a minimal prompt and returns an echo-style response so the
    integration surface is ready for a future real model (e.g. Mistral 7B).
    Errors are recorded as node logs.
    """

    try:
        title = node.get("title", "node")
        recent = messages[-4:]
        user_lines = [m["content"] for m in recent if m.get("role") == "user"]
        user_tail = user_lines[-1] if user_lines else ""
        guidance = system_prompt or "Seed AI reasoning node"
        reply = (
            f"[{title}] {guidance}: Received {len(messages)} messages. "
            f"Latest user note: {user_tail}".strip()
        )
        return reply
    except Exception as exc:  # noqa: BLE001
        logger.exception("LLM adapter error for node %s", node.get("hash"))
        node_logger.log_node_error(
            node.get("hash", "unknown"),
            "LLM adapter failed",
            {"error": str(exc)},
        )
        return "[errore] impossibile completare la risposta LLM in questo momento"
