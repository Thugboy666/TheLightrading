import asyncio
import aiohttp

from llm.prompts_loader import get_system_prompt
from core.config import settings
from core.logger import get_logger

llm_logger = get_logger("thelight24.llm")


async def complete_text(
    prompt: str,
    max_tokens: int = 256,
    temperature: float = 0.7,
    *,
    timeout: float = 120.0,
    fallback_text: str = "[LLM non disponibile in questo momento. Riprova più tardi.]",
) -> str:
    """Esegue una completion sul backend LLM con gestione errori robusta."""

    url = settings.LLM_COMPLETION_URL
    system_prompt = get_system_prompt("system")
    payload = {
        "model": settings.LLM_MODEL,
        "prompt": f"{system_prompt}\n\n{prompt}",
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    llm_logger.info("Calling LLM at %s", url)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=timeout) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as exc:
        llm_logger.error("Errore durante la richiesta LLM (%s): %s", url, exc, exc_info=True)
        return fallback_text

    if "choices" in data and data["choices"]:
        return data["choices"][0].get("text", "").strip() or fallback_text

    return str(data) if data else fallback_text
