import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
from aiohttp import web

# ================== CONFIG BASE ==================

BASE_DIR = Path(__file__).resolve().parent.parent
UI_INDEX = BASE_DIR / "gui" / "index.html"

# backend LLM locale (Termux / llama.cpp / phi ecc.)
#   - LLM_BACKEND_URL ha priorità e può puntare già all'endpoint completo
#   - altrimenti usiamo THELIGHT_LLM_BASE_URL / completions di default
LLM_BACKEND_URL = os.environ.get("LLM_BACKEND_URL", "http://127.0.0.1:8081/completion")


# ================== FALLBACK UTILS ==================

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_price(sku: str, base_price: float, customer_segment: str, quantity: int):
    """
    Fallback stupidamente funzionante:
    - rivenditore10: prezzo base
    - rivenditore: -10%
    - distributore: -20%
    - ospite/altro: +10%
    """
    factor = {
        "rivenditore10": 1.0,
        "rivenditore": 0.9,
        "distributore": 0.8,
    }.get(customer_segment, 1.1)
    price = base_price * factor
    return {
        "sku": sku,
        "base_price": base_price,
        "segment": customer_segment,
        "quantity": quantity,
        "price": price,
    }


# ================== HANDLER FRONTEND ==================

async def ui_index(request: web.Request) -> web.Response:
    """Serve la GUI 3D (index.html)."""
    return web.FileResponse(path=UI_INDEX)


# ================== SYSTEM ==================

async def health(request: web.Request) -> web.Response:
    data = {
        "status": "ok",
        "project": "TheLight24 v7",
        "env": "dev",
        "time": now_iso(),
    }
    return web.json_response(data)


# ================== AUTH (STUB) ==================

async def auth_register(request: web.Request) -> web.Response:
    body = await request.json()
    name = body.get("name") or body.get("email", "ospite")
    tier = "rivenditore10"
    return web.json_response({"status": "ok", "name": name, "tier": tier})


async def auth_login(request: web.Request) -> web.Response:
    body = await request.json()
    name = body.get("email", "utente")
    tier = "rivenditore10"
    return web.json_response({"status": "ok", "name": name, "tier": tier})


# ================== ECOMMERCE ==================

async def pricing(request: web.Request) -> web.Response:
    """
    Endpoint usato dalla GUI:
    POST /ecom/pricing
    body: { sku, base_price, customer_segment, quantity }
    """
    body = await request.json()
    sku = body.get("sku", "UNKNOWN")
    base_price = float(body.get("base_price", 0))
    segment = body.get("customer_segment", "ospite")
    qty = int(body.get("quantity", 1))

    result = compute_price(sku, base_price, segment, qty)
    return web.json_response(result)


async def order_draft(request: web.Request) -> web.Response:
    body = await request.json()
    return web.json_response({"status": "ok", "received": body})


async def product_update(request: web.Request) -> web.Response:
    body = await request.json()
    return web.json_response({"status": "ok", "product": body})


# ================== LLM LOCALE ==================

async def llm_complete(request: web.Request) -> web.Response:
    """
    Endpoint interno TheLight24 (non usato dalla GUI attuale)
    POST /llm/complete
    """
    body = await request.json()
    prompt = body.get("prompt", "")
    max_tokens = int(body.get("max_tokens", 128))

    # stub finché non colleghi un client reale
    result = {
        "completion": f"[LLM stub interno] Richiesta: {prompt[:80]}...",
        "max_tokens": max_tokens,
    }
    return web.json_response(result)


async def llm_chat(request: web.Request) -> web.Response:
    """
    Endpoint usato dalla GUI:
    LLM_URL = '/api/llm/chat'
    Proxi verso il backend 127.0.0.1:8081/completion
    Normalizziamo SEMPRE in: {"content": "..."}
    """
    payload = await request.json()

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(LLM_BACKEND_URL, json=payload)
    except Exception as e:  # noqa: BLE001
        return web.json_response(
            {"error": f"impossibile contattare backend LLM: {e}"},
            status=502,
        )

    # Proviamo a fare parse JSON, altrimenti teniamo il testo puro
    text_body = r.text
    try:
        raw = r.json()
    except Exception:  # noqa: BLE001
        raw = None

    content = ""

    if isinstance(raw, dict):
        # 1) Caso classico llama.cpp: {"content": "...."}
        if isinstance(raw.get("content"), str):
            content = raw["content"]
        # 2) content come lista di pezzi
        elif isinstance(raw.get("content"), list):
            parts = []
            for p in raw["content"]:
                if isinstance(p, str):
                    parts.append(p)
                elif isinstance(p, dict) and isinstance(p.get("content"), str):
                    parts.append(p["content"])
            content = "".join(parts)
        # 3) stile OpenAI-like con "choices"
        elif isinstance(raw.get("choices"), list) and raw["choices"]:
            ch = raw["choices"][0]
            if isinstance(ch.get("text"), str):
                content = ch["text"]
            elif isinstance(ch.get("message"), dict) and isinstance(
                ch["message"].get("content"), str
            ):
                content = ch["message"]["content"]
        # Fallback: json intero
        else:
            content = text_body or str(raw)
    else:
        # Nessun JSON decente, usiamo il body testuale
        content = text_body or "[risposta LLM vuota]"

    content = (content or "").strip()
    if not content:
        content = "[LLM non ha restituito testo utile]"

    return web.json_response({"content": content})


# ================== APP FACTORY ==================

def create_app() -> web.Application:
    app = web.Application()

    # GUI
    app.router.add_get("/", ui_index)

    # SYSTEM
    app.router.add_get("/system/health", health)

    # AUTH (stub locale)
    app.router.add_post("/auth/register", auth_register)
    app.router.add_post("/auth/login", auth_login)

    # ECOM
    app.router.add_post("/ecom/pricing", pricing)
    app.router.add_post("/ecom/order_draft", order_draft)
    app.router.add_post("/ecom/product/update", product_update)

    # LLM
    app.router.add_post("/llm/complete", llm_complete)
    app.router.add_post("/api/llm/chat", llm_chat)

    return app


app = create_app()


def main() -> None:
    """Avvia il server API aiohttp."""
    host = os.environ.get("API_HOST", "127.0.0.1")
    port = int(os.environ.get("API_PORT", 8080))
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()
