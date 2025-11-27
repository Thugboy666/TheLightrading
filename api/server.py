import os
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import ClientSession, web

# ================== CONFIG BASE ==================

BASE_DIR = Path(__file__).resolve().parent.parent
UI_INDEX = BASE_DIR / "ui" / "index.html"

# backend LLM locale (Termux / llama.cpp / phi ecc.)
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
    """
    payload = await request.json()
    try:
        async with ClientSession(timeout=web.ClientTimeout(total=120.0)) as session:
            async with session.post(LLM_BACKEND_URL, json=payload) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except Exception as exc:  # noqa: BLE001 (surface errore al client)
        return web.json_response(
            {"error": "LLM backend non raggiungibile", "detail": str(exc)}, status=502
        )

    content = ""
    if isinstance(data, dict):
        if isinstance(data.get("content"), list):
            content = "".join(chunk.get("text", "") for chunk in data["content"])
        elif isinstance(data.get("content"), str):
            content = data["content"]
        elif "completion" in data:
            content = str(data.get("completion", ""))

    return web.json_response({"content": content, "raw": data})


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
