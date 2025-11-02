# proxy_gateway.py — robust version
import os, time, hmac, hashlib, json
from typing import Optional, Tuple, Dict, Any
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx

APP_NAME = "ds24-proxy-gateway"

# Куда проксируем (задано в Environment Render)
UPSTREAM_LIVE = os.getenv("UPSTREAM_LIVE", "http://127.0.0.1:8000/api/live")
UPSTREAM_GRAPH = os.getenv("UPSTREAM_GRAPH", "http://127.0.0.1:8000/observe/graph")
CACHE_TTL = int(os.getenv("CACHE_TTL", "10"))

# HMAC для /ingest-pass (опционально)
INGEST_KID = os.getenv("INGEST_KID", "demo")
INGEST_SECRET = os.getenv("INGEST_SECRET", "")

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── простой кэш в памяти ─────────────────────────────────────
_cache: Dict[str, Tuple[Any, float]] = {}

def _cache_get(key: str):
    item = _cache.get(key)
    if not item:
        return None
    data, exp = item
    if time.time() > exp:
        _cache.pop(key, None)
        return None
    return data

def _cache_put(key: str, data: Any, ttl: int = CACHE_TTL):
    _cache[key] = (data, time.time() + ttl)

# ── HTTP helper с мягкой обработкой ошибок ───────────────────
async def _get_json(url: str, params: Optional[dict] = None) -> Tuple[bool, Any, int]:
    """
    Возвращает (ok, payload, status_code).
    Никогда не бросает исключения наружу — прокси не валится 500-кой.
    """
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get(url, params=params)
            ct = (r.headers.get("content-type") or "").lower()
            if r.status_code >= 400:
                # пробрасываем тело ошибки апстрима
                body = r.text if "application/json" not in ct else r.json()
                return False, {"upstream_status": r.status_code, "upstream_body": body}, r.status_code
            if "application/json" in ct:
                return True, r.json(), r.status_code
            # не-JSON — отдадим как raw
            return True, {"raw": r.text}, r.status_code
    except httpx.HTTPError as e:
        return False, {"upstream_error": str(e), "url": url, "params": params}, 502

# ── health ────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"ok": True, "app": APP_NAME, "ts": int(time.time())}

# ── LIVE → forecast.py (/api/live) ───────────────────────────
@app.get("/live")
async def live(symbol: str = Query(...), tf: str = Query("1m")):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, data, status = await _get_json(UPSTREAM_LIVE, params={"symbol": symbol, "tf": tf})
    if not ok:
        # Не уроняем прокси — отдаём понятный JSON и статус апстрима
        return JSONResponse(status_code=status, content={"proxy": APP_NAME, "error": data})

    # нормализуем ключи под SpineEventCryptoV1
    event = {
        "ts": data.get("ts"),
        "symbol": data.get("symbol", symbol),
        "tf": data.get("tf", tf),
        "price": data.get("price"),
        "features": data.get("features", {}),
        "forecast": data.get("forecast", {}),
        "source": data.get("source", "ds24-crypto-forecast"),
        "q_score": data.get("q_score"),
    }
    _cache_put(key, event)
    return {"cached": False, **event}

# ── GRAPH → render_adapter.py (/observe/graph/{job_id}) ──────
@app.get("/graph/{job_id}")
async def graph(job_id: str):
    key = f"graph:{job_id}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    url = f"{UPSTREAM_GRAPH.rstrip('/')}/{job_id}"
    ok, data, status = await _get_json(url)
    if not ok:
        return JSONResponse(status_code=status, content={"proxy": APP_NAME, "error": data})

    _cache_put(key, data)
    return {"cached": False, **data}

# ── INGEST pass-through с HMAC (опц.) ────────────────────────
@app.post("/ingest-pass")
async def ingest_pass(payload: dict):
    url = f"{UPSTREAM_GRAPH.rstrip('/')}/ingest"
    headers = {"Content-Type": "application/json"}
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    if INGEST_SECRET:
        sig = hmac.new(INGEST_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
        headers.update({"x-kid": INGEST_KID, "x-ts": str(int(time.time() * 1000)), "x-signature": sig})

    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.post(url, headers=headers, content=body)
            ct = (r.headers.get("content-type") or "").lower()
            if r.status_code >= 400:
                body_err = r.text if "application/json" not in ct else r.json()
                return JSONResponse(status_code=r.status_code, content={"proxy": APP_NAME, "upstream_body": body_err})
            return r.json() if "application/json" in ct else {"raw": r.text}
    except httpx.HTTPError as e:
        return JSONResponse(status_code=502, content={"proxy": APP_NAME, "upstream_error": str(e)})
