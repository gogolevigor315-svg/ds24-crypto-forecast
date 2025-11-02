# proxy_gateway.py
import os, time, hmac, hashlib, json
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx

APP_NAME = "ds24-proxy-gateway"

# ВНУТРЕННИЕ эндпоинты твоих модулей (те, что уже работают)
# если forecast.py и render_adapter.py подняты в одном процессе — укажи их URL-ы
UPSTREAM_LIVE = os.getenv("UPSTREAM_LIVE", "http://127.0.0.1:8000/api/live")
UPSTREAM_GRAPH = os.getenv("UPSTREAM_GRAPH", "http://127.0.0.1:8000/observe/graph")

# опционально для подписи ingest
INGEST_KID = os.getenv("INGEST_KID", "demo")
INGEST_SECRET = os.getenv("INGEST_SECRET", "")  # оставь пустым, если не требуется
CACHE_TTL = int(os.getenv("CACHE_TTL", "10"))

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["GET","POST"], allow_headers=["*"]
)

_cache = {}
def _cache_get(k):
    v = _cache.get(k)
    if not v: return None
    data, exp = v
    if time.time() > exp:
        _cache.pop(k, None); return None
    return data

def _cache_put(k, data, ttl=CACHE_TTL):
    _cache[k] = (data, time.time() + ttl)

async def _get_json(url, params=None):
    async with httpx.AsyncClient(timeout=6.0) as c:
        r = await c.get(url, params=params)
        r.raise_for_status()
        return r.json()

@app.get("/health")
async def health():
    return {"ok": True, "app": APP_NAME, "ts": int(time.time())}

# 1) LIVE → forecast.py
@app.get("/live")
async def live(symbol: str = Query(...), tf: str = Query("1m")):
    key = f"live:{symbol}:{tf}"
    c = _cache_get(key)
    if c: return {"cached": True, **c}
    data = await _get_json(UPSTREAM_LIVE, params={"symbol": symbol, "tf": tf})
    event = {
        "ts": data.get("ts"),
        "symbol": data.get("symbol", symbol),
        "tf": data.get("tf", tf),
        "price": data.get("price"),
        "features": data.get("features", {}),
        "forecast": data.get("forecast", {}),
        "source": data.get("source", "ds24-crypto-forecast"),
        "q_score": data.get("q_score")
    }
    _cache_put(key, event)
    return {"cached": False, **event}

# 2) GRAPH → render_adapter.py
@app.get("/graph/{job_id}")
async def graph(job_id: str):
    key = f"graph:{job_id}"
    c = _cache_get(key)
    if c: return {"cached": True, **c}
    url = f"{UPSTREAM_GRAPH.rstrip('/')}/{job_id}"
    data = await _get_json(url)
    _cache_put(key, data)
    return {"cached": False, **data}

# 3) (опц.) INGEST pass-through c HMAC
@app.post("/ingest-pass")
async def ingest_pass(payload: dict):
    url = f"{UPSTREAM_GRAPH.rstrip('/')}/ingest"
    headers = {"Content-Type": "application/json"}
    if INGEST_SECRET:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        sig = hmac.new(INGEST_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
        headers.update({"x-kid": INGEST_KID, "x-ts": str(int(time.time()*1000)), "x-signature": sig})
    async with httpx.AsyncClient(timeout=6.0) as c:
        r = await c.post(url, headers=headers, json=payload)
        r.raise_for_status()
        return r.json()
