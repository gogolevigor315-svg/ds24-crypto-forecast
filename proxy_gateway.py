# proxy_gateway.py — robust + built-in feeder (no shell required)
import os, time, hmac, hashlib, json, asyncio
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx

APP_NAME = "ds24-proxy-gateway"

# ── Upstreams (задаются в Render → Environment) ───────────────────────────────
UPSTREAM_LIVE  = os.getenv("UPSTREAM_LIVE",  "http://127.0.0.1:8000/api/live")
UPSTREAM_GRAPH = os.getenv("UPSTREAM_GRAPH", "http://127.0.0.1:8000/observe/graph")
CACHE_TTL      = int(os.getenv("CACHE_TTL", "10"))

# ── HMAC для /ingest-pass (если нужен у твоего render_adapter) ───────────────
INGEST_KID    = os.getenv("INGEST_KID", "demo")
INGEST_SECRET = os.getenv("INGEST_SECRET", "")

# ── Авто-фидер (включён по умолчанию, без shell) ─────────────────────────────
FEEDER_ENABLED      = os.getenv("FEEDER_ENABLED", "true").lower() in ("1","true","yes","on")
FEEDER_SYMBOL       = os.getenv("FEEDER_SYMBOL", "BTCUSDT")
FEEDER_TF           = os.getenv("FEEDER_TF", "1m")
FEEDER_JOB_ID       = os.getenv("FEEDER_JOB_ID", "portfolio-live")
FEEDER_INTERVAL_SEC = int(os.getenv("FEEDER_INTERVAL_SEC", "15"))

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── простой кэш в памяти ─────────────────────────────────────────────────────
_cache: Dict[str, Tuple[Any, float]] = {}

def _cache_get(key: str):
    item = _cache.get(key)
    if not item: return None
    data, exp = item
    if time.time() > exp:
        _cache.pop(key, None)
        return None
    return data

def _cache_put(key: str, data: Any, ttl: int = CACHE_TTL):
    _cache[key] = (data, time.time() + ttl)

# ── HTTP helper с мягкой обработкой ошибок ───────────────────────────────────
async def _get_json(url: str, params: Optional[dict] = None) -> Tuple[bool, Any, int]:
    """Возвращает (ok, payload, status_code). Не кидает исключения наружу."""
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get(url, params=params)
            ct = (r.headers.get("content-type") or "").lower()
            if r.status_code >= 400:
                body = r.text if "application/json" not in ct else r.json()
                return False, {"upstream_status": r.status_code, "upstream_body": body}, r.status_code
            if "application/json" in ct:
                return True, r.json(), r.status_code
            return True, {"raw": r.text}, r.status_code
    except httpx.HTTPError as e:
        return False, {"upstream_error": str(e), "url": url, "params": params}, 502

# ── health ───────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"ok": True, "app": APP_NAME, "ts": int(time.time()),
            "feeder": {"enabled": FEEDER_ENABLED, "job_id": FEEDER_JOB_ID, "interval_sec": FEEDER_INTERVAL_SEC}}

# ── LIVE → forecast.py (/api/live) ───────────────────────────────────────────
@app.get("/live")
async def live(symbol: str = Query(...), tf: str = Query("1m")):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, data, status = await _get_json(UPSTREAM_LIVE, params={"symbol": symbol, "tf": tf})
    if not ok:
        return JSONResponse(status_code=status, content={"proxy": APP_NAME, "error": data})

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

# ── GRAPH → render_adapter.py (/observe/graph/{job_id}) ──────────────────────
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

# ── INGEST pass-through с HMAC (опц.) ────────────────────────────────────────
@app.post("/ingest-pass")
async def ingest_pass(payload: dict):
    url = f"{UPSTREAM_GRAPH.rstrip('/')}/ingest"
    headers = {"Content-Type": "application/json"}
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    if INGEST_SECRET:
        sig = hmac.new(INGEST_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
        headers.update({"x-kid": INGEST_KID, "x-ts": str(int(time.time()*1000)), "x-signature": sig})

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

# ── BACKGROUND FEEDER (работает внутри Render, без shell) ────────────────────
async def _feeder_loop():
    """Каждые FEEDER_INTERVAL_SEC: /live → строим payload → /ingest-pass."""
    if not FEEDER_ENABLED:
        return
    while True:
        try:
            async with httpx.AsyncClient(timeout=6.0) as c:
                # 1) Получаем live от апстрима
                r = await c.get(UPSTREAM_LIVE, params={"symbol": FEEDER_SYMBOL, "tf": FEEDER_TF})
                evt = r.json() if "application/json" in (r.headers.get("content-type","").lower()) else {}

                # 2) Достаём фичи
                feat = evt.get("features", {}) if isinstance(evt, dict) else {}
                rsi = float(feat.get("rsi", 0.5) or 0.5)
                vol = float(feat.get("vol", 0.5) or 0.5)
                ts  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                # 3) Собираем граф-payload
                payload = {
                    "job_id": FEEDER_JOB_ID,
                    "decision_links": [
                        {"from":"source:live","to":"signal:rsi","kind":"emits","weight": round(min(max(rsi/100.0, 0.0),1.0),3),"ts": ts},
                        {"from":"source:live","to":"signal:vol","kind":"emits","weight": round(min(max(vol, 0.0),1.0),3),"ts": ts},
                        {"from":"signal:rsi","to":"policy:finops_guard","kind":"influences","weight":0.6,"ts": ts},
                        {"from":"signal:vol","to":"risk:cvar","kind":"influences","weight":0.7,"ts": ts}
                    ],
                    "arena_events": [
                        {"id": f"tick-{ts}","policy":"policy:finops_guard","outcome":0.001,"finops":0.017,"dqi":0.90,"ts": ts}
                    ],
                    "mind_reflect": [
                        {"id": f"note-{ts}","text":"live→graph via built-in feeder","tags":["reflect","live"],"ts": ts}
                    ]
                }

                # 4) Шлём в граф напрямую через тот же код, что в /ingest-pass
                url = f"{UPSTREAM_GRAPH.rstrip('/')}/ingest"
                headers = {"Content-Type": "application/json"}
                body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
                if INGEST_SECRET:
                    sig = hmac.new(INGEST_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
                    headers.update({"x-kid": INGEST_KID, "x-ts": str(int(time.time()*1000)), "x-signature": sig})
                await c.post(url, headers=headers, content=body)
        except Exception:
            # глушим любые сбои, чтобы не падал процесс
            pass
        await asyncio.sleep(FEEDER_INTERVAL_SEC)

@app.on_event("startup")
async def _on_startup():
    if FEEDER_ENABLED:
        app.state.feeder_task = asyncio.create_task(_feeder_loop())

@app.on_event("shutdown")
async def _on_shutdown():
    task = getattr(app.state, "feeder_task", None)
    if task:
        task.cancel()
