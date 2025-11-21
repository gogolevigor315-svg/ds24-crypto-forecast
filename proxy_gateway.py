# ds24-proxy-gateway (Bybit-only REALDATA version, Cloudflare Worker proxy)

import os, time, hmac, hashlib, json, asyncio
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx

APP_NAME = "ds24-proxy-gateway"

# ──────────────────────────────────────────
# PUBLIC BASE URL (Render)
# ──────────────────────────────────────────
PUBLIC_BASE = "https://ds24-proxy-gateway.onrender.com"

# ──────────────────────────────────────────
# BYBIT ENDPOINT → через твой Cloudflare Worker
# ──────────────────────────────────────────
BYBIT_MARKET_URL = os.getenv(
    "BYBIT_MARKET_URL",
    "https://polished-math-8078.gogolevigor315.workers.dev/bybit/v5/market/tickers?category=spot"
)

# Upstream (не трогаем)
UPSTREAM_GRAPH = os.getenv("UPSTREAM_GRAPH", "http://127.0.0.1:8000/observe/graph")

CACHE_TTL = int(os.getenv("CACHE_TTL", "10"))

INGEST_KID    = os.getenv("INGEST_KID", "demo")
INGEST_SECRET = os.getenv("INGEST_SECRET", "")

FEEDER_ENABLED      = os.getenv("FEEDER_ENABLED", "true").lower() in ("1", "true", "yes", "on")
FEEDER_SYMBOL       = os.getenv("FEEDER_SYMBOL", "BTCUSDT")
FEEDER_TF           = os.getenv("FEEDER_TF", "1m")
FEEDER_JOB_ID       = os.getenv("FEEDER_JOB_ID", "portfolio-live")
FEEDER_INTERVAL_SEC = int(os.getenv("FEEDER_INTERVAL_SEC", "15"))

TOP10 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT"
]

# ──────────────────────────────────────────
# LOCAL GRAPH
# ──────────────────────────────────────────

_local_graph: Dict[str, Dict[str, Any]] = {}

def _graph_get(job_id: str):
    return _local_graph.get(job_id, {
        "nodes": [],
        "edges": [],
        "metrics": {
            "dqi_avg": None,
            "risk_cvar": None,
            "finops_cost_usd": None,
            "updated_at": None
        }
    })

def _graph_put(job_id: str, payload: Dict[str, Any]):
    g = _graph_get(job_id)
    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _local_graph[job_id] = g

# ──────────────────────────────────────────
# FASTAPI INIT
# ──────────────────────────────────────────

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────
# CACHE
# ──────────────────────────────────────────

_cache: Dict[str, Tuple[Any, float]] = {}

def _cache_get(key):
    item = _cache.get(key)
    if not item:
        return None
    data, exp = item
    if time.time() > exp:
        _cache.pop(key, None)
        return None
    return data

def _cache_put(key, data, ttl=CACHE_TTL):
    _cache[key] = (data, time.time() + ttl)

# ──────────────────────────────────────────
# HTTP CLIENT
# ──────────────────────────────────────────

async def _get_json(url: str, params=None):
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get(url, params=params)
            ct = (r.headers.get("content-type") or "").lower()

            if r.status_code >= 400:
                return False, {"upstream_error": r.text}, r.status_code

            if "application/json" in ct:
                return True, r.json(), r.status_code

            return True, {"raw": r.text}, r.status_code

    except Exception as e:
        return False, {"error": str(e)}, 502

# ──────────────────────────────────────────
# BYBIT TICKER
# ──────────────────────────────────────────

async def _bybit_ticker(symbol: str):
    ok, data, status = await _get_json(BYBIT_MARKET_URL, params={"symbol": symbol})
    if not ok:
        return False, data, status

    try:
        if data.get("retCode") != 0:
            return False, data, 502

        lst = data.get("result", {}).get("list", [])
        if not lst:
            return False, {"error": "empty_list"}, 502

        return True, lst[0], 200

    except Exception as e:
        return False, {"error": str(e), "raw": data}, 502

# ──────────────────────────────────────────
# PARSE TICKER
# ──────────────────────────────────────────

def _parse_bybit_ticker(symbol, t, tf):
    f = lambda x: float(x) if x not in (None, "") else None
    price = f(t.get("lastPrice"))

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "ts": ts,
        "symbol": symbol,
        "tf": tf,
        "price": price,
        "features": {
            "change24h": f(t.get("price24hPcnt")),
            "volume24h": f(t.get("turnover24h")),
            "high24h": f(t.get("highPrice24h")),
            "low24h":  f(t.get("lowPrice24h")),
        },
        "source": "bybit-spot-v5"
    }

# ──────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────

@app.get("/live")
async def live(symbol: str, tf: str = "1m"):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, ticker, status = await _bybit_ticker(symbol)
    if not ok:
        return JSONResponse(status_code=status, content={"error": ticker})

    event = _parse_bybit_ticker(symbol, ticker, tf)
    _cache_put(key, event)
    return {"cached": False, **event}


@app.get("/trade/top10")
async def trade_top10():
    ok, data, status = await _get_json(BYBIT_MARKET_URL)
    if not ok:
        return JSONResponse(status_code=status, content=data)

    lst = data.get("result", {}).get("list", [])
    filtered = [t for t in lst if t.get("symbol") in TOP10]
    filtered_sorted = sorted(filtered, key=lambda t: TOP10.index(t["symbol"]))

    return {"top10": filtered_sorted}


@app.get("/health")
async def health():
    return {"ok": True, "ts": int(time.time())}

# ──────────────────────────────────────────
# START FEEDER
# ──────────────────────────────────────────

async def _feeder_loop():
    if not FEEDER_ENABLED:
        return

    while True:
        try:
            ok, t, _ = await _bybit_ticker(FEEDER_SYMBOL)
            if ok:
                await asyncio.sleep(0.1)
        except:
            pass

        await asyncio.sleep(FEEDER_INTERVAL_SEC)


@app.on_event("startup")
async def _startup():
    app.state.feeder_task = asyncio.create_task(_feeder_loop())

@app.on_event("shutdown")
async def _shutdown():
    t = getattr(app.state, "feeder_task", None)
    if t:
        t.cancel()
