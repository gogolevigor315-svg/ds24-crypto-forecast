# ds24-proxy-gateway (Bybit-only REALDATA version, Cloudflare Worker proxy)

import os
import time
import hmac
import hashlib
import json
import asyncio
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx

APP_NAME = "ds24-proxy-gateway"

# ──────────────────────────────────────────
# URL-ы
# ──────────────────────────────────────────

# Публичный URL самого гейтвея (Render) — ИСПОЛЬЗУЕТСЯ СНАРУЖИ
GATEWAY_PUBLIC_URL = "https://ds24-proxy-gateway.onrender.com"

# Cloudflare Worker, который ходит в Bybit (REALDATA)
WORKER_BASE = "https://polished-math-8078.gogolevigor315.workers.dev"

# Bybit Spot v5 через Cloudflare Worker
BYBIT_MARKET_URL = os.getenv(
    "BYBIT_MARKET_URL",
    f"{WORKER_BASE}/bybit/v5/market/tickers?category=spot",
)

# Внешний Graph-движок (оставляем как есть, если понадобится)
UPSTREAM_GRAPH = os.getenv("UPSTREAM_GRAPH", "http://127.0.0.1:8000/observe/graph")

# ──────────────────────────────────────────
# НАСТРОЙКИ
# ──────────────────────────────────────────

CACHE_TTL = int(os.getenv("CACHE_TTL", "10"))

INGEST_KID = os.getenv("INGEST_KID", "demo")
INGEST_SECRET = os.getenv("INGEST_SECRET", "")

FEEDER_ENABLED = os.getenv("FEEDER_ENABLED", "true").lower() in ("1", "true", "yes", "on")
FEEDER_SYMBOL = os.getenv("FEEDER_SYMBOL", "BTCUSDT")
FEEDER_TF = os.getenv("FEEDER_TF", "1m")
FEEDER_JOB_ID = os.getenv("FEEDER_JOB_ID", "portfolio-live")
FEEDER_INTERVAL_SEC = int(os.getenv("FEEDER_INTERVAL_SEC", "15"))

TOP10 = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "TONUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
]

# ──────────────────────────────────────────
# LOCAL GRAPH (пока минимум, без сложной логики)
# ──────────────────────────────────────────

_local_graph: Dict[str, Dict[str, Any]] = {}


def _graph_get(job_id: str) -> Dict[str, Any]:
    return _local_graph.get(
        job_id,
        {
            "nodes": [],
            "edges": [],
            "metrics": {
                "dqi_avg": None,
                "risk_cvar": None,
                "finops_cost_usd": None,
                "updated_at": None,
            },
        },
    )


def _graph_put(job_id: str, payload: Dict[str, Any]) -> None:
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
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────
# CACHE
# ──────────────────────────────────────────

_cache: Dict[str, Tuple[Any, float]] = {}


def _cache_get(key: str) -> Optional[Any]:
    item = _cache.get(key)
    if not item:
        return None
    data, exp = item
    if time.time() > exp:
        _cache.pop(key, None)
        return None
    return data


def _cache_put(key: str, data: Any, ttl: int = CACHE_TTL) -> None:
    _cache[key] = (data, time.time() + ttl)


# ──────────────────────────────────────────
# HTTP CLIENT
# ──────────────────────────────────────────

async def _get_json(url: str, params: Optional[dict] = None) -> Tuple[bool, Any, int]:
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get(url, params=params)
            ct = (r.headers.get("content-type") or "").lower()

            if r.status_code >= 400:
                body = r.text
                if "application/json" in ct:
                    try:
                        body = r.json()
                    except Exception:
                        pass
                return False, {"upstream_status": r.status_code, "upstream_body": body}, r.status_code

            if "application/json" in ct:
                return True, r.json(), r.status_code

            return True, {"raw": r.text}, r.status_code

    except Exception as e:
        return False, {"error": str(e)}, 502


# ──────────────────────────────────────────
# BYBIT TICKER
# ──────────────────────────────────────────

async def _bybit_ticker(symbol: str) -> Tuple[bool, Any, int]:
    ok, data, status = await _get_json(BYBIT_MARKET_URL, params={"symbol": symbol})
    if not ok:
        return False, data, status

    try:
        if data.get("retCode") != 0:
            return False, {"error": "bybit_retcode", "raw": data}, 502

        lst = (data.get("result") or {}).get("list") or []
        if not lst:
            return False, {"error": "empty_list", "raw": data}, 502

        return True, lst[0], 200

    except Exception as e:
        return False, {"error": "parse_error", "details": str(e), "raw": data}, 502


# ──────────────────────────────────────────
# PARSE TICKER → UNIFIED LIVE FORMAT
# ──────────────────────────────────────────

def _safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None or x == "":
        return default
    try:
        return float(x)
    except Exception:
        return default


def _parse_bybit_ticker(symbol: str, t: dict, tf: str) -> dict:
    price = _safe_float(t.get("lastPrice"))

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "ts": ts,
        "symbol": symbol,
        "tf": tf,
        "price": price,
        "features": {
            "change24h": _safe_float(t.get("price24hPcnt")),
            "volume24h": _safe_float(t.get("turnover24h")),
            "high24h": _safe_float(t.get("highPrice24h")),
            "low24h": _safe_float(t.get("lowPrice24h")),
        },
        "forecast": {},
        "source": "bybit-spot-v5",
        "q_score": None,
    }


# ──────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────

@app.get("/live")
async def live(symbol: str = Query(...), tf: str = Query("1m")):
    """
    Реальные данные с Bybit Spot v5 через Cloudflare Worker.
    """
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
    """
    Топ-10 спот-тикеров Bybit (чистые числовые поля).
    """
    ok, data, status = await _get_json(BYBIT_MARKET_URL)
    if not ok:
        return JSONResponse(status_code=status, content=data)

    if data.get("retCode") != 0:
        return JSONResponse(
            status_code=502,
            content={"error": "invalid_response_from_bybit", "raw": data},
        )

    result = data.get("result") or {}
    lst = result.get("list") or []

    filtered = [t for t in lst if t.get("symbol") in TOP10]
    filtered_sorted = sorted(filtered, key=lambda t: TOP10.index(t["symbol"]))

    cleaned = [
        {
            "symbol": t.get("symbol"),
            "price": _safe_float(t.get("lastPrice")),
            "change24h": _safe_float(t.get("price24hPcnt")),
            "volume": _safe_float(t.get("turnover24h")),
            "high": _safe_float(t.get("highPrice24h")),
            "low": _safe_float(t.get("lowPrice24h")),
            "bid1": _safe_float(t.get("bid1Price")),
            "ask1": _safe_float(t.get("ask1Price")),
        }
        for t in filtered_sorted
    ]

    return {"top10": cleaned}


@app.get("/health")
async def health():
    """
    Простой health-check для ISKRA / Render / мониторинга.
    """
    return {
        "ok": True,
        "app": APP_NAME,
        "ts": int(time.time()),
        "feeder": {
            "enabled": FEEDER_ENABLED,
            "symbol": FEEDER_SYMBOL,
            "tf": FEEDER_TF,
            "job_id": FEEDER_JOB_ID,
            "interval_sec": FEEDER_INTERVAL_SEC,
        },
        "graph_jobs": list(_local_graph.keys()),
        "gateway_public": GATEWAY_PUBLIC_URL,
        "worker_base": WORKER_BASE,
    }


# ──────────────────────────────────────────
# FEEDER (минимальный, без Graph ingest)
# ──────────────────────────────────────────

async def _feeder_loop():
    if not FEEDER_ENABLED:
        return

    while True:
        try:
            ok, t, _ = await _bybit_ticker(FEEDER_SYMBOL)
            if not ok:
                # Можно добавить логирование, если нужно
                pass
            else:
                # Здесь можно в будущем подвесить ingest в Graph
                _ = t  # заглушка, чтобы не ругался линтер
        except Exception:
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
