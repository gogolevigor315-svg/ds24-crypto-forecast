# ds24-proxy-gateway (Bybit-only REALDATA version)
# Реальные данные с Bybit Spot v5, без localhost, без симуляций цены.

import os, time, hmac, hashlib, json, asyncio
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx

APP_NAME = "ds24-proxy-gateway"

# ─────────────────────────────────────────────────────────────
#  PUBLIC BASE URL (Render)
# ─────────────────────────────────────────────────────────────
PUBLIC_BASE = "https://ds24-proxy-gateway.onrender.com"

# ─────────────────────────────────────────────────────────────
#  BYBIT ENDPOINTS (REAL DATA)
# ─────────────────────────────────────────────────────────────
# Один и тот же endpoint:
# - без symbol → все тикеры (для /trade/top10)
# - с symbol   → конкретный тикер (для /live и feeder)
BYBIT_MARKET_URL = os.getenv(
    "BYBIT_MARKET_URL",
    "https://api.bybit.com/v5/market/tickers?category=spot"
)

# UPSTREAM_GRAPH остаётся как есть (может быть внешний OBSERVE+ / GRAPH сервис)
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

# ─────────────────────────────────────────────────────────────
# LOCAL GRAPH STORAGE
# ─────────────────────────────────────────────────────────────

_local_graph: Dict[str, Dict[str, Any]] = {}


def _graph_get(job_id: str) -> Dict[str, Any]:
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
    for k in ("decision_links", "arena_events", "mind_reflect"):
        if k in payload and isinstance(payload[k], list):
            if k == "decision_links":
                for e in payload[k]:
                    src = e.get("from")
                    tgt = e.get("to")

                    if src and not any(n["id"] == src for n in g["nodes"]):
                        g["nodes"].append({
                            "id": src,
                            "label": src.split(":")[-1],
                            "type": "signal",
                            "score": None,
                            "tags": []
                        })

                    if tgt and not any(n["id"] == tgt for n in g["nodes"]):
                        g["nodes"].append({
                            "id": tgt,
                            "label": tgt.split(":")[-1],
                            "type": "signal",
                            "score": None,
                            "tags": []
                        })

                    g["edges"].append({
                        "source": src,
                        "target": tgt,
                        "kind": e.get("kind", "signal"),
                        "weight": e.get("weight", 0.5),
                        "ts": e.get("ts")
                    })

    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _local_graph[job_id] = g


# ─────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────
# HTTP JSON CLIENT (GENERIC)
# ─────────────────────────────────────────────────────────────
async def _get_json(url: str, params: Optional[dict] = None) -> Tuple[bool, Any, int]:
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


# ─────────────────────────────────────────────────────────────
# BYBIT HELPERS
# ─────────────────────────────────────────────────────────────

async def _bybit_ticker(symbol: str) -> Tuple[bool, Optional[dict], int]:
    """
    Реальный запрос к Bybit Spot v5 для одного тикера.
    """
    ok, data, status = await _get_json(BYBIT_MARKET_URL, params={"symbol": symbol})
    if not ok:
        return False, data, status

    try:
        if data.get("retCode") != 0:
            return False, {"error": "bybit_retcode", "raw": data}, 502

        result = data.get("result") or {}
        lst = result.get("list") or []
        if not lst:
            return False, {"error": "empty_list", "raw": data}, 502

        ticker = lst[0]
        return True, ticker, 200
    except Exception as e:
        return False, {"error": "parse_error", "details": str(e), "raw": data}, 502


def _parse_bybit_ticker(symbol: str, ticker: dict, tf: str) -> dict:
    """
    Приводим Bybit-тикер к унифицированному ISKRA3-формату /live.
    Всё основано на реальных полях ответа.
    """
    def _f(x, default=None):
        if x is None:
            return default
        try:
            return float(x)
        except Exception:
            return default

    price = _f(ticker.get("lastPrice"))
    change24 = _f(ticker.get("price24hPcnt"), 0.0)
    vol24 = _f(ticker.get("turnover24h"), 0.0)
    high24 = _f(ticker.get("highPrice24h"), None)
    low24 = _f(ticker.get("lowPrice24h"), None)

    # НЕЛИНЕЙНАЯ, НО РЕАЛЬНАЯ ОЦЕНКА (НЕ СИМУЛЯЦИЯ, А ФУНКЦИЯ ОТ РЫНКА)
    # RSI_approx ~ позиция цены в дневном диапазоне
    if high24 is not None and low24 is not None and high24 > low24 and price is not None:
        rsi_approx = 100.0 * (price - low24) / (high24 - low24)
    else:
        rsi_approx = 50.0

    # vol_index ~ модуль суточного % изменения, нормированный к 10%
    vol_index = min(max(abs(change24) / 0.10, 0.0), 1.0)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "ts": ts,
        "symbol": ticker.get("symbol", symbol),
        "tf": tf,
        "price": price,
        "features": {
            "change24h": change24,
            "volume24h": vol24,
            "high24h": high24,
            "low24h": low24,
            "rsi_approx": rsi_approx,        # 0..100
            "volatility_index": vol_index    # 0..1
        },
        "forecast": {},  # тут пока пусто — это не симуляция, а отсутствие прогноза
        "source": "bybit-spot-v5",
        "q_score": None
    }


# ─────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "ts": int(time.time()),
        "feeder": {
            "enabled": FEEDER_ENABLED,
            "job_id": FEEDER_JOB_ID,
            "interval_sec": FEEDER_INTERVAL_SEC
        },
        "graph_jobs": list(_local_graph.keys())
    }


@app.get("/live")
async def live(symbol: str = Query(...), tf: str = Query("1m")):
    """
    Реальные данные с Bybit Spot v5 по конкретному символу.
    Без localhost, без фейковых цен.
    """
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, ticker, status = await _bybit_ticker(symbol)
    if not ok:
        return JSONResponse(status_code=status, content={"proxy": APP_NAME, "error": ticker})

    event = _parse_bybit_ticker(symbol, ticker, tf)
    _cache_put(key, event)
    return {"cached": False, **event}


@app.get("/graph/{job_id}")
async def graph(job_id: str):
    """
    GraphView: сначала пытаемся сходить в внешний UPSTREAM_GRAPH,
    если не получилось — локальный fallback.
    """
    url = f"{UPSTREAM_GRAPH.rstrip('/')}/{job_id}"
    ok, data, status = await _get_json(url)

    if ok:
        return {"cached": False, **data}

    return {"cached": True, **_graph_get(job_id), "fallback": "local"}


# ─────────────────────────────────────────────────────────────
# INGEST (WITH FALLBACK)
# ─────────────────────────────────────────────────────────────

@app.post("/ingest-pass")
async
