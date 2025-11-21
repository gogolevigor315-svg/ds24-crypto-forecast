# ds24-proxy-gateway (REALDATA: Bybit via Cloudflare Worker)
# + технически чистый API под ISKRA3

import os
import time
import json
import asyncio
from typing import Dict, Any, Tuple, Optional, List
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

APP_NAME = "ds24-proxy-gateway"

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

PUBLIC_BASE = os.getenv(
    "PUBLIC_BASE",
    "https://ds24-crypto-forecast-1.onrender.com",
)

# Cloudflare Worker, который уже ходит на Bybit и обходит CloudFront
BYBIT_WORKER_BASE = os.getenv(
    "BYBIT_WORKER_BASE",
    "https://polished-math-8078.gogolevigor315.workers.dev",
)

BYBIT_WORKER_TICKER_PATH = "/bybit/v5/market/tickers"

CACHE_TTL = int(os.getenv("CACHE_TTL", "8"))

FEEDER_ENABLED = os.getenv("FEEDER_ENABLED", "true").lower() in ("1", "true", "yes", "on")
FEEDER_SYMBOL = os.getenv("FEEDER_SYMBOL", "BTCUSDT")
FEEDER_TF = os.getenv("FEEDER_TF", "1m")
FEEDER_JOB_ID = os.getenv("FEEDER_JOB_ID", "portfolio-live")
FEEDER_INTERVAL_SEC = int(os.getenv("FEEDER_INTERVAL_SEC", "20"))

TOP10 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT",
]

# ─────────────────────────────────────────────
# LOCAL GRAPH STORAGE (для /graph и /ingest-pass)
# ─────────────────────────────────────────────

_local_graph: Dict[str, Dict[str, Any]] = {}


def _graph_get(job_id: str) -> Dict[str, Any]:
    return _local_graph.get(job_id, {
        "nodes": [],
        "edges": [],
        "metrics": {
            "dqi_avg": None,
            "risk_cvar": None,
            "finops_cost_usd": None,
            "updated_at": None,
        },
    })


def _graph_put(job_id: str, payload: Dict[str, Any]) -> None:
    g = _graph_get(job_id)

    # Наращиваем nodes/edges по decision_links (минимально, без лишней логики)
    for link in payload.get("decision_links", []) or []:
        src = link.get("from")
        tgt = link.get("to")

        if src and not any(n["id"] == src for n in g["nodes"]):
            g["nodes"].append({
                "id": src,
                "label": src.split(":")[-1],
                "type": "signal",
                "score": None,
                "tags": [],
            })
        if tgt and not any(n["id"] == tgt for n in g["nodes"]):
            g["nodes"].append({
                "id": tgt,
                "label": tgt.split(":")[-1],
                "type": "signal",
                "score": None,
                "tags": [],
            })

        g["edges"].append({
            "source": src,
            "target": tgt,
            "kind": link.get("kind", "signal"),
            "weight": link.get("weight", 0.5),
            "ts": link.get("ts"),
        })

    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _local_graph[job_id] = g


# ─────────────────────────────────────────────
# FASTAPI INIT
# ─────────────────────────────────────────────

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# SIMPLE IN-MEMORY CACHE
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# HTTP CLIENT
# ─────────────────────────────────────────────

async def _get_json(url: str, params: Optional[dict] = None) -> Tuple[bool, Any, int]:
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(url, params=params)
            ct = (r.headers.get("content-type") or "").lower()

            if r.status_code >= 400:
                body = r.text if "application/json" not in ct else r.json()
                return False, {
                    "upstream_status": r.status_code,
                    "upstream_body": body,
                }, r.status_code

            if "application/json" in ct:
                return True, r.json(), r.status_code

            return True, {"raw": r.text}, r.status_code

    except httpx.HTTPError as e:
        return False, {"upstream_error": str(e)}, 502


# ─────────────────────────────────────────────
# BYBIT WORKER HELPERS
# ─────────────────────────────────────────────

async def _bybit_worker_ticker(symbol: str) -> Tuple[bool, Any, int]:
    """
    Дергаем Cloudflare worker, который просто проксирует:
    /bybit/v5/market/tickers?category=spot&symbol=...
    и возвращает оригинальный JSON Bybit.
    """
    url = BYBIT_WORKER_BASE.rstrip("/") + BYBIT_WORKER_TICKER_PATH
    params = {"category": "spot", "symbol": symbol}
    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, data, status

    try:
        if data.get("retCode") != 0:
            return False, {
                "error": "bybit_retcode",
                "raw": data,
            }, 502

        result = data.get("result") or {}
        lst = result.get("list") or []
        if not lst:
            return False, {"error": "empty_list", "raw": data}, 502

        return True, lst[0], 200

    except Exception as e:
        return False, {"error": "parse_error", "details": str(e), "raw": data}, 502


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _normalize_ticker(symbol: str, ticker: dict, tf: str) -> Dict[str, Any]:
    price = _safe_float(ticker.get("lastPrice"))
    change24 = _safe_float(ticker.get("price24hPcnt"), 0.0)
    vol24 = _safe_float(ticker.get("turnover24h"), 0.0)
    high24 = _safe_float(ticker.get("highPrice24h"))
    low24 = _safe_float(ticker.get("lowPrice24h"))
    bid1 = _safe_float(ticker.get("bid1Price"))
    ask1 = _safe_float(ticker.get("ask1Price"))

    if high24 is not None and low24 is not None and high24 > low24 and price is not None:
        rsi_approx = 100.0 * (price - low24) / (high24 - low24)
    else:
        rsi_approx = 50.0

    vol_index = min(max(abs(change24) / 0.10, 0.0), 1.0)  # нормализованный "volatility index"

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
            "rsi_approx": rsi_approx,
            "volatility_index": vol_index,
            "bid": bid1,
            "ask": ask1,
        },
        "forecast": {},
        "source": "bybit-spot-v5/worker",
        "q_score": None,
    }


# ─────────────────────────────────────────────
# SYSTEM ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/health")
async def health() -> Dict[str, Any]:
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
        "gateway_public": PUBLIC_BASE,
        "worker_base": BYBIT_WORKER_BASE,
    }


# ─────────────────────────────────────────────
# REALDATA ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/live")
async def get_live(
    symbol: str = Query(..., description="Spot symbol, e.g. BTCUSDT"),
    tf: str = Query("1m", description="Synthetic timeframe tag"),
):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached is not None:
        return {"cached": True, **cached}

    ok, ticker, status = await _bybit_worker_ticker(symbol)
    if not ok:
        return JSONResponse(status_code=status, content={"proxy": APP_NAME, "error": ticker})

    event = _normalize_ticker(symbol, ticker, tf)
    _cache_put(key, event)
    return {"cached": False, **event}


@app.get("/trade/top10")
async def get_top10():
    items: List[Dict[str, Any]] = []

    for sym in TOP10:
        ok, ticker, status = await _bybit_worker_ticker(sym)
        if not ok:
            # Добавляем упрощённую ошибку по символу, но не роняем весь список
            items.append({
                "symbol": sym,
                "error": True,
                "details": ticker,
            })
            continue

        norm = _normalize_ticker(sym, ticker, tf="1m")
        feat = norm.get("features", {})

        items.append({
            "symbol": norm.get("symbol", sym),
            "price": norm.get("price"),
            "change24h": feat.get("change24h"),
            "volume": feat.get("volume24h"),
            "high": feat.get("high24h"),
            "low": feat.get("low24h"),
            "bid1": feat.get("bid"),
            "ask1": feat.get("ask"),
        })

    return {"top10": items}


# ─────────────────────────────────────────────
# GRAPH & INGEST
# ─────────────────────────────────────────────

@app.get("/graph/{job_id}")
async def get_graph(
    job_id: str = Path(..., description="Graph job id"),
) -> Dict[str, Any]:
    g = _graph_get(job_id)
    g_out = {"cached": True, "fallback": "local"}
    g_out.update(g)
    return g_out


@app.post("/ingest-pass")
async def ingest_graph(payload: Dict[str, Any]):
    """
    Принимаем payload с decision_links / arena_events / mind_reflect.
    Сохраняем в локальный _local_graph, чтобы ISKRA3 могла читать через /graph/{job_id}.
    """
    job_id = payload.get("job_id") or "default"
    _graph_put(job_id, payload)
    return {"status": "ok", "fallback": "local"}


# ─────────────────────────────────────────────
# SIMPLE INDICATORS (RSI / MACD / ATR)
# ─────────────────────────────────────────────

def _rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    gains = []
    losses = []
    for i in range(1, period + 1):
        diff = closes[-i] - closes[-i - 1]
        if diff >= 0:
            gains.append(diff)
        else:
            losses.append(-diff)
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


@app.post("/indicators/rsi")
async def calc_rsi(body: Dict[str, Any]):
    closes = body.get("closes") or []
    closes_f = [_safe_float(c, 0.0) for c in closes]
    value = _rsi(closes_f)
    return {"rsi": value}


@app.post("/indicators/macd")
async def calc_macd(body: Dict[str, Any]):
    closes = body.get("closes") or []
    closes_f = [_safe_float(c, 0.0) for c in closes]

    def ema(values: List[float], period: int) -> float:
        if not values:
            return 0.0
        k = 2 / (period + 1)
        ema_val = values[0]
        for v in values[1:]:
            ema_val = v * k + ema_val * (1 - k)
        return ema_val

    fast = ema(closes_f, 12)
    slow = ema(closes_f, 26)
    macd_val = fast - slow
    signal = ema([macd_val], 9)
    return {"macd": macd_val, "signal": signal}


@app.post("/indicators/atr")
async def calc_atr(body: Dict[str, Any]):
    highs = body.get("highs") or []
    lows = body.get("lows") or []
    closes = body.get("closes") or []

    n = min(len(highs), len(lows), len(closes))
    if n < 2:
        return {"atr": 0.0}

    trs: List[float] = []
    for i in range(1, n):
        h = _safe_float(highs[i], 0.0)
        l = _safe_float(lows[i], 0.0)
        c_prev = _safe_float(closes[i - 1], 0.0)
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs.append(tr)

    atr_val = sum(trs) / len(trs) if trs else 0.0
    return {"atr": atr_val}


# ─────────────────────────────────────────────
# RISK & RADAR (ПРОСТЫЕ ХЕЛПЕРЫ)
# ─────────────────────────────────────────────

@app.post("/risk/calc")
async def risk_calc(body: Dict[str, Any]):
    closes = body.get("closes") or []
    closes_f = [_safe_float(c, 0.0) for c in closes]
    if len(closes_f) < 2:
        return {"riskScore": 0.0, "riskLevel": "LOW"}

    mean = sum(closes_f) / len(closes_f)
    var = sum((c - mean) ** 2 for c in closes_f) / len(closes_f)
    vol = var ** 0.5

    if vol < 0.005 * mean:
        level = "LOW"
    elif vol < 0.02 * mean:
        level = "MEDIUM"
    else:
        level = "HIGH"

    return {"riskScore": vol, "riskLevel": level}


@app.post("/radar/forecast")
async def radar_forecast(body: Dict[str, Any]):
    closes = body.get("closes") or []
    closes_f = [_safe_float(c, 0.0) for c in closes]
    if len(closes_f) < 2:
        return {"direction": "flat", "confidence": 0.0}

    last = closes_f[-1]
    prev = closes_f[-2]
    diff = last - prev
    if diff > 0:
        direction = "up"
    elif diff < 0:
        direction = "down"
    else:
        direction = "flat"

    magnitude = abs(diff) / (prev or 1.0)
    confidence = max(0.0, min(magnitude * 10.0, 1.0))

    return {"direction": direction, "confidence": confidence}


# ─────────────────────────────────────────────
# SNAPSHOT / EVENTS / GOVERNANCE — STUBS
# ─────────────────────────────────────────────

@app.post("/snapshot/create")
async def create_snapshot(body: Dict[str, Any]):
    # здесь можно сохранять state_hash/proof_hash в хранилище; пока — просто echo
    return {"status": "created"}


@app.post("/events/route")
async def route_event(body: Dict[str, Any]):
    # просто echo-маршрутизатор, чтобы ISKRA3 могла писать event-stream
    return {"ok": True, "echo": body}


@app.post("/governance/policy")
async def apply_policy(body: Dict[str, Any]):
    rule = body.get("rule")
    value = body.get("value")
    # здесь можно применять свои политические правила; пока считается, что применено
    return {"applied": True, "rule": rule, "value": value}


# ─────────────────────────────────────────────
# FEEDER LOOP (ОПЦИОНАЛЬНО)
# ─────────────────────────────────────────────

async def _feeder_loop():
    if not FEEDER_ENABLED:
        return

    while True:
        try:
            ok, ticker, _ = await _bybit_worker_ticker(FEEDER_SYMBOL)
            if ok:
                # В минимальном варианте просто дергаем, чтобы не простаивал воркер.
                pass
        except Exception as e:
            print("Feeder error:", e)

        await asyncio.sleep(FEEDER_INTERVAL_SEC)


@app.on_event("startup")
async def _on_startup():
    if FEEDER_ENABLED:
        app.state.feeder_task = asyncio.create_task(_feeder_loop())


@app.on_event("shutdown")
async def _on_shutdown():
    t = getattr(app.state, "feeder_task", None)
    if t:
        t.cancel()
