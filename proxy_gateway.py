# ds24-proxy-gateway (Binance REALDATA version)
# Реальные данные с Binance Spot API, без Bybit, без Cloudflare, без воркеров.

import os
import time
import json
import math
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx

APP_NAME = "ds24-proxy-gateway"

# ─────────────────────────────────────────────
# BINANCE ENDPOINT
# ─────────────────────────────────────────────
BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com")

CACHE_TTL = int(os.getenv("CACHE_TTL", "10"))

# ─────────────────────────────────────────────
# LOCAL GRAPH STORAGE (простейший)
# ─────────────────────────────────────────────
_local_graph: Dict[str, Dict[str, Any]] = {}


def _graph_get(job_id: str) -> Dict[str, Any]:
    if job_id not in _local_graph:
        _local_graph[job_id] = {
            "nodes": [],
            "edges": [],
            "metrics": {
                "dqi_avg": None,
                "risk_cvar": None,
                "finops_cost_usd": None,
                "updated_at": None,
            },
        }
    return _local_graph[job_id]


def _graph_put(job_id: str, payload: Dict[str, Any]):
    g = _graph_get(job_id)

    def ensure_node(node_id: str):
        if not any(n["id"] == node_id for n in g["nodes"]):
            g["nodes"].append(
                {
                    "id": node_id,
                    "label": node_id.split(":")[-1],
                    "type": "signal",
                    "score": None,
                    "tags": [],
                }
            )

    decision_links = payload.get("decision_links") or []
    if isinstance(decision_links, list):
        for e in decision_links:
            src = e.get("from")
            tgt = e.get("to")
            if src:
                ensure_node(src)
            if tgt:
                ensure_node(tgt)
            g["edges"].append(
                {
                    "source": src,
                    "target": tgt,
                    "kind": e.get("kind", "signal"),
                    "weight": float(e.get("weight", 0.5) or 0.5),
                    "ts": e.get("ts") or datetime.now(timezone.utc).isoformat(),
                }
            )

    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    _local_graph[job_id] = g


# ─────────────────────────────────────────────
# FASTAPI APP
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
# SIMPLE CACHE
# ─────────────────────────────────────────────
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


# ─────────────────────────────────────────────
# HTTP CLIENT
# ─────────────────────────────────────────────
async def _get_json(url: str, params: Optional[dict] = None) -> Tuple[bool, Any, int]:
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get(url, params=params)
            ct = (r.headers.get("content-type") or "").lower()

            if r.status_code >= 400:
                body = r.text
                try:
                    if "application/json" in ct:
                        body = r.json()
                except Exception:
                    pass
                return False, {
                    "upstream_status": r.status_code,
                    "upstream_body": body,
                }, r.status_code

            if "application/json" in ct:
                return True, r.json(), r.status_code

            return True, {"raw": r.text}, r.status_code
    except httpx.HTTPError as e:
        return False, {"upstream_error": str(e), "url": url, "params": params}, 502


# ─────────────────────────────────────────────
# BINANCE HELPERS
# ─────────────────────────────────────────────
async def _binance_ticker(symbol: str) -> Tuple[bool, Any, int]:
    """
    /api/v3/ticker/24hr?symbol=BTCUSDT
    """
    url = f"{BINANCE_BASE.rstrip('/')}/api/v3/ticker/24hr"
    ok, data, status = await _get_json(url, params={"symbol": symbol})
    if not ok:
        return False, data, status
    # ожидаем dict
    if not isinstance(data, dict) or data.get("symbol") != symbol:
        return False, {"error": "unexpected_binance_format", "raw": data}, 502
    return True, data, 200


def _f(x, default=None):
    if x is None:
        return default
    try:
        v = float(x)
        if math.isfinite(v):
            return v
        return default
    except Exception:
        return default


def _parse_binance_ticker(symbol: str, data: dict, tf: str) -> dict:
    price = _f(data.get("lastPrice"))
    change24 = _f(data.get("priceChangePercent"), 0.0)
    vol24 = _f(data.get("volume"), 0.0)
    high24 = _f(data.get("highPrice"))
    low24 = _f(data.get("lowPrice"))
    bid = _f(data.get("bidPrice"))
    ask = _f(data.get("askPrice"))

    # Мини-аналог rsi_approx по позиции в диапазоне
    if high24 is not None and low24 is not None and high24 > low24 and price is not None:
        rsi_approx = 100.0 * (price - low24) / (high24 - low24)
    else:
        rsi_approx = 50.0

    vol_index = min(max(abs(change24) / 10.0, 0.0), 1.0)  # 10% → 1.0

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "ts": ts,
        "symbol": data.get("symbol", symbol),
        "tf": tf,
        "price": price,
        "features": {
            "change24h": change24,
            "volume24h": vol24,
            "high24h": high24,
            "low24h": low24,
            "rsi_approx": rsi_approx,
            "volatility_index": vol_index,
            "bid": bid,
            "ask": ask,
        },
        "forecast": {},
        "source": "binance-spot-v3",
        "q_score": None,
    }


# ─────────────────────────────────────────────
# INDICATORS / RISK / RADAR (локальные расчёты)
# ─────────────────────────────────────────────
def _calc_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = diff if diff > 0 else 0.0
        loss = -diff if diff < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def _ema(series: List[float], period: int) -> List[float]:
    if not series:
        return []
    k = 2.0 / (period + 1.0)
    out = []
    prev = series[0]
    out.append(prev)
    for i in range(1, len(series)):
        val = series[i] * k + prev * (1.0 - k)
        out.append(val)
        prev = val
    return out


def _calc_macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9):
    if len(closes) < slow + signal:
        return None
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd = [f - s for f, s in zip(ema_fast, ema_slow)]
    # signal line
    sig = _ema(macd[slow - 1 :], signal)
    if not sig:
        return None
    return {"macd": macd[-1], "signal": sig[-1]}


def _calc_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14):
    if not (len(highs) == len(lows) == len(closes)):
        return None
    if len(highs) < period + 1:
        return None
    trs = []
    for i in range(1, len(highs)):
        h = highs[i]
        l = lows[i]
        pc = closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr


def _calc_risk(closes: List[float]):
    if len(closes) < 2:
        return {"riskScore": None, "riskLevel": "unknown"}
    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] == 0:
            continue
        rets.append((closes[i] - closes[i - 1]) / closes[i - 1])
    if not rets:
        return {"riskScore": None, "riskLevel": "unknown"}
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / len(rets)
    vol = math.sqrt(var)
    score = max(0, min(100, round(vol * 1000)))
    if score < 30:
        level = "low"
    elif score > 70:
        level = "high"
    else:
        level = "medium"
    return {"riskScore": score, "riskLevel": level}


def _radar_forecast(closes: List[float]):
    if len(closes) < 2:
        return {"direction": "flat", "confidence": 0.0}
    first = closes[0]
    last = closes[-1]
    if first == 0:
        return {"direction": "flat", "confidence": 0.0}
    ret = (last - first) / first
    if ret > 0.002:
        direction = "up"
    elif ret < -0.002:
        direction = "down"
    else:
        direction = "flat"
    confidence = max(0.0, min(1.0, abs(ret) * 50.0))
    return {"direction": direction, "confidence": confidence}


# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "ts": int(time.time()),
        "binance_base": BINANCE_BASE,
        "graph_jobs": list(_local_graph.keys()),
    }


@app.get("/live")
async def live(symbol: str = Query(...), tf: str = Query("1m")):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, data, status = await _binance_ticker(symbol)
    if not ok:
        return JSONResponse(status_code=status, content={"proxy": APP_NAME, "error": data})

    event = _parse_binance_ticker(symbol, data, tf)
    _cache_put(key, event)
    return {"cached": False, **event}


@app.get("/trade/top10")
async def trade_top10():
    """
    Топ-10 USDT-пар по объёму с Binance.
    """
    url = f"{BINANCE_BASE.rstrip('/')}/api/v3/ticker/24hr"
    ok, data, status = await _get_json(url)
    if not ok:
        return JSONResponse(status_code=status, content={"proxy": APP_NAME, "error": data})

    if not isinstance(data, list):
        return JSONResponse(
            status_code=502,
            content={"proxy": APP_NAME, "error": "unexpected_binance_list_format", "raw": data},
        )

    # фильтруем только USDT-пары
    usdt = [t for t in data if isinstance(t, dict) and isinstance(t.get("symbol"), str) and t["symbol"].endswith("USDT")]
    # сортируем по quoteVolume (если есть) или volume
    def _vol(t):
        qv = _f(t.get("quoteVolume"))
        if qv is not None:
            return qv
        return _f(t.get("volume"), 0.0)

    usdt_sorted = sorted(usdt, key=_vol, reverse=True)
    top10 = usdt_sorted[:10]

    cleaned = []
    for t in top10:
        cleaned.append(
            {
                "symbol": t.get("symbol"),
                "price": _f(t.get("lastPrice")),
                "change24h": _f(t.get("priceChangePercent")),
                "volume": _f(t.get("volume")),
                "high": _f(t.get("highPrice")),
                "low": _f(t.get("lowPrice")),
                "bid1": _f(t.get("bidPrice")),
                "ask1": _f(t.get("askPrice")),
            }
        )

    return {"top10": cleaned}


# ─────────────────────────────────────────────
# GRAPH ENDPOINTS
# ─────────────────────────────────────────────
@app.get("/graph/{job_id}")
async def graph(job_id: str):
    g = _graph_get(job_id)
    return {"cached": True, "fallback": "local", **g}


@app.post("/ingest-pass")
async def ingest_pass(payload: dict):
    job_id = payload.get("job_id", "default")
    _graph_put(job_id, payload)
    return {"status": "ok", "fallback": "local"}


# ─────────────────────────────────────────────
# INDICATORS / RISK / RADAR
# ─────────────────────────────────────────────
@app.post("/indicators/rsi")
async def indicators_rsi(payload: dict):
    closes = payload.get("closes") or []
    closes_f = [float(x) for x in closes]
    rsi_val = _calc_rsi(closes_f)
    return {"rsi": rsi_val}


@app.post("/indicators/macd")
async def indicators_macd(payload: dict):
    closes = payload.get("closes") or []
    closes_f = [float(x) for x in closes]
    res = _calc_macd(closes_f)
    if not res:
        return {"macd": None, "signal": None}
    return res


@app.post("/indicators/atr")
async def indicators_atr(payload: dict):
    highs = [float(x) for x in (payload.get("highs") or [])]
    lows = [float(x) for x in (payload.get("lows") or [])]
    closes = [float(x) for x in (payload.get("closes") or [])]
    atr_val = _calc_atr(highs, lows, closes)
    return {"atr": atr_val}


@app.post("/risk/calc")
async def risk_calc(payload: dict):
    closes = [float(x) for x in (payload.get("closes") or [])]
    return _calc_risk(closes)


@app.post("/radar/forecast")
async def radar_forecast(payload: dict):
    closes = [float(x) for x in (payload.get("closes") or [])]
    return _radar_forecast(closes)


# ─────────────────────────────────────────────
# SNAPSHOT / EVENTS / GOVERNANCE (заглушки)
# ─────────────────────────────────────────────
@app.post("/snapshot/create")
async def snapshot_create(payload: dict):
    return {"status": "ok"}


@app.post("/events/route")
async def events_route(payload: dict):
    return {"ok": True}


@app.post("/governance/policy")
async def governance_policy(payload: dict):
    return {"applied": True}
