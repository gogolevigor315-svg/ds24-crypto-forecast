# ds24-proxy-gateway FIXED v3.1 (CryptoCompare + CoinAPI unified gateway for ISKRA3)
# Полностью исправлено: top10, coinapi ohlcv, fallback логика, стабильно работает на Render.

import os
import time
import json
import asyncio
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

APP_NAME = "ds24-proxy-gateway"

# ============================================================
#  CONFIG
# ============================================================

PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://ds24-crypto-forecast-1.onrender.com")

# CryptoCompare
CC_BASE = os.getenv("CC_BASE", "https://min-api.cryptocompare.com")
CC_API_KEY = os.getenv("CC_API_KEY", "")

# CoinAPI
COINAPI_BASE = os.getenv("COINAPI_BASE", "https://rest.coinapi.io")
COINAPI_KEY = os.getenv("COINAPI_KEY", "")

CACHE_TTL = int(os.getenv("CACHE_TTL", "10"))

TOP10 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT"
]

# ============================================================
#  LOCAL GRAPH STORAGE
# ============================================================

_local_graph: Dict[str, Dict[str, Any]] = {}


def _graph_get(job_id: str):
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


def _graph_put(job_id: str, payload: Dict[str, Any]):
    g = _graph_get(job_id)
    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _local_graph[job_id] = g


# ============================================================
#  FASTAPI
# ============================================================

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ============================================================
# CACHE
# ============================================================

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


# ============================================================
# HTTP Helper
# ============================================================

async def _get_json(url, params=None, headers=None):
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(url, params=params, headers=headers)
            ct = (r.headers.get("content-type") or "").lower()

            if r.status_code >= 400:
                body = r.text
                try:
                    if "json" in ct:
                        body = r.json()
                except:
                    pass
                return False, {"upstream_status": r.status_code, "upstream_body": body}, r.status_code

            if "json" in ct:
                return True, r.json(), 200

            return True, {"raw": r.text}, 200

    except Exception as e:
        return False, {"error": str(e)}, 502


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_symbol(symbol: str):
    symbol = symbol.upper()
    for q in ["USDT", "USD", "USDC", "EUR", "BTC"]:
        if symbol.endswith(q):
            return symbol[:-len(q)], q
    return symbol, "USDT"


# ============================================================
# CRYPTOCOMPARE
# ============================================================

def _cc_headers():
    return {"authorization": f"Apikey {CC_API_KEY}"} if CC_API_KEY else {}


async def cc_live(symbol: str):
    base, quote = _split_symbol(symbol)
    url = f"{CC_BASE}/data/pricemultifull"
    params = {"fsyms": base, "tsyms": quote}
    ok, data, _ = await _get_json(url, params, _cc_headers())
    if not ok:
        return False, None, data

    try:
        raw = data["RAW"][base][quote]
        return True, {
            "price": float(raw.get("PRICE", 0)),
            "change24h": float(raw.get("CHANGEPCT24HOUR", 0)) / 100.0,
            "volume24h": float(raw.get("TOTALVOLUME24H", 0)),
            "high24h": float(raw.get("HIGH24HOUR", 0)),
            "low24h": float(raw.get("LOW24HOUR", 0)),
            "bid": float(raw.get("BID", 0)),
            "ask": float(raw.get("ASK", 0)),
        }, data
    except Exception as e:
        return False, None, {"error": "parse", "raw": data}


async def cc_top10():
    url = f"{CC_BASE}/data/top/totalvolfull"
    params = {"limit": 50, "tsym": "USDT"}
    ok, data, _ = await _get_json(url, params, _cc_headers())
    if not ok:
        return False, None, data

    out = []
    for item in data.get("Data", []):
        ci = item.get("CoinInfo") or {}
        symbol = ci.get("Name", "") + "USDT"
        if symbol not in TOP10:
            continue

        raw = (item.get("RAW") or {}).get(ci.get("Name", ""), {}).get("USDT", {})
        if not raw:
            continue

        out.append({
            "symbol": symbol,
            "price": float(raw.get("PRICE", 0)),
            "change24h": float(raw.get("CHANGEPCT24HOUR", 0)) / 100.0,
            "volume": float(raw.get("TOTALVOLUME24H", 0)),
            "high": float(raw.get("HIGH24HOUR", 0)),
            "low": float(raw.get("LOW24HOUR", 0)),
            "bid1": float(raw.get("BID", 0)),
            "ask1": float(raw.get("ASK", 0)),
        })

    # строго 10
    out_sorted = sorted(out, key=lambda x: TOP10.index(x["symbol"]))
    return True, out_sorted[:10], data


# ============================================================
# COINAPI
# ============================================================

def _coinapi_headers():
    return {"X-CoinAPI-Key": COINAPI_KEY} if COINAPI_KEY else {}


async def coinapi_live(symbol: str):
    if not COINAPI_KEY:
        return False, None, {"error": "no_key"}

    base, quote = _split_symbol(symbol)
    url = f"{COINAPI_BASE}/v1/exchangerate/{base}/{quote}"

    ok, data, _ = await _get_json(url, headers=_coinapi_headers())
    if not ok:
        return False, None, data

    try:
        return True, {"price": float(data.get("rate", 0))}, data
    except:
        return False, None, {"error": "parse", "raw": data}


async def coinapi_ohlcv(symbol: str, tf: str, limit: int):
    if not COINAPI_KEY:
        return False, None, {"error": "no_key"}

    tf_map = {"1m": "1MIN", "5m": "5MIN", "15m": "15MIN", "1h": "1HRS", "4h": "4HRS", "1d": "1DAY"}
    period = tf_map.get(tf, "1MIN")

    base, quote = _split_symbol(symbol)
    # ВАЖНО! CoinAPI формат: BASE_QUOTE
    url = f"{COINAPI_BASE}/v1/ohlcv/BINANCE_SPOT_{base}_{quote}/latest"
    params = {"period_id": period, "limit": limit}

    ok, data, _ = await _get_json(url, params=params, headers=_coinapi_headers())
    if not ok:
        return False, None, data

    try:
        bars = [
            {
                "ts": b.get("time_close"),
                "open": float(b.get("price_open", 0)),
                "high": float(b.get("price_high", 0)),
                "low": float(b.get("price_low", 0)),
                "close": float(b.get("price_close", 0)),
                "volume": float(b.get("volume_traded", 0)),
            }
            for b in data
        ]
        return True, bars, data
    except:
        return False, None, {"error": "parse", "raw": data}


# ============================================================
# UNIFIED LAYER
# ============================================================

def _approx_rsi(price, low, high):
    if price is None or low is None or high is None:
        return 50.0
    if high <= low:
        return 50.0
    return max(0.0, min(100.0, 100 * (price - low) / (high - low)))


async def unified_live(symbol: str, tf: str):
    # crypto compare — основной источник
    cc_ok, cc_data, cc_raw = await cc_live(symbol)

    # coinapi — fallback
    ca_ok, ca_data, ca_raw = await coinapi_live(symbol)

    if not cc_ok and not ca_ok:
        return False, {
            "proxy": APP_NAME,
            "error": "providers_failed",
            "cc": cc_raw,
            "coinapi": ca_raw
        }, 502

    price = None
    features = {
        "change24h": None,
        "volume24h": None,
        "high24h": None,
        "low24h": None,
        "bid": None,
        "ask": None,
    }

    sources_used = []

    if cc_ok:
        sources_used.append("cryptocompare")
        price = cc_data["price"]
        features["change24h"] = cc_data["change24h"]
        features["volume24h"] = cc_data["volume24h"]
        features["high24h"] = cc_data["high24h"]
        features["low24h"] = cc_data["low24h"]
        features["bid"] = cc_data["bid"]
        features["ask"] = cc_data["ask"]

    elif ca_ok:
        sources_used.append("coinapi")
        price = ca_data["price"]

    rsi_approx = _approx_rsi(price, features["low24h"], features["high24h"])
    vol_index = min(1.0, abs(features["change24h"] or 0) / 0.10)

    event = {
        "ts": _now(),
        "symbol": symbol.upper(),
        "tf": tf,
        "price": price,
        "features": {
            **features,
            "rsi_approx": rsi_approx,
            "volatility_index": vol_index
        },
        "forecast": {},
        "source": "+".join(sources_used),
        "q_score": None
    }

    return True, event, 200


async def unified_top10():
    ok, data, raw = await cc_top10()
    if not ok:
        return False, {
            "proxy": APP_NAME,
            "error": "top10_failed",
            "detail": raw
        }, 502

    return True, {"provider": "cryptocompare", "top10": data}, 200


# ============================================================
# INDICATORS / RISK / RADAR
# ============================================================

def _rsi(values: List[float], period=14):
    if len(values) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = values[-i] - values[-i - 1]
        (gains if diff >= 0 else losses).append(abs(diff))
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 1e-9
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _macd(values):
    if len(values) < 35:
        return 0.0, 0.0

    def ema(v, p):
        k = 2 / (p + 1)
        e = [v[0]]
        for x in v[1:]:
            e.append(x * k + e[-1] * (1 - k))
        return e

    fast = ema(values, 12)
    slow = ema(values, 26)
    macd = [f - s for f, s in zip(fast[-len(slow):], slow)]
    signal = ema(macd, 9)
    return macd[-1], signal[-1]


def _atr(highs, lows, closes, period=14):
    if len(highs) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        tr = max(highs[-i] - lows[-i],
                 abs(highs[-i] - closes[-i - 1]),
                 abs(lows[-i] - closes[-i - 1]))
        trs.append(tr)
    return sum(trs) / period


def _risk(closes):
    if len(closes) < 3:
        return 0.5, "medium"
    returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes))]
    vol = (sum(r*r for r in returns)/len(returns)) ** 0.5
    if vol < 0.01: return vol, "low"
    if vol < 0.03: return vol, "medium"
    return vol, "high"


def _radar(closes):
    if len(closes) < 5:
        return "flat", 0.5
    last = closes[-1]
    avg = sum(closes[-5:]) / 5
    if last > avg * 1.003:
        return "up", 0.65
    if last < avg * 0.997:
        return "down", 0.65
    return "flat", 0.55


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/health")
async def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "ts": int(time.time()),
        "providers": {
            "cryptocompare": bool(CC_API_KEY),
            "coinapi": bool(COINAPI_KEY),
        },
        "graph_jobs": list(_local_graph.keys()),
        "public_base": PUBLIC_BASE,
    }


@app.get("/live")
async def live(symbol: str, tf: str = "1m"):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, data, status = await unified_live(symbol, tf)
    if not ok:
        return JSONResponse(status_code=status, content=data)

    _cache_put(key, data)
    return {"cached": False, **data}


@app.get("/trade/top10")
async def trade_top10():
    key = "top10"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, data, status = await unified_top10()
    if not ok:
        return JSONResponse(status_code=status, content=data)

    _cache_put(key, data, ttl=30)
    return {"cached": False, **data}


@app.get("/graph/{job_id}")
async def graph(job_id: str):
    return {"cached": True, **_graph_get(job_id), "fallback": "local"}


@app.post("/ingest-pass")
async def ingest_pass(payload: Dict[str, Any]):
    job_id = payload.get("job_id", "default")
    _graph_put(job_id, payload)
    return {"status": "ok", "fallback": "local"}


@app.post("/indicators/rsi")
async def indicators_rsi(body: Dict[str, Any]):
    closes = [float(x) for x in body.get("closes", [])]
    return {"rsi": _rsi(closes)}


@app.post("/indicators/macd")
async def indicators_macd(body: Dict[str, Any]):
    closes = [float(x) for x in body.get("closes", [])]
    macd_val, signal = _macd(closes)
    return {"macd": macd_val, "signal": signal}


@app.post("/indicators/atr")
async def indicators_atr(body: Dict[str, Any]):
    highs = [float(x) for x in body.get("highs", [])]
    lows = [float(x) for x in body.get("lows", [])]
    closes = [float(x) for x in body.get("closes", [])]
    return {"atr": _atr(highs, lows, closes)}


@app.post("/risk/calc")
async def risk_calc(body: Dict[str, Any]):
    closes = [float(x) for x in body.get("closes", [])]
    score, level = _risk(closes)
    return {"riskScore": score, "riskLevel": level}


@app.post("/radar/forecast")
async def radar_forecast(body: Dict[str, Any]):
    closes = [float(x) for x in body.get("closes", [])]
    direction, conf = _radar(closes)
    return {"direction": direction, "confidence": conf}


@app.post("/snapshot/create")
async def snapshot_create(body: Dict[str, Any]):
    return {"status": "ok"}


@app.post("/events/route")
async def events_route(body: Dict[str, Any]):
    return {"ok": True}


@app.post("/governance/policy")
async def governance_policy(body: Dict[str, Any]):
    return {"applied": True}


@app.on_event("startup")
async def start():
    pass
