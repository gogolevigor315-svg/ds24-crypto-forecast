# ds24-proxy-gateway (DS24 Market Relay: CryptoCompare + CoinAPI)
# Без прямых бирж, без CloudFront, без гео-банов.
# Единый поток данных для ISKRA3.

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

APP_NAME = "ds24-market-relay"

# ─────────────────────────────────────────────
# CONFIG: PROVIDERS
# ─────────────────────────────────────────────

CRYPTOCOMPARE_API_KEY = os.getenv("CRYPTOCOMPARE_API_KEY", "").strip()
CRYPTOCOMPARE_BASE = os.getenv(
    "CRYPTOCOMPARE_BASE",
    "https://min-api.cryptocompare.com",
)

COINAPI_API_KEY = os.getenv("COINAPI_API_KEY", "").strip()
COINAPI_BASE = os.getenv(
    "COINAPI_BASE",
    "https://rest.coinapi.io",
)

CACHE_TTL = int(os.getenv("CACHE_TTL", "8"))

FEEDER_ENABLED = os.getenv("FEEDER_ENABLED", "true").lower() in ("1", "true", "yes", "on")
FEEDER_SYMBOL = os.getenv("FEEDER_SYMBOL", "BTCUSDT")
FEEDER_TF = os.getenv("FEEDER_TF", "1m")
FEEDER_JOB_ID = os.getenv("FEEDER_JOB_ID", "portfolio-live")
FEEDER_INTERVAL_SEC = int(os.getenv("FEEDER_INTERVAL_SEC", "20"))

TOP10_TSYMBOL = os.getenv("TOP10_TSYMBOL", "USDT")

# ─────────────────────────────────────────────
# LOCAL GRAPH STORAGE
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
# SIMPLE CACHE
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

async def _get_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> Tuple[bool, Any, int]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params, headers=headers)
            ct = (r.headers.get("content-type") or "").lower()

            if r.status_code >= 400:
                body = r.text if "application/json" not in ct else r.json()
                return False, {
                    "upstream_status": r.status_code,
                    "upstream_body": body,
                    "url": url,
                    "params": params,
                }, r.status_code

            if "application/json" in ct:
                return True, r.json(), r.status_code

            return True, {"raw": r.text}, r.status_code

    except httpx.HTTPError as e:
        return False, {"upstream_error": str(e), "url": url, "params": params}, 502


def _safe_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    if val is None:
        return default
    try:
        return float(val)
    except Exception:
        return default


# ─────────────────────────────────────────────
# SYMBOL HELPERS
# ─────────────────────────────────────────────

def _split_symbol(symbol: str) -> Tuple[str, str]:
    """
    Примитивный парсер для крипты: BTCUSDT, ETHUSD, ETHUSDC и т.п.
    """
    s = symbol.upper()
    for q in ("USDT", "USDC", "USD", "EUR", "BTC", "ETH"):
        if s.endswith(q):
            return s[:-len(q)], q
    # fallback
    if len(s) > 3:
        return s[:-3], s[-3:]
    return s, ""


# ─────────────────────────────────────────────
# CRYPTOCOMPARE PROVIDER
# ─────────────────────────────────────────────

async def _cc_live(symbol: str) -> Tuple[bool, Any]:
    base, quote = _split_symbol(symbol)
    fsym = base
    tsym = quote or TOP10_TSYMBOL

    url = CRYPTOCOMPARE_BASE.rstrip("/") + "/data/pricemultifull"
    params = {"fsyms": fsym, "tsyms": tsym}
    if CRYPTOCOMPARE_API_KEY:
        params["api_key"] = CRYPTOCOMPARE_API_KEY

    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, {"provider": "cryptocompare", "error": data, "status": status}

    try:
        raw = data.get("RAW", {})
        fs = raw.get(fsym, {})
        ts = fs.get(tsym)
        if not ts:
            return False, {"provider": "cryptocompare", "error": "no_ticker"}

        return True, {
            "provider": "cryptocompare",
            "symbol": symbol,
            "fsym": fsym,
            "tsym": tsym,
            "price": _safe_float(ts.get("PRICE")),
            "change24h": _safe_float(ts.get("CHANGE24HOUR")),
            "volume24h": _safe_float(ts.get("TOTALVOLUME24H")),
            "high24h": _safe_float(ts.get("HIGH24HOUR")),
            "low24h": _safe_float(ts.get("LOW24HOUR")),
            "bid": _safe_float(ts.get("BID")),
            "ask": _safe_float(ts.get("ASK")),
        }
    except Exception as e:
        return False, {"provider": "cryptocompare", "error": "parse_error", "details": str(e)}


async def _cc_ohlcv(symbol: str, tf: str, limit: int = 100) -> Tuple[bool, Any]:
    base, quote = _split_symbol(symbol)
    fsym = base
    tsym = quote or TOP10_TSYMBOL

    tf = tf.lower()
    if tf.endswith("m"):
        url_path = "/data/v2/histominute"
    elif tf.endswith("h"):
        url_path = "/data/v2/histohour"
    else:
        url_path = "/data/v2/histoday"

    url = CRYPTOCOMPARE_BASE.rstrip("/") + url_path
    params = {
        "fsym": fsym,
        "tsym": tsym,
        "limit": min(limit, 2000),
    }
    if CRYPTOCOMPARE_API_KEY:
        params["api_key"] = CRYPTOCOMPARE_API_KEY

    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, {"provider": "cryptocompare", "error": data, "status": status}

    try:
        if data.get("Response") != "Success":
            return False, {"provider": "cryptocompare", "error": "not_success", "raw": data}

        dlist = data.get("Data", {}).get("Data", [])
        candles = []
        for c in dlist:
            candles.append({
                "ts": c.get("time"),
                "open": _safe_float(c.get("open")),
                "high": _safe_float(c.get("high")),
                "low": _safe_float(c.get("low")),
                "close": _safe_float(c.get("close")),
                "volume": _safe_float(c.get("volumefrom")),
            })
        return True, candles

    except Exception as e:
        return False, {"provider": "cryptocompare", "error": "parse_error", "details": str(e)}


async def _cc_top10() -> Tuple[bool, Any]:
    """
    Топ-10 по объёму к USDT.
    """
    url = CRYPTOCOMPARE_BASE.rstrip("/") + "/data/top/totalvolfull"
    params = {
        "limit": 10,
        "tsym": TOP10_TSYMBOL,
    }
    if CRYPTOCOMPARE_API_KEY:
        params["api_key"] = CRYPTOCOMPARE_API_KEY

    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, {"provider": "cryptocompare", "error": data, "status": status}

    try:
        dlist = data.get("Data", []) or []
        out: List[Dict[str, Any]] = []
        for item in dlist:
            coin_info = item.get("CoinInfo", {}) or {}
            raw = item.get("RAW", {}) or {}
            ts = raw.get(TOP10_TSYMBOL, {}) or {}
            symbol = coin_info.get("Name")
            if not symbol:
                continue
            out.append({
                "symbol": symbol + TOP10_TSYMBOL,
                "price": _safe_float(ts.get("PRICE")),
                "change24h": _safe_float(ts.get("CHANGE24HOUR")),
                "volume": _safe_float(ts.get("TOTALVOLUME24H")),
                "high": _safe_float(ts.get("HIGH24HOUR")),
                "low": _safe_float(ts.get("LOW24HOUR")),
                "bid1": _safe_float(ts.get("BID")),
                "ask1": _safe_float(ts.get("ASK")),
            })
        return True, out
    except Exception as e:
        return False, {"provider": "cryptocompare", "error": "parse_error", "details": str(e)}


# ─────────────────────────────────────────────
# COINAPI PROVIDER
# ─────────────────────────────────────────────

async def _ca_live(symbol: str) -> Tuple[bool, Any]:
    if not COINAPI_API_KEY:
        return False, {"provider": "coinapi", "error": "no_api_key"}

    base, quote = _split_symbol(symbol)
    # по умолчанию Binance Spot-маркет
    asset_id = f"BINANCE_SPOT_{base}_{quote}"

    url = COINAPI_BASE.rstrip("/") + "/v1/quotes/current"
    headers = {"X-CoinAPI-Key": COINAPI_API_KEY}
    params = {"filter_symbol": asset_id}

    ok, data, status = await _get_json(url, params=params, headers=headers)
    if not ok:
        return False, {"provider": "coinapi", "error": data, "status": status}

    try:
        quotes = data if isinstance(data, list) else data.get("data") or data
        if isinstance(quotes, dict):
            quotes = [quotes]
        if not quotes:
            return False, {"provider": "coinapi", "error": "empty_quotes", "raw": data}

        q = quotes[0]
        last = _safe_float(q.get("last_price")) or _safe_float(q.get("ask_price")) or _safe_float(q.get("bid_price"))
        bid = _safe_float(q.get("bid_price"))
        ask = _safe_float(q.get("ask_price"))

        return True, {
            "provider": "coinapi",
            "symbol": symbol,
            "asset_id": asset_id,
            "price": last,
            "bid": bid,
            "ask": ask,
        }
    except Exception as e:
        return False, {"provider": "coinapi", "error": "parse_error", "details": str(e)}


# ─────────────────────────────────────────────
# AGGREGATOR (CryptoCompare + CoinAPI)
# ─────────────────────────────────────────────

async def _merged_live(symbol: str, tf: str) -> Tuple[bool, Any]:
    cc_ok, cc_data = await _cc_live(symbol)
    ca_ok, ca_data = await _ca_live(symbol)

    if not cc_ok and not ca_ok:
        return False, {
            "error": "both_providers_failed",
            "cc": cc_data,
            "ca": ca_data,
        }

    primary = None
    secondary = None

    if cc_ok:
        primary = cc_data
        if ca_ok:
            secondary = ca_data
    else:
        primary = ca_data
        secondary = cc_data if cc_ok else None

    p_price = _safe_float(primary.get("price"))
    s_price = _safe_float(secondary.get("price")) if secondary else None

    divergence = None
    anomaly = False
    if p_price is not None and s_price is not None and s_price > 0:
        divergence = abs(p_price - s_price) / ((p_price + s_price) / 2.0)
        if divergence > 0.01:  # >1% расхождение
            anomaly = True

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    features = {
        "change24h": primary.get("change24h"),
        "volume24h": primary.get("volume24h"),
        "high24h": primary.get("high24h"),
        "low24h": primary.get("low24h"),
        "bid": primary.get("bid"),
        "ask": primary.get("ask"),
        "primary_source": primary.get("provider"),
        "secondary_source": secondary.get("provider") if secondary else None,
        "secondary_price": s_price,
        "price_divergence": divergence,
        "anomaly": anomaly,
    }

    return True, {
        "ts": ts,
        "symbol": symbol,
        "tf": tf,
        "price": p_price,
        "features": features,
        "forecast": {},
        "source": "ds24-relay",
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
        "providers": {
            "cryptocompare": {
                "base": CRYPTOCOMPARE_BASE,
                "api_key_set": bool(CRYPTOCOMPARE_API_KEY),
            },
            "coinapi": {
                "base": COINAPI_BASE,
                "api_key_set": bool(COINAPI_API_KEY),
            },
        },
        "feeder": {
            "enabled": FEEDER_ENABLED,
            "symbol": FEEDER_SYMBOL,
            "tf": FEEDER_TF,
            "job_id": FEEDER_JOB_ID,
            "interval_sec": FEEDER_INTERVAL_SEC,
        },
        "graph_jobs": list(_local_graph.keys()),
    }


# ─────────────────────────────────────────────
# MARKET DATA ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/live")
async def live(
    symbol: str = Query(..., description="e.g. BTCUSDT"),
    tf: str = Query("1m", description="synthetic timeframe label"),
):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached is not None:
        return {"cached": True, **cached}

    ok, data = await _merged_live(symbol, tf)
    if not ok:
        return JSONResponse(status_code=502, content={"proxy": APP_NAME, "error": data})

    _cache_put(key, data)
    return {"cached": False, **data}


@app.get("/ohlcv")
async def ohlcv(
    symbol: str = Query(..., description="e.g. BTCUSDT"),
    tf: str = Query("1h", description="1m,5m,1h,1d"),
    limit: int = Query(100, ge=1, le=2000),
):
    ok, candles = await _cc_ohlcv(symbol, tf, limit)
    if not ok:
        return JSONResponse(status_code=502, content={"proxy": APP_NAME, "error": candles})
    return {
        "symbol": symbol,
        "tf": tf,
        "limit": limit,
        "candles": candles,
    }


@app.get("/trade/top10")
async def trade_top10():
    ok, data = await _cc_top10()
    if not ok:
        return JSONResponse(status_code=502, content={"proxy": APP_NAME, "error": data})
    return {"top10": data}


# ─────────────────────────────────────────────
# GRAPH & INGEST
# ─────────────────────────────────────────────

@app.get("/graph/{job_id}")
async def get_graph(
    job_id: str = Path(..., description="Graph job id"),
):
    g = _graph_get(job_id)
    out = {"cached": True, "fallback": "local"}
    out.update(g)
    return out


@app.post("/ingest-pass")
async def ingest_pass(payload: Dict[str, Any]):
    job_id = payload.get("job_id") or "default"
    _graph_put(job_id, payload)
    return {"status": "ok", "fallback": "local"}


# ─────────────────────────────────────────────
# INDICATORS (RSI / MACD / ATR)
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
# RISK & RADAR
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

    if mean == 0:
        rel_vol = 0.0
    else:
        rel_vol = vol / abs(mean)

    if rel_vol < 0.005:
        level = "LOW"
    elif rel_vol < 0.02:
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
# SNAPSHOT / EVENTS / GOVERNANCE
# ─────────────────────────────────────────────

@app.post("/snapshot/create")
async def create_snapshot(body: Dict[str, Any]):
    return {"status": "created"}


@app.post("/events/route")
async def route_event(body: Dict[str, Any]):
    return {"ok": True, "echo": body}


@app.post("/governance/policy")
async def apply_policy(body: Dict[str, Any]):
    rule = body.get("rule")
    value = body.get("value")
    return {"applied": True, "rule": rule, "value": value}


# ─────────────────────────────────────────────
# FEEDER LOOP (OPTIONAL)
# ─────────────────────────────────────────────

async def _feeder_loop():
    if not FEEDER_ENABLED:
        return

    while True:
        try:
            ok, data = await _merged_live(FEEDER_SYMBOL, FEEDER_TF)
            if ok:
                # тут можно писать в graph через _graph_put или внешний ingest
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
