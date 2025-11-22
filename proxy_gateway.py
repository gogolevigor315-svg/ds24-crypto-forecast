# ds24-proxy-gateway (CryptoCompare + CoinAPI unified gateway for ISKRA3)

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
#  CONFIG: PUBLIC BASE (Render) + PROVIDERS
# ============================================================

PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://ds24-crypto-forecast-1.onrender.com")

# CryptoCompare
CC_BASE = os.getenv("CC_BASE", "https://min-api.cryptocompare.com")
CC_API_KEY = os.getenv("CC_API_KEY", "")

# CoinAPI
COINAPI_BASE = os.getenv("COINAPI_BASE", "https://rest.coinapi.io")
COINAPI_KEY = os.getenv("COINAPI_KEY", "")

# CACHE
CACHE_TTL = int(os.getenv("CACHE_TTL", "10"))

# Top symbols (USDT-мир)
TOP10 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT"
]

# ============================================================
#  SIMPLE LOCAL GRAPH STORAGE (для ISKRA3 Graph)
# ============================================================

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
    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _local_graph[job_id] = g


# ============================================================
#  FASTAPI APP
# ============================================================

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ============================================================
#  CACHE
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
#  HTTP HELPERS
# ============================================================

async def _get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[bool, Any, int]:
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(url, params=params, headers=headers)
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

            # текстовый ответ
            return True, {"raw": r.text}, r.status_code

    except httpx.HTTPError as e:
        return False, {"error": str(e), "url": url, "params": params}, 502


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_symbol(symbol: str) -> Tuple[str, str]:
    """
    Простейший парсер тикера вида BTCUSDT -> ("BTC","USDT").
    Для наших TOP10 этого достаточно.
    """
    symbol = symbol.upper()
    quotes = ["USDT", "USD", "USDC", "EUR", "BTC"]
    for q in quotes:
        if symbol.endswith(q):
            base = symbol[: -len(q)]
            return base, q
    # Фоллбек — весь тикер как base, USDT как quote
    return symbol, "USDT"


# ============================================================
#  CRYPTOCOMPARE CLIENT
# ============================================================

def _cc_headers() -> Dict[str, str]:
    headers = {}
    if CC_API_KEY:
        headers["authorization"] = f"Apikey {CC_API_KEY}"
    return headers


async def cc_live_ticker(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    base, quote = _split_symbol(symbol)
    url = f"{CC_BASE}/data/pricemultifull"
    params = {"fsyms": base, "tsyms": quote}
    ok, data, _ = await _get_json(url, params=params, headers=_cc_headers())
    if not ok:
        return False, None, data

    try:
        raw = data.get("RAW", {}).get(base, {}).get(quote, {})
        if not raw:
            return False, None, {"error": "empty_raw", "raw": data}

        return True, {
            "price": float(raw.get("PRICE") or 0.0),
            "change24h": float(raw.get("CHANGEPCT24HOUR") or 0.0) / 100.0,
            "volume24h": float(raw.get("TOTALVOLUME24H") or 0.0),
            "high24h": float(raw.get("HIGH24HOUR") or 0.0),
            "low24h": float(raw.get("LOW24HOUR") or 0.0),
            "bid": float(raw.get("BID") or 0.0),
            "ask": float(raw.get("ASK") or 0.0),
        }, data
    except Exception as e:
        return False, None, {"error": "parse_error", "details": str(e), "raw": data}


async def cc_ohlcv(symbol: str, tf: str, limit: int) -> Tuple[bool, Optional[List[Dict[str, Any]]], Any]:
    """
    Берём минутные свечи /data/v2/histominute и далее уже ISKRA3 может агрегировать сама.
    """
    base, quote = _split_symbol(symbol)
    url = f"{CC_BASE}/data/v2/histominute"
    params = {"fsym": base, "tsym": quote, "limit": limit}
    ok, data, _ = await _get_json(url, params=params, headers=_cc_headers())
    if not ok:
        return False, None, data

    try:
        bars = data.get("Data", {}).get("Data", [])
        ohlcv = [
            {
                "ts": datetime.utcfromtimestamp(b["time"]).replace(tzinfo=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "open": float(b.get("open") or 0.0),
                "high": float(b.get("high") or 0.0),
                "low": float(b.get("low") or 0.0),
                "close": float(b.get("close") or 0.0),
                "volume": float(b.get("volumeto") or 0.0),
            }
            for b in bars
        ]
        return True, ohlcv, data
    except Exception as e:
        return False, None, {"error": "parse_error", "details": str(e), "raw": data}


async def cc_top10() -> Tuple[bool, Optional[List[Dict[str, Any]]], Any]:
    url = f"{CC_BASE}/data/top/totalvolfull"
    params = {"limit": 50, "tsym": "USDT"}
    ok, data, _ = await _get_json(url, params=params, headers=_cc_headers())
    if not ok:
        return False, None, data

    try:
        arr = data.get("Data", [])
        out: List[Dict[str, Any]] = []
        for item in arr:
            coin_info = item.get("CoinInfo", {}) or {}
            raw = (
                item.get("RAW", {})
                .get(coin_info.get("Name", ""), {})
                .get("USDT", {})
            )
            if not raw:
                continue

            symbol = f'{coin_info.get("Name","")}' + "USDT"
            out.append(
                {
                    "symbol": symbol,
                    "price": float(raw.get("PRICE") or 0.0),
                    "change24h": float(raw.get("CHANGEPCT24HOUR") or 0.0) / 100.0,
                    "volume": float(raw.get("TOTALVOLUME24H") or 0.0),
                    "high": float(raw.get("HIGH24HOUR") or 0.0),
                    "low": float(raw.get("LOW24HOUR") or 0.0),
                    "bid1": float(raw.get("BID") or 0.0),
                    "ask1": float(raw.get("ASK") or 0.0),
                }
            )

        # Фильтруем по нашему TOP10 списку и сортируем
        filtered = [t for t in out if t["symbol"] in TOP10]
        filtered_sorted = sorted(filtered, key=lambda t: TOP10.index(t["symbol"]))
        return True, filtered_sorted, data
    except Exception as e:
        return False, None, {"error": "parse_error", "details": str(e), "raw": data}


# ============================================================
#  COINAPI CLIENT
# ============================================================

def _coinapi_headers() -> Dict[str, str]:
    headers = {}
    if COINAPI_KEY:
        headers["X-CoinAPI-Key"] = COINAPI_KEY
    return headers


async def coinapi_live_ticker(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    base, quote = _split_symbol(symbol)
    url = f"{COINAPI_BASE}/v1/exchangerate/{base}/{quote}"
    ok, data, _ = await _get_json(url, headers=_coinapi_headers())
    if not ok:
        return False, None, data

    try:
        rate = float(data.get("rate") or 0.0)
        # CoinAPI не даёт полный 24h snapshot в этом endpoint — оставим только price
        return True, {
            "price": rate,
        }, data
    except Exception as e:
        return False, None, {"error": "parse_error", "details": str(e), "raw": data}


async def coinapi_ohlcv(symbol: str, tf: str, limit: int) -> Tuple[bool, Optional[List[Dict[str, Any]]], Any]:
    # Простейший маппинг таймфрейма в CoinAPI period_id
    tf_map = {
        "1m": "1MIN",
        "5m": "5MIN",
        "15m": "15MIN",
        "1h": "1HRS",
        "4h": "4HRS",
        "1d": "1DAY",
    }
    period_id = tf_map.get(tf, "1MIN")

    # Для простоты считаем, что биржа BINANCE_SPOT
    url_symbol = symbol.upper()
    url = f"{COINAPI_BASE}/v1/ohlcv/BINANCE_SPOT_{url_symbol}/latest"
    params = {"period_id": period_id, "limit": limit}
    ok, data, _ = await _get_json(url, params=params, headers=_coinapi_headers())
    if not ok:
        return False, None, data

    try:
        ohlcv = [
            {
                "ts": b.get("time_close"),
                "open": float(b.get("price_open") or 0.0),
                "high": float(b.get("price_high") or 0.0),
                "low": float(b.get("price_low") or 0.0),
                "close": float(b.get("price_close") or 0.0),
                "volume": float(b.get("volume_traded") or 0.0),
            }
            for b in data
        ]
        return True, ohlcv, data
    except Exception as e:
        return False, None, {"error": "parse_error", "details": str(e), "raw": data}


# ============================================================
#  UNIFIED AGGREGATION LAYER
# ============================================================

def _approx_rsi_from_range(price: Optional[float], low: Optional[float], high: Optional[float]) -> float:
    if price is None or low is None or high is None:
        return 50.0
    if high <= low:
        return 50.0
    return float(max(0.0, min(100.0, 100.0 * (price - low) / (high - low))))


async def unified_live(symbol: str, tf: str) -> Tuple[bool, Dict[str, Any], int]:
    cc_ok, cc_parsed, cc_raw = await cc_live_ticker(symbol)
    ca_ok, ca_parsed, ca_raw = await coinapi_live_ticker(symbol)

    if not cc_ok and not ca_ok:
        return False, {
            "proxy": APP_NAME,
            "error": "no_provider_ok",
            "cc_error": cc_raw,
            "coinapi_error": ca_raw,
        }, 502

    # цена: если есть CC — берём её, иначе CoinAPI
    price = None
    change24h = None
    volume24h = None
    high24h = None
    low24h = None
    bid = None
    ask = None

    sources_used: List[str] = []
    if cc_ok and cc_parsed:
        sources_used.append("cryptocompare")
        price = cc_parsed.get("price")
        change24h = cc_parsed.get("change24h")
        volume24h = cc_parsed.get("volume24h")
        high24h = cc_parsed.get("high24h")
        low24h = cc_parsed.get("low24h")
        bid = cc_parsed.get("bid")
        ask = cc_parsed.get("ask")

    if ca_ok and ca_parsed:
        sources_used.append("coinapi")
        if price is None:
            price = ca_parsed.get("price")
        # CoinAPI в этом эндпоинте даёт только price — используем как sanity-check
        # Можно усреднить:
        if price is not None and ca_parsed.get("price") is not None and cc_parsed:
            price = (float(price) + float(ca_parsed["price"])) / 2.0

    rsi_approx = _approx_rsi_from_range(price, low24h, high24h)
    vol_index = float(abs(change24h or 0.0))
    vol_index = max(0.0, min(1.0, vol_index / 0.10))  # грубая нормировка

    event = {
        "ts": _now_iso(),
        "symbol": symbol.upper(),
        "tf": tf,
        "price": price,
        "features": {
            "change24h": change24h,
            "volume24h": volume24h,
            "high24h": high24h,
            "low24h": low24h,
            "rsi_approx": rsi_approx,
            "volatility_index": vol_index,
            "bid": bid,
            "ask": ask,
        },
        "forecast": {},
        "source": "cc+coinapi",
        "q_score": None,
        "debug": {
            "providers_used": sources_used,
        },
    }

    return True, event, 200


async def unified_ohlcv(symbol: str, tf: str, limit: int) -> Tuple[bool, Dict[str, Any], int]:
    cc_ok, cc_bars, cc_raw = await cc_ohlcv(symbol, tf, limit)
    ca_ok, ca_bars, ca_raw = await coinapi_ohlcv(symbol, tf, limit)

    if not cc_ok and not ca_ok:
        return False, {
            "proxy": APP_NAME,
            "error": "no_provider_ok",
            "cc_error": cc_raw,
            "coinapi_error": ca_raw,
        }, 502

    merged: List[Dict[str, Any]] = []

    if cc_ok and cc_bars:
        merged = cc_bars
    elif ca_ok and ca_bars:
        merged = ca_bars

    return True, {
        "symbol": symbol.upper(),
        "tf": tf,
        "bars": merged,
        "providers_used": [p for p, ok in [["cryptocompare", cc_ok], ["coinapi", ca_ok]] if ok],
    }, 200


async def unified_top10() -> Tuple[bool, Dict[str, Any], int]:
    cc_ok, cc_list, cc_raw = await cc_top10()

    if not cc_ok or not cc_list:
        return False, {
            "proxy": APP_NAME,
            "error": "top10_failed",
            "cc_error": cc_raw,
        }, 502

    return True, {
        "top10": cc_list,
        "provider": "cryptocompare",
    }, 200


# ============================================================
#  INDICATOR / RISK / RADAR — ЛОКАЛЬНЫЕ ВЫЧИСЛЕНИЯ
# ============================================================

def _rsi(values: List[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, period + 1):
        diff = values[-i] - values[-i - 1]
        if diff >= 0:
            gains.append(diff)
        else:
            losses.append(abs(diff))
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 1e-9
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float]:
    if len(values) < slow + signal:
        return 0.0, 0.0

    def ema(series: List[float], period: int) -> List[float]:
        k = 2.0 / (period + 1.0)
        ema_vals = [series[0]]
        for price in series[1:]:
            ema_vals.append(price * k + ema_vals[-1] * (1.0 - k))
        return ema_vals

    ema_fast = ema(values, fast)
    ema_slow = ema(values, slow)
    macd_line = [f - s for f, s in zip(ema_fast[-len(ema_slow):], ema_slow)]
    signal_line = ema(macd_line, signal)
    return macd_line[-1], signal_line[-1]


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        high = highs[-i]
        low = lows[-i]
        prev_close = closes[-i - 1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return sum(trs) / period


def _risk_score(closes: List[float]) -> Tuple[float, str]:
    if len(closes) < 2:
        return 0.5, "medium"
    returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes))]
    vol = (sum(r * r for r in returns) / len(returns)) ** 0.5
    # грубая шкала
    if vol < 0.01:
        return vol, "low"
    if vol < 0.03:
        return vol, "medium"
    return vol, "high"


def _radar_forecast(closes: List[float]) -> Tuple[str, float]:
    if len(closes) < 5:
        return "flat", 0.5
    last = closes[-1]
    avg = sum(closes[-5:]) / 5.0
    if last > avg * 1.003:
        return "up", 0.65
    if last < avg * 0.997:
        return "down", 0.65
    return "flat", 0.55


# ============================================================
#  ENDPOINTS
# ============================================================

@app.get("/health")
async def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "ts": int(time.time()),
        "providers": {
            "cryptocompare": {"base": CC_BASE, "has_key": bool(CC_API_KEY)},
            "coinapi": {"base": COINAPI_BASE, "has_key": bool(COINAPI_KEY)},
        },
        "graph_jobs": list(_local_graph.keys()),
        "gateway_public": PUBLIC_BASE,
    }


@app.get("/live")
async def live(
    symbol: str = Query(..., description="Ticker, e.g. BTCUSDT"),
    tf: str = Query("1m"),
):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, data, status = await unified_live(symbol, tf)
    if not ok:
        return JSONResponse(status_code=status, content=data)

    _cache_put(key, data)
    return {"cached": False, **data}


@app.get("/ohlcv")
async def ohlcv(
    symbol: str = Query(..., description="Ticker, e.g. BTCUSDT"),
    tf: str = Query("1m"),
    limit: int = Query(200, ge=10, le=1000),
):
    key = f"ohlcv:{symbol}:{tf}:{limit}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, data, status = await unified_ohlcv(symbol, tf, limit)
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
async def graph(job_id: str = Path(...)):
    return {"cached": True, **_graph_get(job_id), "fallback": "local"}


@app.post("/ingest-pass")
async def ingest_pass(payload: Dict[str, Any]):
    """
    Stub: просто кладём в локальный graph-storage.
    """
    job_id = payload.get("job_id", "default")
    _graph_put(job_id, payload)
    return {"status": "ok", "fallback": "local"}


@app.post("/indicators/rsi")
async def indicators_rsi(body: Dict[str, Any]):
    closes = body.get("closes") or []
    closes = [float(x) for x in closes]
    value = _rsi(closes)
    return {"rsi": value}


@app.post("/indicators/macd")
async def indicators_macd(body: Dict[str, Any]):
    closes = body.get("closes") or []
    closes = [float(x) for x in closes]
    macd_val, signal_val = _macd(closes)
    return {"macd": macd_val, "signal": signal_val}


@app.post("/indicators/atr")
async def indicators_atr(body: Dict[str, Any]):
    highs = [float(x) for x in (body.get("highs") or [])]
    lows = [float(x) for x in (body.get("lows") or [])]
    closes = [float(x) for x in (body.get("closes") or [])]
    value = _atr(highs, lows, closes)
    return {"atr": value}


@app.post("/risk/calc")
async def risk_calc(body: Dict[str, Any]):
    closes = [float(x) for x in (body.get("closes") or [])]
    score, level = _risk_score(closes)
    return {"riskScore": score, "riskLevel": level}


@app.post("/radar/forecast")
async def radar_forecast(body: Dict[str, Any]):
    closes = [float(x) for x in (body.get("closes") or [])]
    direction, confidence = _radar_forecast(closes)
    return {"direction": direction, "confidence": confidence}


@app.post("/snapshot/create")
async def snapshot_create(body: Dict[str, Any]):
    # Пока просто подтверждаем получение
    return {"status": "ok"}


@app.post("/events/route")
async def events_route(body: Dict[str, Any]):
    # Stub: просто отмечаем, что событие принято
    return {"ok": True}


@app.post("/governance/policy")
async def governance_policy(body: Dict[str, Any]):
    # Stub: считаем, что политика применена
    return {"applied": True}


# ============================================================
#  STARTUP / SHUTDOWN (если нужно фоновые задачи — можно добавить)
# ============================================================

@app.on_event("startup")
async def _on_startup():
    # место для фонового фидера, если понадобится
    pass


@app.on_event("shutdown")
async def _on_shutdown():
    pass
