# ds24-proxy-gateway v4.0
# REALDATA gateway for ISKRA3
# Binance (основа) + CoinGecko (резерв), без API-ключей

import os
import time
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

BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com")
COINGECKO_BASE = os.getenv("COINGECKO_BASE", "https://api.coingecko.com/api/v3")

CACHE_TTL = int(os.getenv("CACHE_TTL", "10"))

TOP10 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT",
]

# для CoinGecko: тикер → coin_id
COINGECKO_ID_MAP: Dict[str, str] = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "BNBUSDT": "binancecoin",
    "XRPUSDT": "ripple",
    "DOGEUSDT": "dogecoin",
    "TONUSDT": "toncoin",
    "ADAUSDT": "cardano",
    "AVAXUSDT": "avalanche-2",
    "LINKUSDT": "chainlink",
}

# ============================================================
#  LOCAL GRAPH STORAGE
# ============================================================

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
    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    _local_graph[job_id] = g


# ============================================================
#  FASTAPI
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
#  HTTP HELPER
# ============================================================

async def _get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[bool, Any, int]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params, headers=headers)
            ct = (r.headers.get("content-type") or "").lower()

            if r.status_code >= 400:
                body = r.text
                if "json" in ct:
                    try:
                        body = r.json()
                    except Exception:
                        pass
                return False, {
                    "upstream_status": r.status_code,
                    "upstream_body": body,
                }, r.status_code

            if "json" in ct:
                return True, r.json(), 200

            return True, {"raw": r.text}, 200

    except Exception as e:
        return False, {"error": str(e)}, 502


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================================================
#  BINANCE CLIENT
# ============================================================

async def binance_ticker_24h(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    """
    /api/v3/ticker/24hr — основа для live-данных.
    """
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"
    params = {"symbol": symbol.upper()}
    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, None, data

    try:
        return True, {
            "symbol": data.get("symbol", symbol.upper()),
            "price": float(data.get("lastPrice", 0.0)),
            "change24h": float(data.get("priceChangePercent", 0.0)) / 100.0,
            "volume24h": float(data.get("quoteVolume", 0.0)),
            "high24h": float(data.get("highPrice", 0.0)),
            "low24h": float(data.get("lowPrice", 0.0)),
            "bid": float(data.get("bidPrice", 0.0)),
            "ask": float(data.get("askPrice", 0.0)),
        }, data
    except Exception as e:
        return False, None, {"error": "binance_parse", "details": str(e), "raw": data}


def _map_tf_to_binance_interval(tf: str) -> str:
    tf = tf.lower()
    mapping = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    return mapping.get(tf, "1m")


async def binance_klines(
    symbol: str, tf: str, limit: int
) -> Tuple[bool, Optional[List[Dict[str, Any]]], Any]:
    """
    /api/v3/klines — свечи.
    """
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": _map_tf_to_binance_interval(tf),
        "limit": limit,
    }
    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, None, data

    try:
        bars: List[Dict[str, Any]] = []
        for k in data:
            # формат: [openTime, open, high, low, close, volume, closeTime, ...]
            open_time = int(k[0])
            bars.append(
                {
                    "ts": datetime.fromtimestamp(open_time / 1000.0, tz=timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                }
            )
        return True, bars, data
    except Exception as e:
        return False, None, {"error": "binance_klines_parse", "details": str(e), "raw": data}


async def binance_top10() -> Tuple[bool, Optional[List[Dict[str, Any]]], Any]:
    """
    /api/v3/ticker/24hr (все), фильтруем TOP10, режем до 10.
    """
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"
    ok, data, status = await _get_json(url)
    if not ok:
        return False, None, data

    try:
        if not isinstance(data, list):
            return False, None, {"error": "unexpected_binance_format", "raw": data}

        by_symbol: Dict[str, Dict[str, Any]] = {item.get("symbol"): item for item in data}
        out: List[Dict[str, Any]] = []

        for sym in TOP10:
            item = by_symbol.get(sym)
            if not item:
                continue
            out.append(
                {
                    "symbol": sym,
                    "price": float(item.get("lastPrice", 0.0)),
                    "change24h": float(item.get("priceChangePercent", 0.0)) / 100.0,
                    "volume": float(item.get("quoteVolume", 0.0)),
                    "high": float(item.get("highPrice", 0.0)),
                    "low": float(item.get("lowPrice", 0.0)),
                    "bid1": float(item.get("bidPrice", 0.0)),
                    "ask1": float(item.get("askPrice", 0.0)),
                }
            )

        return True, out[:10], data
    except Exception as e:
        return False, None, {"error": "binance_top10_parse", "details": str(e), "raw": data}


# ============================================================
#  COINGECKO (BACKUP)
# ============================================================

async def coingecko_price(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    """
    Резервный источник цены/24h-change.
    """
    coin_id = COINGECKO_ID_MAP.get(symbol.upper())
    if not coin_id:
        return False, None, {"error": "no_coingecko_id", "symbol": symbol}

    url = f"{COINGECKO_BASE}/simple/price"
    params = {
        "ids": coin_id,
        "vs_currencies": "usd",
        "include_24hr_change": "true",
        "include_24hr_vol": "true",
        "include_high_24h": "true",
        "include_low_24h": "true",
    }
    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, None, data

    try:
        d = data.get(coin_id, {})
        price = float(d.get("usd", 0.0))
        change = float(d.get("usd_24h_change", 0.0)) / 100.0
        vol = float(d.get("usd_24h_vol", 0.0)) if d.get("usd_24h_vol") is not None else None
        high = float(d.get("usd_24h_high", 0.0)) if d.get("usd_24h_high") is not None else None
        low = float(d.get("usd_24h_low", 0.0)) if d.get("usd_24h_low") is not None else None

        return True, {
            "price": price,
            "change24h": change,
            "volume24h": vol,
            "high24h": high,
            "low24h": low,
        }, data
    except Exception as e:
        return False, None, {"error": "coingecko_parse", "details": str(e), "raw": data}


# ============================================================
#  UNIFIED LAYER
# ============================================================

def _approx_rsi_from_range(
    price: Optional[float], low: Optional[float], high: Optional[float]
) -> float:
    if price is None or low is None or high is None:
        return 50.0
    if high <= low:
        return 50.0
    val = 100.0 * (price - low) / (high - low)
    if val < 0.0:
        val = 0.0
    if val > 100.0:
        val = 100.0
    return float(val)


async def unified_live(symbol: str, tf: str) -> Tuple[bool, Dict[str, Any], int]:
    """
    Объединяем Binance (основной) + CoinGecko (резерв).
    """
    bin_ok, bin_data, bin_raw = await binance_ticker_24h(symbol)
    cg_ok, cg_data, cg_raw = await coingecko_price(symbol)

    if not bin_ok and not cg_ok:
        return False, {
            "proxy": APP_NAME,
            "error": "providers_failed",
            "binance": bin_raw,
            "coingecko": cg_raw,
        }, 502

    price: Optional[float] = None
    change24h: Optional[float] = None
    volume24h: Optional[float] = None
    high24h: Optional[float] = None
    low24h: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None

    providers_used: List[str] = []

    if bin_ok and bin_data:
        providers_used.append("binance")
        price = bin_data["price"]
        change24h = bin_data["change24h"]
        volume24h = bin_data["volume24h"]
        high24h = bin_data["high24h"]
        low24h = bin_data["low24h"]
        bid = bin_data["bid"]
        ask = bin_data["ask"]

    if cg_ok and cg_data:
        providers_used.append("coingecko")
        # если бинанс дал цену — усредняем, если нет — берём с CG
        cg_price = cg_data.get("price")
        if price is None and cg_price is not None:
            price = cg_price
        elif price is not None and cg_price is not None:
            price = (price + cg_price) / 2.0

        if change24h is None and cg_data.get("change24h") is not None:
            change24h = cg_data["change24h"]
        if volume24h is None and cg_data.get("volume24h") is not None:
            volume24h = cg_data["volume24h"]
        if high24h is None and cg_data.get("high24h") is not None:
            high24h = cg_data["high24h"]
        if low24h is None and cg_data.get("low24h") is not None:
            low24h = cg_data["low24h"]

    rsi_approx = _approx_rsi_from_range(price, low24h, high24h)
    vol_index = abs(change24h or 0.0)
    if vol_index > 1.0:
        vol_index = 1.0
    vol_index = float(vol_index / 0.10) if vol_index is not None else 0.0
    if vol_index > 1.0:
        vol_index = 1.0

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
        "source": "+".join(providers_used),
        "q_score": None,
        "debug": {
            "providers_used": providers_used,
        },
    }

    return True, event, 200


async def unified_ohlcv(symbol: str, tf: str, limit: int) -> Tuple[bool, Dict[str, Any], int]:
    bin_ok, bins, bin_raw = await binance_klines(symbol, tf, limit)
    if not bin_ok or bins is None:
        return False, {
            "proxy": APP_NAME,
            "error": "ohlcv_failed",
            "binance": bin_raw,
        }, 502

    return True, {
        "symbol": symbol.upper(),
        "tf": tf,
        "bars": bins,
        "providers_used": ["binance"],
    }, 200


async def unified_top10() -> Tuple[bool, Dict[str, Any], int]:
    ok, items, raw = await binance_top10()
    if not ok or items is None:
        return False, {
            "proxy": APP_NAME,
            "error": "top10_failed",
            "binance": raw,
        }, 502

    # строго 10 записей, без лишних полей
    return True, {"provider": "binance", "top10": items[:10]}, 200


# ============================================================
#  INDICATORS / RISK / RADAR
# ============================================================

def _rsi(values: List[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, period + 1):
        diff = values[-i] - values[-i - 1]
        if diff >= 0:
            gains.append(diff)
        else:
            losses.append(-diff)
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 1e-9
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _macd(values: List[float]) -> Tuple[float, float]:
    if len(values) < 35:
        return 0.0, 0.0

    def ema(v: List[float], p: int) -> List[float]:
        k = 2.0 / (p + 1)
        out = [v[0]]
        for x in v[1:]:
            out.append(x * k + out[-1] * (1.0 - k))
        return out

    ema_fast = ema(values, 12)
    ema_slow = ema(values, 26)
    macd_line = [f - s for f, s in zip(ema_fast[-len(ema_slow):], ema_slow)]
    signal_line = ema(macd_line, 9)
    return float(macd_line[-1]), float(signal_line[-1])


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
        return 0.0
    trs: List[float] = []
    for i in range(1, period + 1):
        h = highs[-i]
        l = lows[-i]
        prev_close = closes[-i - 1]
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
    return float(sum(trs) / period)


def _risk(closes: List[float]) -> Tuple[float, str]:
    if len(closes) < 3:
        return 0.5, "medium"
    returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes))]
    vol = (sum(r * r for r in returns) / len(returns)) ** 0.5
    if vol < 0.01:
        return float(vol), "low"
    if vol < 0.03:
        return float(vol), "medium"
    return float(vol), "high"


def _radar(closes: List[float]) -> Tuple[str, float]:
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
            "binance": {"base": BINANCE_BASE},
            "coingecko": {"base": COINGECKO_BASE},
        },
        "graph_jobs": list(_local_graph.keys()),
        "public_base": PUBLIC_BASE,
    }


@app.get("/live")
async def live(symbol: str = Query(...), tf: str = Query("1m")):
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
    symbol: str = Query(...),
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
    macd_val, signal_val = _macd(closes)
    return {"macd": macd_val, "signal": signal_val}


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
    direction, confidence = _radar(closes)
    return {"direction": direction, "confidence": confidence}


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
async def _on_startup():
    # место для будущих фоновых задач (фидер и т.п.)
    pass
