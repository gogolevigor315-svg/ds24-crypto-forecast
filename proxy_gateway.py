# ds24-proxy-gateway v3.2 (Binance + CryptoCompare + CoinAPI, with orderbook depth)
# Balanced mode, готово для ISKRA3 RealFlow

import os
import time
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

APP_NAME = "ds24-proxy-gateway"

# ============================================================
# CONFIG
# ============================================================

PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://ds24-crypto-forecast-1.onrender.com")

# Binance (основной провайдер, без ключа)
BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com")

# CryptoCompare (опционально, по ключу)
CC_BASE = os.getenv("CC_BASE", "https://min-api.cryptocompare.com")
CC_API_KEY = os.getenv("CC_API_KEY", "")

# CoinAPI (опционально, по ключу)
COINAPI_BASE = os.getenv("COINAPI_BASE", "https://rest.coinapi.io")
COINAPI_KEY = os.getenv("COINAPI_KEY", "")

# Кеш: Balanced режим
CACHE_TTL = int(os.getenv("CACHE_TTL", "3"))           # live-данные
TOP10_TTL = int(os.getenv("TOP10_TTL", "30"))          # top10
DEPTH_LIMIT = int(os.getenv("DEPTH_LIMIT", "10"))      # уровни стакана

TOP10 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT"
]

# ============================================================
# LOCAL GRAPH STORAGE
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
# FASTAPI INIT
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


def _cache_put(key: str, data: Any, ttl: int) -> None:
    _cache[key] = (data, time.time() + ttl)


# ============================================================
# HTTP HELPER
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
                    if "json" in ct:
                        body = r.json()
                except Exception:
                    pass
                return False, {
                    "upstream_status": r.status_code,
                    "upstream_body": body,
                    "url": url,
                    "params": params,
                }, r.status_code

            if "json" in ct:
                return True, r.json(), 200

            return True, {"raw": r.text}, 200

    except Exception as e:
        return False, {"error": str(e), "url": url, "params": params}, 502


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_symbol(symbol: str) -> Tuple[str, str]:
    symbol = symbol.upper()
    for q in ["USDT", "USD", "USDC", "EUR", "BTC"]:
        if symbol.endswith(q):
            return symbol[:-len(q)], q
    return symbol, "USDT"


# ============================================================
# PROVIDERS: BINANCE
# ============================================================

async def binance_24h(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    """Основной источник цены и 24h статистики."""
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"
    params = {"symbol": symbol.upper()}
    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, None, data

    try:
        return True, {
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


async def binance_depth(symbol: str, limit: int = DEPTH_LIMIT) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    """Глубина стакана (orderbook)."""
    url = f"{BINANCE_BASE}/api/v3/depth"
    params = {"symbol": symbol.upper(), "limit": int(limit)}
    ok, data, status = await _get_json(url, params=params)
    if not ok:
        return False, None, data

    try:
        bids = [[float(p), float(q)] for p, q in data.get("bids", [])]
        asks = [[float(p), float(q)] for p, q in data.get("asks", [])]
        return True, {"bids": bids, "asks": asks}, data
    except Exception as e:
        return False, None, {"error": "depth_parse", "details": str(e), "raw": data}


async def binance_top10() -> Tuple[bool, Optional[List[Dict[str, Any]]], Any]:
    """Топ-10 по нашему списку через Binance 24hr tickers."""
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"
    ok, data, status = await _get_json(url)
    if not ok:
        return False, None, data

    try:
        out: List[Dict[str, Any]] = []
        for item in data:
            sym = str(item.get("symbol", "")).upper()
            if sym not in TOP10:
                continue

            out.append({
                "symbol": sym,
                "price": float(item.get("lastPrice", 0.0)),
                "change24h": float(item.get("priceChangePercent", 0.0)) / 100.0,
                "volume": float(item.get("quoteVolume", 0.0)),
                "high": float(item.get("highPrice", 0.0)),
                "low": float(item.get("lowPrice", 0.0)),
                "bid1": float(item.get("bidPrice", 0.0)),
                "ask1": float(item.get("askPrice", 0.0)),
            })

        out_sorted = sorted(out, key=lambda x: TOP10.index(x["symbol"]))
        return True, out_sorted[:10], data
    except Exception as e:
        return False, None, {"error": "binance_top10_parse", "details": str(e), "raw": data}


# ============================================================
# PROVIDERS: CRYPTOCOMPARE (OPTIONAL)
# ============================================================

def _cc_headers() -> Dict[str, str]:
    return {"authorization": f"Apikey {CC_API_KEY}"} if CC_API_KEY else {}


async def cc_live(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    if not CC_API_KEY:
        return False, None, {"error": "no_cc_key"}

    base, quote = _split_symbol(symbol)
    url = f"{CC_BASE}/data/pricemultifull"
    params = {"fsyms": base, "tsyms": quote}
    ok, data, status = await _get_json(url, params=params, headers=_cc_headers())
    if not ok:
        return False, None, data

    try:
        raw = data["RAW"][base][quote]
        return True, {
            "price": float(raw.get("PRICE", 0.0)),
            "change24h": float(raw.get("CHANGEPCT24HOUR", 0.0)) / 100.0,
            "volume24h": float(raw.get("TOTALVOLUME24H", 0.0)),
            "high24h": float(raw.get("HIGH24HOUR", 0.0)),
            "low24h": float(raw.get("LOW24HOUR", 0.0)),
            "bid": float(raw.get("BID", 0.0)),
            "ask": float(raw.get("ASK", 0.0)),
        }, data
    except Exception as e:
        return False, None, {"error": "cc_parse", "details": str(e), "raw": data}


# ============================================================
# PROVIDERS: COINAPI (OPTIONAL)
# ============================================================

def _coinapi_headers() -> Dict[str, str]:
    return {"X-CoinAPI-Key": COINAPI_KEY} if COINAPI_KEY else {}


async def coinapi_live(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    if not COINAPI_KEY:
        return False, None, {"error": "no_coinapi_key"}

    base, quote = _split_symbol(symbol)
    url = f"{COINAPI_BASE}/v1/exchangerate/{base}/{quote}"
    ok, data, status = await _get_json(url, headers=_coinapi_headers())
    if not ok:
        return False, None, data

    try:
        return True, {"price": float(data.get("rate", 0.0))}, data
    except Exception as e:
        return False, None, {"error": "coinapi_parse", "details": str(e), "raw": data}


# ============================================================
# UNIFIED LAYER (BALANCED + DEPTH)
# ============================================================

def _approx_rsi(price: Optional[float], low: Optional[float], high: Optional[float]) -> float:
    if price is None or low is None or high is None:
        return 50.0
    if high <= low:
        return 50.0
    return max(0.0, min(100.0, 100.0 * (price - low) / (high - low)))


async def unified_live(symbol: str, tf: str) -> Tuple[bool, Dict[str, Any], int]:
    # Binance всегда пробуем первым
    bn_ok, bn_data, bn_raw = await binance_24h(symbol)
    depth_ok, depth_data, depth_raw = await binance_depth(symbol, DEPTH_LIMIT)

    # Опциональные провайдеры — только если есть ключи
    cc_ok = cc_data = cc_raw = None
    ca_ok = ca_data = ca_raw = None

    if CC_API_KEY:
        cc_ok, cc_data, cc_raw = await cc_live(symbol)
    if COINAPI_KEY:
        ca_ok, ca_data, ca_raw = await coinapi_live(symbol)

    if not bn_ok and not (cc_ok or ca_ok):
        return False, {
            "proxy": APP_NAME,
            "error": "no_provider_ok",
            "binance": bn_raw,
            "cryptocompare": cc_raw,
            "coinapi": ca_raw,
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
    providers_used: List[str] = []

    # 1) Binance как базовый
    if bn_ok and bn_data:
        providers_used.append("binance")
        price = bn_data["price"]
        features["change24h"] = bn_data["change24h"]
        features["volume24h"] = bn_data["volume24h"]
        features["high24h"] = bn_data["high24h"]
        features["low24h"] = bn_data["low24h"]
        features["bid"] = bn_data["bid"]
        features["ask"] = bn_data["ask"]

    # 2) CryptoCompare — уточнение и кросс-проверка
    if cc_ok and cc_data:
        providers_used.append("cryptocompare")
        if price is not None and cc_data["price"] is not None:
            price = (price + cc_data["price"]) / 2.0

    # 3) CoinAPI — если есть, только sanity-check цены
    if ca_ok and ca_data:
        providers_used.append("coinapi")
        if price is not None and ca_data["price"] is not None:
            price = (price + ca_data["price"]) / 2.0

    rsi_approx = _approx_rsi(price, features["low24h"], features["high24h"])
    vol_index = min(1.0, abs(features["change24h"] or 0.0) / 0.10)

    event: Dict[str, Any] = {
        "ts": _now_iso(),
        "symbol": symbol.upper(),
        "tf": tf,
        "price": price,
        "features": {
            **features,
            "rsi_approx": rsi_approx,
            "volatility_index": vol_index,
        },
        "forecast": {},
        "source": "+".join(providers_used) if providers_used else "unknown",
        "q_score": None,
    }

    if depth_ok and depth_data:
        event["orderbook"] = {
            "provider": "binance",
            "depth_limit": DEPTH_LIMIT,
            "bids": depth_data["bids"],
            "asks": depth_data["asks"],
        }

    return True, event, 200


async def unified_top10() -> Tuple[bool, Dict[str, Any], int]:
    # Сначала пробуем Binance
    bn_ok, bn_list, bn_raw = await binance_top10()
    if bn_ok and bn_list:
        return True, {"provider": "binance", "top10": bn_list}, 200

    # Фоллбек на CryptoCompare при наличии ключа
    if CC_API_KEY:
        # Можно переиспользовать старую логику, но если CC закрыт — не ломаемся
        return False, {
            "proxy": APP_NAME,
            "error": "top10_failed",
            "binance": bn_raw,
        }, 502

    return False, {
        "proxy": APP_NAME,
        "error": "top10_failed_no_provider",
        "binance": bn_raw,
    }, 502


# ============================================================
# INDICATORS / RISK / RADAR
# ============================================================

def _rsi(values: List[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = values[-i] - values[-i - 1]
        (gains if diff >= 0 else losses).append(abs(diff))
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 1e-9
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _macd(values: List[float]) -> Tuple[float, float]:
    if len(values) < 35:
        return 0.0, 0.0

    def ema(v: List[float], p: int) -> List[float]:
        k = 2.0 / (p + 1.0)
        e = [v[0]]
        for x in v[1:]:
            e.append(x * k + e[-1] * (1.0 - k))
        return e

    fast = ema(values, 12)
    slow = ema(values, 26)
    macd_line = [f - s for f, s in zip(fast[-len(slow):], slow)]
    signal = ema(macd_line, 9)
    return macd_line[-1], signal[-1]


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    if len(highs) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        tr = max(
            highs[-i] - lows[-i],
            abs(highs[-i] - closes[-i - 1]),
            abs(lows[-i] - closes[-i - 1]),
        )
        trs.append(tr)
    return sum(trs) / period


def _risk(closes: List[float]) -> Tuple[float, str]:
    if len(closes) < 3:
        return 0.5, "medium"
    returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes))]
    vol = (sum(r * r for r in returns) / len(returns)) ** 0.5
    if vol < 0.01:
        return vol, "low"
    if vol < 0.03:
        return vol, "medium"
    return vol, "high"


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
# ENDPOINTS
# ============================================================

@app.get("/health")
async def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "ts": int(time.time()),
        "providers": {
            "binance": True,
            "cryptocompare": bool(CC_API_KEY),
            "coinapi": bool(COINAPI_KEY),
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

    _cache_put(key, data, ttl=CACHE_TTL)
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

    _cache_put(key, data, ttl=TOP10_TTL)
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
async def _startup():
    # тут можно будет подвесить фонового фидера, если понадобится
    pass
