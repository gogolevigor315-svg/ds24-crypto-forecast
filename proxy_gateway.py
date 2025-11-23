# ======================================================================
# ds24-proxy-gateway v4.1 · MultiFeed Edition (Binance Proxy-Ready)
# Binance (core) + CryptoCompare (assist), без CoinAPI
# Готово для ISKRA3 RealFlow / DS24 stack
#
# Особенности:
# - Binance: основной поставщик цены, объёмов, стакана, top10.
# - CryptoCompare: вторичный провайдер для кросс-проверки и уточнения цены.
# - Если CC даёт адекватные данные → цена усредняется, растёт confidence.
# - Если CC даёт аномалию → Binance остаётся единственным источником.
# - CoinAPI убран полностью, чтобы не плодить ошибки и шум.
# - Добавлен обход региональных ограничений Binance через BINANCE_PROXY_BASE.
# ======================================================================

import os
import time
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

APP_NAME = "ds24-proxy-gateway-v4.1"

# ============================================================
# CONFIG
# ============================================================

PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://ds24-crypto-forecast-1.onrender.com")

# Binance (основной провайдер, без ключа)
BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com")
# Резервный бинанс-бэйс для обхода региональных ограничений (через прокси/воркер)
# Если не задан, по умолчанию равен BINANCE_BASE (т.е. без прокси).
BINANCE_PROXY_BASE = os.getenv("BINANCE_PROXY_BASE", BINANCE_BASE)

# CryptoCompare (вторичный провайдер, с ключом)
CC_BASE = os.getenv("CC_BASE", "https://min-api.cryptocompare.com")
# Твой ключ: можно переопределить через ENV CC_API_KEY, но по умолчанию он уже здесь
CC_API_KEY = os.getenv("CC_API_KEY", "18421fbe-d8b5-45d9-a291-2091e39c21a4").strip()

# Кеш: Balanced режим
CACHE_TTL = int(os.getenv("CACHE_TTL", "3"))           # live-данные
TOP10_TTL = int(os.getenv("TOP10_TTL", "30"))          # top10
DEPTH_LIMIT = int(os.getenv("DEPTH_LIMIT", "10"))      # уровни стакана

TOP10 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT",
]

# ============================================================
# LOCAL GRAPH STORAGE
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
    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _local_graph[job_id] = g


# ============================================================
# FASTAPI INIT
# ============================================================

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware(
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
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
# HELPERS
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
        # сетевые ошибки
        return False, {"error": str(e), "url": url, "params": params}, 502


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_symbol(symbol: str) -> Tuple[str, str]:
    symbol = symbol.upper()
    for q in ["USDT", "USD", "USDC", "EUR", "BTC"]:
        if symbol.endswith(q):
            return symbol[:-len(q)], q
    return symbol, "USDT"


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


# ============================================================
# BINANCE HELPER (c обходом региональных ограничений)
# ============================================================

async def _binance_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Any, int, Dict[str, Any]]:
    """
    Унифицированный вызов Binance с fallback на BINANCE_PROXY_BASE.
    Сначала пробуем BINANCE_BASE. Если 451/403/502 или явная сетевая ошибка —
    пробуем BINANCE_PROXY_BASE. Диагностика возвращается в extra.
    """
    diagnostics: Dict[str, Any] = {
        "base": BINANCE_BASE,
        "proxy_base": BINANCE_PROXY_BASE,
        "used_proxy": False,
        "primary_status": None,
        "proxy_status": None,
    }

    # 1) Пытаемся пойти на основной BINANCE_BASE
    url_main = f"{BINANCE_BASE}{path}"
    ok, data, status = await _get_json(url_main, params=params)
    diagnostics["primary_status"] = status

    # успешный ответ → возвращаем сразу
    if ok:
        return True, data, status, diagnostics

    # Если нет успеха, проверяем, надо ли пробовать прокси
    upstream_status = None
    if isinstance(data, dict):
        upstream_status = data.get("upstream_status")

    # Если статус указывает на геоблок/доступ, пробуем прокси
    try_proxy = False
    if upstream_status in (451, 403):
        try_proxy = True
    elif status in (451, 403, 502):
        try_proxy = True

    # Если прокси такой же как основной — пробовать бессмысленно
    if BINANCE_PROXY_BASE == BINANCE_BASE:
        try_proxy = False

    if try_proxy:
        url_proxy = f"{BINANCE_PROXY_BASE}{path}"
        ok_p, data_p, status_p = await _get_json(url_proxy, params=params)
        diagnostics["used_proxy"] = True
        diagnostics["proxy_status"] = status_p

        if ok_p:
            return True, data_p, status_p, diagnostics
        # если даже прокси не помог — возвращаем ошибку прокси
        return False, data_p, status_p, diagnostics

    # если не решили пробовать прокси — возвращаем что есть
    return False, data, status, diagnostics


# ============================================================
# PROVIDERS: BINANCE
# ============================================================

async def binance_24h(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any, Dict[str, Any]]:
    """Основной источник цены и 24h статистики."""
    path = "/api/v3/ticker/24hr"
    params = {"symbol": symbol.upper()}
    ok, data, status, diag = await _binance_get(path, params=params)
    if not ok:
        return False, None, data, diag

    try:
        return True, {
            "price": _safe_float(data.get("lastPrice", 0.0)),
            "change24h": _safe_float(data.get("priceChangePercent", 0.0)) / 100.0,
            "volume24h": _safe_float(data.get("quoteVolume", 0.0)),
            "high24h": _safe_float(data.get("highPrice", 0.0)),
            "low24h": _safe_float(data.get("lowPrice", 0.0)),
            "bid": _safe_float(data.get("bidPrice", 0.0)),
            "ask": _safe_float(data.get("askPrice", 0.0)),
        }, data, diag
    except Exception as e:
        return False, None, {"error": "binance_parse", "details": str(e), "raw": data}, diag


async def binance_depth(symbol: str, limit: int = DEPTH_LIMIT) -> Tuple[bool, Optional[Dict[str, Any]], Any, Dict[str, Any]]:
    """Глубина стакана (orderbook)."""
    path = "/api/v3/depth"
    params = {"symbol": symbol.upper(), "limit": int(limit)}
    ok, data, status, diag = await _binance_get(path, params=params)
    if not ok:
        return False, None, data, diag

    try:
        bids = [[_safe_float(p), _safe_float(q)] for p, q in data.get("bids", [])]
        asks = [[_safe_float(p), _safe_float(q)] for p, q in data.get("asks", [])]
        return True, {"bids": bids, "asks": asks}, data, diag
    except Exception as e:
        return False, None, {"error": "depth_parse", "details": str(e), "raw": data}, diag


async def binance_top10() -> Tuple[bool, Optional[List[Dict[str, Any]]], Any, Dict[str, Any]]:
    """Топ-10 по нашему списку через Binance 24hr tickers."""
    path = "/api/v3/ticker/24hr"
    ok, data, status, diag = await _binance_get(path)
    if not ok:
        return False, None, data, diag

    try:
        out: List[Dict[str, Any]] = []
        for item in data:
            sym = str(item.get("symbol", "")).upper()
            if sym not in TOP10:
                continue

            out.append({
                "symbol": sym,
                "price": _safe_float(item.get("lastPrice", 0.0)),
                "change24h": _safe_float(item.get("priceChangePercent", 0.0)) / 100.0,
                "volume": _safe_float(item.get("quoteVolume", 0.0)),
                "high": _safe_float(item.get("highPrice", 0.0)),
                "low": _safe_float(item.get("lowPrice", 0.0)),
                "bid1": _safe_float(item.get("bidPrice", 0.0)),
                "ask1": _safe_float(item.get("askPrice", 0.0)),
            })

        out_sorted = sorted(out, key=lambda x: TOP10.index(x["symbol"]))
        return True, out_sorted[:10], data, diag
    except Exception as e:
        return False, None, {"error": "binance_top10_parse", "details": str(e), "raw": data}, diag


# ============================================================
# PROVIDER: CRYPTOCOMPARE
# ============================================================

def _cc_headers() -> Dict[str, str]:
    if CC_API_KEY:
        return {"authorization": f"Apikey {CC_API_KEY}"}
    return {}


async def cc_live(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]], Any]:
    """
    CryptoCompare live data (2nd provider).
    Требует CC_API_KEY. Без ключа — мягкий отказ, не ломающий unified_live.
    """
    if not CC_API_KEY:
        return False, None, {"error": "no_cc_key", "hint": "set CC_API_KEY to enable CryptoCompare"}

    base, quote = _split_symbol(symbol)
    url = f"{CC_BASE}/data/pricemultifull"
    params = {"fsyms": base, "tsyms": quote}
    ok, data, status = await _get_json(url, params=params, headers=_cc_headers())
    if not ok:
        return False, None, data

    try:
        raw = data["RAW"][base][quote]
        return True, {
            "price": _safe_float(raw.get("PRICE", 0.0)),
            "change24h": _safe_float(raw.get("CHANGEPCT24HOUR", 0.0)) / 100.0,
            "volume24h": _safe_float(raw.get("TOTALVOLUME24H", 0.0)),
            "high24h": _safe_float(raw.get("HIGH24HOUR", 0.0)),
            "low24h": _safe_float(raw.get("LOW24HOUR", 0.0)),
            "bid": _safe_float(raw.get("BID", 0.0)),
            "ask": _safe_float(raw.get("ASK", 0.0)),
        }, data
    except Exception as e:
        return False, None, {"error": "cc_parse", "details": str(e), "raw": data}


# ============================================================
# UNIFIED LAYER (Hybrid Pro: Binance-Core + CC-Assist)
# ============================================================

def _approx_rsi(price: Optional[float], low: Optional[float], high: Optional[float]) -> float:
    if price is None or low is None or high is None:
        return 50.0
    if high <= low:
        return 50.0
    return max(0.0, min(100.0, 100.0 * (price - low) / (high - low)))


async def unified_live(symbol: str, tf: str) -> Tuple[bool, Dict[str, Any], int]:
    # 1) Binance как ядро (через helper с прокси-обходом)
    bn_ok, bn_data, bn_raw, bn_diag = await binance_24h(symbol)
    depth_ok, depth_data, depth_raw, depth_diag = await binance_depth(symbol, DEPTH_LIMIT)

    # 2) CryptoCompare как ассистент
    cc_ok = cc_data = cc_raw = None
    if CC_API_KEY:
        cc_ok, cc_data, cc_raw = await cc_live(symbol)

    if not bn_ok and not cc_ok:
        return False, {
            "proxy": APP_NAME,
            "error": "no_provider_ok",
            "binance": bn_raw,
            "binance_diag": bn_diag,
            "cryptocompare": cc_raw,
        }, 502

    providers_used: List[str] = []
    diagnostics: Dict[str, Any] = {
        "binance_ok": bn_ok,
        "binance_diag": bn_diag,
        "binance_depth_ok": depth_ok,
        "binance_depth_diag": depth_diag,
        "cc_ok": cc_ok,
        "cc_used": False,
        "cc_delta_pct": None,
    }

    # базовая цена и фичи — из Binance, если доступен
    price: Optional[float] = None
    features = {
        "change24h": None,
        "volume24h": None,
        "high24h": None,
        "low24h": None,
        "bid": None,
        "ask": None,
    }

    if bn_ok and bn_data:
        providers_used.append("binance")
        price = bn_data["price"]
        features["change24h"] = bn_data["change24h"]
        features["volume24h"] = bn_data["volume24h"]
        features["high24h"] = bn_data["high24h"]
        features["low24h"] = bn_data["low24h"]
        features["bid"] = bn_data["bid"]
        features["ask"] = bn_data["ask"]
    elif cc_ok and cc_data:
        # если Binance отвалился — fallback на CC
        providers_used.append("cryptocompare")
        price = cc_data["price"]
        features["change24h"] = cc_data.get("change24h")
        features["volume24h"] = cc_data.get("volume24h")
        features["high24h"] = cc_data.get("high24h")
        features["low24h"] = cc_data.get("low24h")
        features["bid"] = cc_data.get("bid")
        features["ask"] = cc_data.get("ask")

    # Hybrid Pro логика: если оба живы — проверяем расхождение
    confidence = 0.7  # базовое
    if bn_ok and bn_data and cc_ok and cc_data and price is not None:
        cc_price = cc_data["price"]
        if cc_price is not None and cc_price > 0 and price > 0:
            delta = abs(cc_price - price) / price
            diagnostics["cc_delta_pct"] = delta

            if delta < 0.01:
                # почти идентичные цены → усреднение + высокий confidence
                price = (price + cc_price) / 2.0
                providers_used.append("cryptocompare")
                diagnostics["cc_used"] = True
                confidence = 0.96
            elif delta < 0.03:
                # умеренное расхождение → осторожное усреднение + средний confidence
                price = (price * 0.7 + cc_price * 0.3)
                providers_used.append("cryptocompare")
                diagnostics["cc_used"] = True
                confidence = 0.85
            else:
                # сильное расхождение → игнорируем CC, остаёмся на Binance
                diagnostics["cc_used"] = False
                confidence = 0.75
        else:
            diagnostics["cc_delta_pct"] = None

    elif bn_ok and bn_data:
        confidence = 0.9
    elif cc_ok and cc_data:
        confidence = 0.8

    rsi_approx = _approx_rsi(price, features["low24h"], features["high24h"])
    vol_index = None
    if features["change24h"] is not None:
        vol_index = min(1.0, abs(features["change24h"]) / 0.10)

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
        "forecast": {},  # сюда можно писать ISKRA-прогнозы
        "source": "+".join(sorted(set(providers_used))) if providers_used else "unknown",
        "q_score": confidence,
        "diagnostics": diagnostics,
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
    # Топ-10 оставляем чисто на Binance — он надёжный и быстрый (через прокси-хелпер)
    bn_ok, bn_list, bn_raw, bn_diag = await binance_top10()
    if bn_ok and bn_list:
        return True, {"provider": "binance", "top10": bn_list, "diagnostics": {"binance": bn_diag}}, 200

    return False, {
        "proxy": APP_NAME,
        "error": "top10_failed",
        "binance": bn_raw,
        "binance_diag": bn_diag,
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

@app.get("/")
async def root():
    return {"ok": True, "app": APP_NAME, "msg": "DS24 ISKRA3 Gateway v4.1 running"}


@app.get("/health")
async def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "ts": int(time.time()),
        "providers": {
            "binance": {
                "enabled": True,
                "base": BINANCE_BASE,
                "proxy_base": BINANCE_PROXY_BASE,
            },
            "cryptocompare": {
                "enabled": bool(CC_API_KEY),
                "requires_key": True,
                "has_key": bool(CC_API_KEY),
                "base": CC_BASE,
            },
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
    # сюда при желании можно повесить фонового фидера
    pass
