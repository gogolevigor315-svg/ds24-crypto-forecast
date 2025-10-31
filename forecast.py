# forecast.py
import asyncio
import math
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

import numpy as np
import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import os
import logging

# -----------------------------
# Конфигурация и загрузка YAML
# -----------------------------

DEFAULT_CONFIG = {
    "server": {"host": "0.0.0.0", "port": 8081},
    "collector": {
        "url": "http://localhost:8080/prices",
        "timeout_sec": 2.0,
        "retry_delay_sec": 3.0,
    },
    "markets": {
        "pairs": ["BTCUSDT", "ETHUSDT"],
        "exchanges": ["binance", "bybit", "okx", "kraken", "bitstamp"],
    },
    "features": {
        "window_sec": 60,
        "history_limit": 300,
        "rsi_period": 14,
        "ema_periods": [20, 50],
        "volatility_window": 120,
        "min_data_points": 10,
    },
    "signals": {
        "spread_threshold_bps": 5,
        "trend_ema_fast": 20,
        "trend_ema_slow": 50,
        "momentum_threshold": 0.01,
        "confidence_threshold": 0.4,
    },
    "arbitrage": {
        "fees_bps_per_exchange": {
            "binance": 0.1,
            "bybit": 0.15,
            "okx": 0.15,
            "kraken": 0.25,
            "bitstamp": 0.2,
        },
        "min_spread_bps": 5,
        "min_notional": 10.0,
        "refresh_interval_sec": 3.0,
    },
    "logging": {"level": "INFO"},
    "health": {"max_delay_sec": 5.0, "timeout_sec": 1.0},
    "misc": {"allow_cors": True},
}


def load_config(path: str = "forecast_config.yaml") -> Dict:
    cfg = DEFAULT_CONFIG.copy()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            y = yaml.safe_load(f) or {}
        # глубокое обновление
        def deep_update(d, u):
            for k, v in u.items():
                if isinstance(v, dict) and isinstance(d.get(k), dict):
                    deep_update(d[k], v)
                else:
                    d[k] = v
        deep_update(cfg, y)

    # ENV overrides (необязательно)
    cfg["collector"]["url"] = os.getenv("FORECAST_COLLECTOR_URL", cfg["collector"]["url"])
    return cfg


CFG = load_config()
logging.basicConfig(level=getattr(logging, CFG.get("logging", {}).get("level", "INFO")))
log = logging.getLogger("forecast")

# ------------
# Модели API
# ------------

@dataclass
class Ticker:
    symbol: str
    exchange: str
    bid: float
    ask: float
    last: float
    ts: float  # seconds epoch


@dataclass
class ForecastResult:
    symbol: str
    direction: str
    confidence: float
    ema_fast: float
    ema_slow: float
    rsi: float
    volatility: float
    momentum: float
    timestamp: str


@dataclass
class ArbitrageDeal:
    symbol: str
    buy_exchange: str
    sell_exchange: str
    buy_ask: float
    sell_bid: float
    gross_spread_bps: float
    net_spread_bps: float
    notional: float
    profit_abs: float
    timestamp: str


class Health(BaseModel):
    ok: bool
    data_age_sec: Optional[float]
    symbols: List[str]
    exchanges_seen: int
    points_in_history: Dict[str, int]
    timestamp: str


# -----------------------
# Внутренние структуры
# -----------------------

# История цен last по символу
HISTORY: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=CFG["features"]["history_limit"]))
LAST_PULL_TS: Optional[float] = None


# -----------------------
# Вспомогательные функции
# -----------------------

def now_ts() -> float:
    return time.time()


def ts_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ema(series: List[float], period: int) -> float:
    if len(series) < period:
        return float("nan")
    k = 2 / (period + 1)
    e = series[0]
    for v in series[1:]:
        e = v * k + e * (1 - k)
    return float(e)


def rsi(series: List[float], period: int = 14) -> float:
    if len(series) < period + 1:
        return float("nan")
    gains = []
    losses = []
    for i in range(1, period + 1):
        diff = series[-i] - series[-i - 1]
        gains.append(max(0, diff))
        losses.append(max(0, -diff))
    avg_gain = np.mean(gains)
    avg_loss = np.mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


def volatility(series: List[float], window: int) -> float:
    if len(series) < window:
        return float("nan")
    sub = np.array(series[-window:])
    rets = np.diff(sub) / sub[:-1]
    return float(np.std(rets) * 100)  # %


def momentum(series: List[float], window: int = 10) -> float:
    if len(series) < window:
        return float("nan")
    return float((series[-1] - series[-window]) / series[-window])


# -----------------------
# Клиент коллектора
# -----------------------

async def pull_prices() -> List[Ticker]:
    """Тянем данные из Collector /prices. Ожидаемый формат элемента:
    {
      "symbol": "BTCUSDT",
      "exchange": "binance",
      "bid": 68000.1,
      "ask": 68000.3,
      "last": 68000.2,
      "timestamp": 1699999999.123  # сек (или мс; ниже нормализуем)
    }
    """
    url = CFG["collector"]["url"]
    timeout = CFG["collector"]["timeout_sec"]
    retries = 2
    delay = CFG["collector"]["retry_delay_sec"]

    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            async with httpx.AsyncClient(timeout=timeout) as cli:
                r = await cli.get(url)
                r.raise_for_status()
                payload = r.json()
                items = []
                for v in payload.values() if isinstance(payload, dict) else payload:
                    ts_raw = v.get("timestamp", now_ts())
                    # нормализация (мс → сек, если нужно)
                    ts_s = float(ts_raw) / (1000.0 if float(ts_raw) > 10**12 else 1.0)
                    items.append(
                        Ticker(
                            symbol=v["symbol"].upper(),
                            exchange=v["exchange"].lower(),
                            bid=float(v["bid"]),
                            ask=float(v["ask"]),
                            last=float(v.get("last", (v["bid"] + v["ask"]) / 2)),
                            ts=ts_s,
                        )
                    )
                return items
        except Exception as e:
            last_exc = e
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                log.warning(f"Collector request failed: {e}")
    raise HTTPException(status_code=502, detail=f"Collector unavailable: {last_exc}")


# -----------------------
# Обновление истории
# -----------------------

def update_history(tickers: List[Ticker]) -> None:
    global LAST_PULL_TS
    LAST_PULL_TS = now_ts()

    # TTL-фильтр по max_delay_sec
    max_age = CFG["health"]["max_delay_sec"]

    by_symbol: Dict[str, List[Ticker]] = defaultdict(list)
    for t in tickers:
        if (now_ts() - t.ts) <= max_age:
            by_symbol[t.symbol].append(t)

    # средний last по биржам в моменте
    for sym, arr in by_symbol.items():
        if not arr:
            continue
        last_mid = float(np.mean([x.last for x in arr]))
        HISTORY[sym].append((t.ts, last_mid))


# -----------------------
# Логика сигналов/прогноза
# -----------------------

def compute_forecast(symbol: str, tickers: List[Ticker]) -> Optional[ForecastResult]:
    """Возвращает прогноз по символу или None, если данных недостаточно."""
    if symbol not in HISTORY or len(HISTORY[symbol]) < CFG["features"]["min_data_points"]:
        return None

    series = [p for _, p in HISTORY[symbol]]
    ema_fast = ema(series, CFG["signals"]["trend_ema_fast"])
    ema_slow = ema(series, CFG["signals"]["trend_ema_slow"])
    rsi_v = rsi(series, CFG["features"]["rsi_period"])
    vol = volatility(series, CFG["features"]["volatility_window"])
    mom = momentum(series, 10)

    # направление
    direction = "neutral"
    if not math.isnan(ema_fast) and not math.isnan(ema_slow):
        if ema_fast > ema_slow * (1 + CFG["signals"]["momentum_threshold"]):
            direction = "bullish"
        elif ema_fast < ema_slow * (1 - CFG["signals"]["momentum_threshold"]):
            direction = "bearish"

    # консенсус по источникам (реальные биржи)
    exgs = {t.exchange for t in tickers if t.symbol == symbol}
    total = max(1, len(exgs))  # защита от деления на ноль

    # доверие
    parts = []
    parts.append(min(1.0, len(HISTORY[symbol]) / CFG["features"]["history_limit"]))
    if not math.isnan(vol):
        parts.append(max(0.1, 1.0 - (vol / 20.0)))
    parts.append(min(1.0, total / max(1, len(CFG["markets"]["exchanges"]))))
    confidence = float(np.mean(parts)) if parts else 0.0

    return ForecastResult(
        symbol=symbol,
        direction=direction,
        confidence=round(confidence, 3),
        ema_fast=round(ema_fast, 6) if not math.isnan(ema_fast) else float("nan"),
        ema_slow=round(ema_slow, 6) if not math.isnan(ema_slow) else float("nan"),
        rsi=round(rsi_v, 3) if not math.isnan(rsi_v) else float("nan"),
        volatility=round(vol, 3) if not math.isnan(vol) else float("nan"),
        momentum=round(mom, 6) if not math.isnan(mom) else float("nan"),
        timestamp=ts_iso(),
    )


def find_arbitrage(
    symbol: str, tickers: List[Ticker], min_bps: Optional[float] = None
) -> List[ArbitrageDeal]:
    """Ищем связки buy/sell по ask/bid с учётом комиссий (bps)."""
    fees = CFG["arbitrage"]["fees_bps_per_exchange"]
    min_bps = float(min_bps) if min_bps is not None else float(CFG["arbitrage"]["min_spread_bps"])

    by_exg = {t.exchange: t for t in tickers if t.symbol == symbol}
    exgs = list(by_exg.keys())
    deals: List[ArbitrageDeal] = []

    for i in range(len(exgs)):
        for j in range(len(exgs)):
            if i == j:
                continue
            b_exg = exgs[i]  # buy на ask
            s_exg = exgs[j]  # sell по bid
            buy_t = by_exg[b_exg]
            sell_t = by_exg[s_exg]

            buy_ask = buy_t.ask
            sell_bid = sell_t.bid

            if buy_ask <= 0 or sell_bid <= 0:
                continue

            gross_bps = (sell_bid - buy_ask) / buy_ask * 10000.0
            net_bps = gross_bps - float(fees.get(b_exg, 0.0)) - float(fees.get(s_exg, 0.0))
            if net_bps >= min_bps:
                notional = float(CFG["arbitrage"]["min_notional"])
                profit_abs = notional * (net_bps / 10000.0)
                deals.append(
                    ArbitrageDeal(
                        symbol=symbol,
                        buy_exchange=b_exg,
                        sell_exchange=s_exg,
                        buy_ask=round(buy_ask, 6),
                        sell_bid=round(sell_bid, 6),
                        gross_spread_bps=round(gross_bps, 3),
                        net_spread_bps=round(net_bps, 3),
                        notional=notional,
                        profit_abs=round(profit_abs, 6),
                        timestamp=ts_iso(),
                    )
                )

    # Сортировка по чистому спрэду
    deals.sort(key=lambda d: d.net_spread_bps, reverse=True)
    return deals


# ------------
# FastAPI app
# ------------

app = FastAPI(title="Forecast Engine", version="1.0")

if CFG.get("misc", {}).get("allow_cors", True):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.get("/health", response_model=Health)
async def health() -> Health:
    sym_points = {s: len(HISTORY[s]) for s in HISTORY}
    age = None
    if LAST_PULL_TS:
        age = max(0.0, now_ts() - LAST_PULL_TS)
    return Health(
        ok=age is not None and age <= CFG["health"]["max_delay_sec"] + 2,
        data_age_sec=age,
        symbols=sorted(list(HISTORY.keys())),
        exchanges_seen=len(CFG["markets"]["exchanges"]),
        points_in_history=sym_points,
        timestamp=ts_iso(),
    )


@app.get("/forecast")
async def forecast(symbol: str = Query("BTCUSDT")):
    symbol = symbol.upper()

    # тянем свежие котировки и обновляем историю
    tickers = await pull_prices()
    update_history(tickers)

    if symbol not in {t.symbol for t in tickers}:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}")

    fc = compute_forecast(symbol, tickers)
    if not fc:
        return {
            "symbol": symbol,
            "ready": False,
            "reason": f"Not enough data (< {CFG['features']['min_data_points']})",
            "timestamp": ts_iso(),
        }
    return asdict(fc)


@app.get("/arbitrage")
async def arbitrage(
    symbol: str = Query("BTCUSDT"),
    min_bps: Optional[float] = Query(None, description="Override min spread threshold (bps)"),
):
    symbol = symbol.upper()
    tickers = await pull_prices()
    update_history(tickers)

    deals = find_arbitrage(symbol, tickers, min_bps=min_bps)
    return {
        "symbol": symbol,
        "count": len(deals),
        "deals": [asdict(d) for d in deals],
        "timestamp": ts_iso(),
    }
