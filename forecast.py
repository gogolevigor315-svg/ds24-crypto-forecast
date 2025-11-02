# forecast.py
# Minimal, self-contained forecast service for DS24
# - /health: health info
# - /api/live: SpineEventCryptoV1 (with degraded fallback)
# - /v1/forecast: compatibility -> proxies to /api/live
#
# Usage:
#   - Optional env: COLLECTOR_URL (e.g. http://localhost:9000/collector)
#   - Deploy to Render (or run uvicorn forecast:app --host 0.0.0.0 --port $PORT)
#
# Requirements (add to requirements.txt):
# fastapi, uvicorn, httpx

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any
import os, time, random, json
from datetime import datetime, timezone
import httpx

app = FastAPI(title="ds24-crypto-forecast (with degraded fallback)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Config
COLLECTOR_URL = os.getenv("COLLECTOR_URL", "").strip()  # optional, e.g. "http://collector:8000/api/tickers"
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_TF = "1m"
SERVICE_NAME = "ds24-crypto-forecast"

# Simple in-memory minimal history (optional, not persisted)
_history = {}

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def now_ts_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

# ---------------------------
# Degraded fallback generator
# ---------------------------
def _degraded_event(symbol: str, tf: str = DEFAULT_TF) -> Dict[str, Any]:
    """Return a minimal valid SpineEventCryptoV1 when real collector is unavailable."""
    # deterministic-seeming price (for simpler testing) but use small randomness
    base = 30000 if symbol.upper().startswith("BTC") else 2000
    price = round(base + random.random() * (base * 0.05), 2)
    rsi = round(50 + random.uniform(-6, 6), 2)
    vol = round(random.uniform(0.15, 0.6), 3)
    momentum = round(random.uniform(-0.02, 0.02), 4)
    event = {
        "ts": now_ts_ms(),
        "symbol": symbol.upper(),
        "tf": tf,
        "price": price,
        "features": {
            "rsi": rsi,
            "vol": vol,
            "momentum": momentum,
            # include placeholder EMAs to satisfy clients that expect them
            "ema_fast": None,
            "ema_slow": None
        },
        "forecast": {
            "mean": round(random.uniform(-0.005, 0.005), 4),
            "cvar95": None,
            "conf": 0.0
        },
        "source": f"{SERVICE_NAME}(degraded)",
        "q_score": 0.5
    }
    # update local history lightly so health shows something non-empty
    _history.setdefault(symbol.upper(), []).append({"ts": event["ts"], "price": price})
    return event

# ---------------------------
# Collector access wrapper
# ---------------------------
async def _fetch_from_collector(symbol: str, tf: str) -> Dict[str, Any]:
    """
    If COLLECTOR_URL is configured, attempt to fetch.
    Expected collector: returns JSON similar to SpineEventCryptoV1 or a minimal structure we can translate.
    If anything fails, raise Exception to trigger degraded fallback.
    """
    if not COLLECTOR_URL:
        raise RuntimeError("Collector URL not configured")

    # build request: allow collector to accept params ?symbol=...&tf=...
    params = {"symbol": symbol, "tf": tf}
    timeout = float(os.getenv("COLLECTOR_TIMEOUT", "5.0"))
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(COLLECTOR_URL, params=params)
        # treat non-2xx as failure
        if resp.status_code != 200:
            raise RuntimeError(f"Collector returned status {resp.status_code}: {resp.text}")
        # parse JSON
        data = resp.json()
        # Try to normalize if collector gives a compatible structure
        # If already SpineEventCryptoV1-like, accept.
        # Minimal required fields: ts, symbol, price
        if isinstance(data, dict) and data.get("ts") and data.get("price"):
            return data
        # If collector returns list/other, try to adapt
        # If adaptation fails, raise
        raise RuntimeError("Collector returned unexpected payload")

# ---------------------------
# Health endpoint
# ---------------------------
@app.get("/health")
async def health():
    """
    Returns health summary. Does not hide collector unavailability.
    """
    symbols = list(_history.keys())
    # basic health: ok true only if collector configured and has recent points
    collector_ok = False
    if COLLECTOR_URL:
        # naive check: we attempt a quick GET to collector root (without params) if possible
        try:
            # synchronous attempt inside async function using httpx
            async with httpx.AsyncClient(timeout=2.0) as c:
                r = await c.get(COLLECTOR_URL)
                collector_ok = (r.status_code == 200)
        except Exception:
            collector_ok = False

    return {
        "ok": bool(collector_ok and len(symbols) > 0),
        "data_age_sec": None,
        "symbols": symbols,
        "exchanges_seen": 0,
        "points_in_history": {k: len(v) for k, v in _history.items()},
        "timestamp": now_iso()
    }

# ---------------------------
# Main API: /api/live
# ---------------------------
@app.get("/api/live")
async def api_live(symbol: str = Query(DEFAULT_SYMBOL), tf: str = Query(DEFAULT_TF)):
    """
    Unified endpoint expected by DS24 Proxy and other modules.
    Always returns a valid JSON following SpineEventCryptoV1 contract.
    If collector is available (COLLECTOR_URL), we try to use it; otherwise we return a degraded event.
    """
    symbol_u = (symbol or DEFAULT_SYMBOL).upper()
    tf_u = tf or DEFAULT_TF

    # Try to fetch from collector if configured
    try:
        data = await _fetch_from_collector(symbol_u, tf_u)
        # basic normalization: ensure required keys exist
        if not (isinstance(data, dict) and data.get("ts") and data.get("price")):
            # unexpected format -> fallback
            return _degraded_event(symbol_u, tf_u)

        # update local history for observability
        try:
            _history.setdefault(symbol_u, []).append({"ts": data.get("ts"), "price": data.get("price")})
        except Exception:
            pass

        # Ensure we return predictable keyset (normalize)
        event = {
            "ts": int(data.get("ts")),
            "symbol": data.get("symbol", symbol_u),
            "tf": data.get("tf", tf_u),
            "price": data.get("price"),
            "features": data.get("features", {}),
            "forecast": data.get("forecast", {"mean": 0.0, "cvar95": None, "conf": 0.0}),
            "source": data.get("source", "collector"),
            "q_score": data.get("q_score", 0.9)
        }
        return event

    except Exception:
        # any failure → return degraded event (stable contract)
        return _degraded_event(symbol_u, tf_u)

# ---------------------------
# Compatibility route
# ---------------------------
@app.get("/v1/forecast")
async def v1_forecast(symbol: str = Query(DEFAULT_SYMBOL), tf: str = Query(DEFAULT_TF)):
    """Compatibility route, proxies to /api/live"""
    return await api_live(symbol=symbol, tf=tf)

# ---------------------------
# Optional: root / simple message
# ---------------------------
@app.get("/")
async def root():
    return {"service": SERVICE_NAME, "now": now_iso(), "note": "Use /api/live?symbol=...&tf=... for SpineEvent"}

# If run directly:
#   uvicorn forecast:app --host 0.0.0.0 --port 8000
