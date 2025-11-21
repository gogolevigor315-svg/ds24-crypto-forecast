# ds24-proxy-gateway (Render-ready version)
# FIXED: SELF_BASE issues, ingest URL autodetect, feeder stability, 404 elimination

import os, time, hmac, hashlib, json, asyncio
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx

APP_NAME = "ds24-proxy-gateway"

# ─────────────────────────────────────────────────────────────
#  PUBLIC BASE URL (AUTO-FIXED FOR RENDER)
# ─────────────────────────────────────────────────────────────
PUBLIC_BASE = "https://ds24-proxy-gateway.onrender.com"

# ─────────────────────────────────────────────────────────────

UPSTREAM_LIVE  = os.getenv("UPSTREAM_LIVE",  "http://127.0.0.1:8000/api/live")
UPSTREAM_GRAPH = os.getenv("UPSTREAM_GRAPH", "http://127.0.0.1:8000/observe/graph")
CACHE_TTL      = int(os.getenv("CACHE_TTL", "10"))

INGEST_KID    = os.getenv("INGEST_KID", "demo")
INGEST_SECRET = os.getenv("INGEST_SECRET", "")

FEEDER_ENABLED      = os.getenv("FEEDER_ENABLED", "true").lower() in ("1","true","yes","on")
FEEDER_SYMBOL       = os.getenv("FEEDER_SYMBOL", "BTCUSDT")
FEEDER_TF           = os.getenv("FEEDER_TF", "1m")
FEEDER_JOB_ID       = os.getenv("FEEDER_JOB_ID", "portfolio-live")
FEEDER_INTERVAL_SEC = int(os.getenv("FEEDER_INTERVAL_SEC", "15"))

BYBIT_MARKET_URL = os.getenv(
    "BYBIT_MARKET_URL",
    "https://api.bybit.com/v5/market/tickers?category=spot"
)

TOP10 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT"
]

# ─────────────────────────────────────────────────────────────
# LOCAL GRAPH STORAGE
# ─────────────────────────────────────────────────────────────

_local_graph: Dict[str, Dict[str, Any]] = {}

def _graph_get(job_id: str) -> Dict[str, Any]:
    return _local_graph.get(job_id, {
        "nodes": [],
        "edges": [],
        "metrics": {
            "dqi_avg": None,
            "risk_cvar": None,
            "finops_cost_usd": None,
            "updated_at": None
        }
    })

def _graph_put(job_id: str, payload: Dict[str, Any]):
    g = _graph_get(job_id)
    for k in ("decision_links","arena_events","mind_reflect"):
        if k in payload and isinstance(payload[k], list):
            if k == "decision_links":
                for e in payload[k]:
                    src = e.get("from")
                    tgt = e.get("to")

                    if src and not any(n["id"] == src for n in g["nodes"]):
                        g["nodes"].append({
                            "id": src,
                            "label": src.split(":")[-1],
                            "type": "signal",
                            "score": None,
                            "tags": []
                        })

                    if tgt and not any(n["id"] == tgt for n in g["nodes"]):
                        g["nodes"].append({
                            "id": tgt,
                            "label": tgt.split(":")[-1],
                            "type": "signal",
                            "score": None,
                            "tags": []
                        })

                    g["edges"].append({
                        "source": src,
                        "target": tgt,
                        "kind": e.get("kind","signal"),
                        "weight": e.get("weight", 0.5),
                        "ts": e.get("ts")
                    })

    g["metrics"]["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _local_graph[job_id] = g

# ─────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────
# HTTP JSON CLIENT (SAFE)
# ─────────────────────────────────────────────────────────────
async def _get_json(url: str, params: Optional[dict] = None) -> Tuple[bool, Any, int]:
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get(url, params=params)
            ct = (r.headers.get("content-type") or "").lower()

            if r.status_code >= 400:
                body = r.text if "application/json" not in ct else r.json()
                return False, {"upstream_status": r.status_code, "upstream_body": body}, r.status_code

            if "application/json" in ct:
                return True, r.json(), r.status_code

            return True, {"raw": r.text}, r.status_code

    except httpx.HTTPError as e:
        return False, {"upstream_error": str(e), "url": url, "params": params}, 502

# ─────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "ts": int(time.time()),
        "feeder": {
            "enabled": FEEDER_ENABLED,
            "job_id": FEEDER_JOB_ID,
            "interval_sec": FEEDER_INTERVAL_SEC
        },
        "graph_jobs": list(_local_graph.keys())
    }

@app.get("/live")
async def live(symbol: str = Query(...), tf: str = Query("1m")):
    key = f"live:{symbol}:{tf}"
    cached = _cache_get(key)
    if cached:
        return {"cached": True, **cached}

    ok, data, status = await _get_json(UPSTREAM_LIVE, params={"symbol": symbol, "tf": tf})
    if not ok:
        return JSONResponse(status_code=status, content={"proxy": APP_NAME, "error": data})

    event = {
        "ts": data.get("ts"),
        "symbol": data.get("symbol", symbol),
        "tf": data.get("tf", tf),
        "price": data.get("price"),
        "features": data.get("features", {}),
        "forecast": data.get("forecast", {}),
        "source": data.get("source", "ds24-crypto-forecast"),
        "q_score": data.get("q_score"),
    }

    _cache_put(key, event)
    return {"cached": False, **event}

@app.get("/graph/{job_id}")
async def graph(job_id: str):
    url = f"{UPSTREAM_GRAPH.rstrip('/')}/{job_id}"
    ok, data, status = await _get_json(url)

    if ok:
        return {"cached": False, **data}

    return {"cached": True, **_graph_get(job_id), "fallback": "local"}

# ─────────────────────────────────────────────────────────────
# INGEST (WITH FALLBACK)
# ─────────────────────────────────────────────────────────────
@app.post("/ingest-pass")
async def ingest_pass(payload: dict):
    url = f"{UPSTREAM_GRAPH.rstrip('/')}/ingest"
    headers = {"Content-Type": "application/json"}
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    if INGEST_SECRET:
        sig = hmac.new(INGEST_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
        headers.update({
            "x-kid": INGEST_KID,
            "x-ts": str(int(time.time() * 1000)),
            "x-signature": sig
        })

    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.post(url, headers=headers, content=body)
            if r.status_code < 400:
                try:
                    return r.json()
                except:
                    return {"raw": r.text}
    except:
        pass

    try:
        payload = json.loads(body.decode("utf-8"))
    except:
        payload = {"job_id": "unknown"}

    _graph_put(payload.get("job_id", "default"), payload)
    return {"status": "ok", "fallback": "local"}

# ─────────────────────────────────────────────────────────────
# BYBIT TOP10
# ─────────────────────────────────────────────────────────────
@app.get("/trade/top10")
async def trade_top10():
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(BYBIT_MARKET_URL)
        data = r.json()

    if "result" not in data or "list" not in data["result"]:
        return {"error": "invalid response from Bybit", "raw": data}

    tickers = data["result"]["list"]
    top = [t for t in tickers if t.get("symbol") in TOP10]
    top_sorted = sorted(top, key=lambda x: TOP10.index(x["symbol"]))

    cleaned = [
        {
            "symbol": t.get("symbol"),
            "price": t.get("lastPrice"),
            "change24h": t.get("price24hPcnt"),
            "volume": t.get("turnover24h"),
            "high": t.get("highPrice24h"),
            "low": t.get("lowPrice24h"),
            "bid1": t.get("bid1Price"),
            "ask1": t.get("ask1Price"),
        }
        for t in top_sorted
    ]

    return {"top10": cleaned}

# ─────────────────────────────────────────────────────────────
# FEEDER (FIXED)
# ─────────────────────────────────────────────────────────────

async def feeder_send(payload: dict):
    """Always send to PUBLIC BASE URL on Render."""
    url = f"{PUBLIC_BASE}/ingest-pass"

    async with httpx.AsyncClient(timeout=6.0) as c:
        try:
            await c.post(url, json=payload)
        except Exception as e:
            print("Feeder send error:", e)

async def _feeder_loop():
    if not FEEDER_ENABLED:
        return

    while True:
        try:
            async with httpx.AsyncClient(timeout=6.0) as c:
                r = await c.get(UPSTREAM_LIVE, params={"symbol": FEEDER_SYMBOL, "tf": FEEDER_TF})
                evt = r.json() if "application/json" in (r.headers.get("content-type", "").lower()) else {}

                feat = evt.get("features", {}) if isinstance(evt, dict) else {}
                rsi = float(feat.get("rsi", 0.5) or 0.5)
                vol = float(feat.get("vol", 0.5) or 0.5)

                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                payload = {
                    "job_id": FEEDER_JOB_ID,
                    "decision_links": [
                        {"from": "source:live", "to": "signal:rsi", "kind": "emits",
                         "weight": round(min(max(rsi / 100.0, 0.0), 1.0), 3), "ts": ts},
                        {"from": "source:live", "to": "signal:vol", "kind": "emits",
                         "weight": round(min(max(vol, 0.0), 1.0), 3), "ts": ts},
                        {"from": "signal:rsi", "to": "policy:finops_guard", "kind": "influences",
                         "weight": 0.6, "ts": ts},
                        {"from": "signal:vol", "to": "risk:cvar", "kind": "influences",
                         "weight": 0.7, "ts": ts}
                    ],
                    "arena_events": [
                        {"id": f"tick-{ts}", "policy": "policy:finops_guard",
                         "outcome": 0.001, "finops": 0.017, "dqi": 0.90, "ts": ts}
                    ],
                    "mind_reflect": [
                        {"id": f"note-{ts}", "text": "live→graph via feeder",
                         "tags": ["reflect", "live"], "ts": ts}
                    ]
                }

                await feeder_send(payload)

        except Exception as e:
            print("Feeder loop error:", e)

        await asyncio.sleep(FEEDER_INTERVAL_SEC)

# ─────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────
@app.on_event("startup")
async def _on_startup():
    app.state.feeder_task = asyncio.create_task(_feeder_loop())

@app.on_event("shutdown")
async def _on_shutdown():
    t = getattr(app.state, "feeder_task", None)
    if t:
        t.cancel()
