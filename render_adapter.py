# render_adapter.py
from __future__ import annotations
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import os, hmac, hashlib, json
from fastapi import APIRouter, HTTPException, Header, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["observe/graph"])

# ── простое in-memory хранилище (достаточно для старта) ─────────────────────────
JOBS: Dict[str, Dict[str, Any]] = {}      # job_id -> {"nodes":[], "edges":[], "metrics":{}}
TRENDS: Dict[str, Any] = {                # общая теплокарта (можно сделать per-job)
    "axes": {"x": ["T-3h","T-2h","T-1h","T"], "y": ["DQI","Risk","Reward"]},
    "matrix": [[0.92,0.93,0.94,0.94],[0.23,0.22,0.21,0.20],[0.45,0.47,0.48,0.49]],
    "palette": ["green","yellow","red"],
    "legend": {"min": 0.0, "max": 1.0}
}
WS_CLIENTS: set[WebSocket] = set()

# ── HMAC-подпись (опционально, включается через env) ────────────────────────────
API_SECRET = os.getenv("RENDER_ADAPTER_SECRET", "").encode("utf-8")
def verify_signature(x_kid: Optional[str], x_ts: Optional[str], x_sig: Optional[str], body: bytes):
    if not API_SECRET:  # секрет не задан — пропускаем проверку
        return
    if not (x_kid and x_ts and x_sig):
        raise HTTPException(401, "missing auth headers")
    try:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if abs(now_ms - int(x_ts)) > 60_000:
            raise HTTPException(401, "timestamp skew > 60s")
        mac = hmac.new(API_SECRET, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(mac, x_sig):
            raise HTTPException(401, "bad signature")
    except ValueError:
        raise HTTPException(401, "bad timestamp")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ── REST ─────────────────────────────────────────────────────────────────────────
@router.post("/observe/graph/ingest")
async def ingest_graph(
    payload: Dict[str, Any],
    x_kid: Optional[str] = Header(None), x_ts: Optional[str] = Header(None), x_signature: Optional[str] = Header(None)
):
    """
    Принимает события и собирает узлы/рёбра/метрики.
    payload (минимум): {job_id, decision_links[], arena_events[], mind_reflect[]}
    """
    verify_signature(x_kid, x_ts, x_signature, json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    job_id = str(payload.get("job_id", "default"))
    dl: List[Dict[str, Any]] = payload.get("decision_links", []) or []
    arena: List[Dict[str, Any]] = payload.get("arena_events", []) or []
    reflect: List[Dict[str, Any]] = payload.get("mind_reflect", []) or []

    nodes, edges = {}, []

    def add_node(_id: str, label: str, typ: str, score: float = 0.0, tags: Optional[List[str]] = None):
        if _id not in nodes:
            nodes[_id] = {"id": _id, "label": label, "type": typ, "score": round(score, 3), "tags": tags or []}

    # decision_links → узлы policy/signal/action и связи
    for d in dl:
        src, dst = d.get("from"), d.get("to")
        kind = d.get("kind", "influences")
        w = float(d.get("weight", 0.5))
        if not src or not dst:  # пропустим битые связи
            continue
        add_node(src, src.split(":")[-1].title(), src.split(":")[0])
        add_node(dst, dst.split(":")[-1].title(), dst.split(":")[0])
        edges.append({"source": src, "target": dst, "kind": kind, "weight": round(w, 3), "ts": _now_iso()})

    # arena_events → узлы policy/outcome, метрики
    finops = 0.0
    dqi_list, risk_list = [], []
    for a in arena:
        pol = a.get("policy", "policy:unknown")
        add_node(pol, pol.split(":")[-1].title(), "policy", score=float(a.get("outcome", 0.0)))
        finops += float(a.get("finops", 0.0))
        if "dqi" in a: dqi_list.append(float(a["dqi"]))
        if "risk" in a: risk_list.append(float(a["risk"]))

    # mind_reflect → узлы reflect
    for r in reflect:
        rid = f"reflect:{r.get('id','n/a')}"
        add_node(rid, "Reflect", "reflect", tags=r.get("tags", []))

    metrics = {
        "dqi_avg": round(sum(dqi_list) / len(dqi_list), 3) if dqi_list else 0.0,
        "risk_cvar": round(sum(risk_list) / len(risk_list), 3) if risk_list else 0.0,
        "finops_cost_usd": round(finops, 6),
        "updated_at": _now_iso(),
    }

    JOBS[job_id] = {"nodes": list(nodes.values()), "edges": edges, "metrics": metrics}

    # пуш в WebSocket-клиенты
    if WS_CLIENTS:
        msg = {"type": "graph_update", "job_id": job_id, "graph": JOBS[job_id]}
        dead = set()
        for ws in list(WS_CLIENTS):
            try:
                await ws.send_text(json.dumps(msg, ensure_ascii=False))
            except Exception:
                dead.add(ws)
        for ws in dead:
            WS_CLIENTS.discard(ws)

    return {"status": "ok", "job_id": job_id, "nodes": len(JOBS[job_id]["nodes"]), "edges": len(edges)}

@router.get("/observe/graph/{job_id}")
async def get_graph(job_id: str):
    return JOBS.get(job_id) or JOBS.get("default") or {
        "nodes": [], "edges": [],
        "metrics": {"dqi_avg": 0.0, "risk_cvar": 0.0, "finops_cost_usd": 0.0, "updated_at": _now_iso()}
    }

@router.get("/observe/graph/trends")
async def get_trends(window: str = "24h"):
    return {"window": window, **TRENDS}

# ── WebSocket (real-time) ───────────────────────────────────────────────────────
@router.websocket("/observe/graph/ws")
async def graph_ws(ws: WebSocket):
    await ws.accept()
    WS_CLIENTS.add(ws)
    try:
        await ws.send_text(json.dumps({"type": "hello", "ts": _now_iso()}, ensure_ascii=False))
        while True:
            # держим соединение живым; входящих сообщений не ждём
            await ws.receive_text()
    except WebSocketDisconnect:
        WS_CLIENTS.discard(ws)
    except Exception:
        WS_CLIENTS.discard(ws)
