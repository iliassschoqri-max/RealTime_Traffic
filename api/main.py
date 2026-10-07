"""
KPI API — FastAPI + WebSocket
Architecture Cache-First : background worker refresh Trino toutes les 5s,
STATE_CACHE global, endpoints O(1), WebSocket push INIT_DATA + UPDATE_DATA.
"""

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

import sys
import json
import asyncio
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "dashboard"))
from core.trino_client import TrinoClient, logger

from api.reporting import generate_csv_bytes, generate_pdf_bytes

app = FastAPI(title="PFA Traffic KPI API", version="2.1.0")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

client = TrinoClient()
STATIC_DIR = Path(__file__).parent / "static"

# ─────────────────────────────────────────────
# 1. STATE_CACHE global + valeurs par défaut
# ─────────────────────────────────────────────

_EMPTY_KPI = {
    "total_events": 0,
    "avg_speed": 0,
    "congestion_pct": 0,
    "incident_pct": 0,
    "active_segments": 0,
    "unique_vehicles": 0,
}

STATE_CACHE = {
    "kpi": dict(_EMPTY_KPI),
    "events": [],
    "geo": {"features": []},
    "segments": {"segments": []},
    "timeline": {"timeline": []},
    "live_metrics": dict(_EMPTY_KPI),
    "last_refresh": 0,
}

_cache_ready = False

# ─────────────────────────────────────────────
# 2. Connection manager WebSocket
# ─────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)

    async def broadcast(self, data: dict):
        msg = json.dumps(data, default=str)
        stale = []
        for ws in self.connections:
            try:
                await ws.send_text(msg)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self.disconnect(ws)

manager = ConnectionManager()

# ─────────────────────────────────────────────
# 3. Helpers Trino asynchrones
# ─────────────────────────────────────────────

async def _query_json(sql: str):
    try:
        df = await asyncio.to_thread(client.query, sql)
        if df is None or df.empty:
            return []
        return json.loads(df.to_json(orient="records", date_format="iso"))
    except Exception:
        return []


async def _query_scalar(sql: str):
    try:
        df = await asyncio.to_thread(client.query, sql)
        if df is None or df.empty:
            return None
        row = df.iloc[0]
        return {col: (int(val) if isinstance(val, (int, float)) else val) for col, val in row.items()}
    except Exception:
        return None


async def _load_dim_cache():
    cache = {}
    try:
        df = await asyncio.to_thread(
            client.query,
            "SELECT DISTINCT segment_key, segment_name, avg_latitude, avg_longitude FROM delta.gold.dim_road_segment"
        )
        if df is not None and not df.empty:
            cache["segments"] = df.drop_duplicates("segment_key").set_index("segment_key").to_dict(orient="index")
        df2 = await asyncio.to_thread(
            client.query,
            "SELECT DISTINCT event_type_key, event_type_name, severity_color FROM delta.gold.dim_event_type"
        )
        if df2 is not None and not df2.empty:
            cache["event_types"] = df2.drop_duplicates("event_type_key").set_index("event_type_key").to_dict(orient="index")
    except Exception as e:
        logger.error(f"dim_cache load: {e}")
    return cache


def _enrich_events(events, sm, em):
    if not events:
        return events
    for e in events:
        sk = e.get("segment_key")
        ek = e.get("event_type_key")
        if sk and sk in sm:
            e["segment_name"] = sm[sk].get("segment_name")
        if ek and ek in em:
            e["event_type_name"] = em[ek].get("event_type_name")
            e["severity_color"] = em[ek].get("severity_color")
    return events


def _enrich_geo(geo, sm):
    if not geo:
        return geo
    for g in geo:
        sk = g.get("segment_key")
        if sk and sk in sm:
            g["segment_name"] = sm[sk].get("segment_name")
            g["latitude"] = sm[sk].get("avg_latitude")
            g["longitude"] = sm[sk].get("avg_longitude")
    return geo


def _enrich_segments(segments, sm):
    if not segments:
        return segments
    for s in segments:
        sk = s.get("segment_key")
        if sk and sk in sm:
            s["segment_name"] = sm[sk].get("segment_name")
    return segments


# ─────────────────────────────────────────────
# 4. Fonction de refresh Trino unique
# ─────────────────────────────────────────────

async def _run_trino_refresh(dim_cache: dict | None = None) -> dict:
    if dim_cache is None:
        dim_cache = {}
    if not dim_cache:
        dim_cache = await _load_dim_cache()
    sm = dim_cache.get("segments", {})
    em = dim_cache.get("event_types", {})

    kpi_sql = """
        SELECT COUNT(*) AS total_events,
            ROUND(AVG(speed_kmh), 1) AS avg_speed,
            ROUND(AVG(CASE WHEN is_congestion = 1 THEN 1 ELSE 0 END) * 100, 1) AS congestion_pct,
            ROUND(AVG(CASE WHEN is_incident = 1 THEN 1 ELSE 0 END) * 100, 1) AS incident_pct,
            COUNT(DISTINCT segment_key) AS active_segments,
            COUNT(DISTINCT vehicle_id) AS unique_vehicles
        FROM delta.gold.fact_traffic_events
    """
    events_sql = """
        SELECT event_timestamp, speed_kmh, is_congestion, is_incident,
            speed_category, segment_key, event_type_key, latitude, longitude
        FROM delta.gold.fact_traffic_events
        ORDER BY event_timestamp DESC LIMIT 25
    """
    geo_sql = """
        SELECT f.segment_key,
            ROUND(AVG(f.speed_kmh), 1) AS avg_speed,
            ROUND(AVG(f.is_congestion), 2) AS congestion_rate,
            COUNT(*) AS event_count
        FROM delta.gold.fact_traffic_events f
        GROUP BY f.segment_key
    """
    segments_sql = """
        SELECT segment_key, COUNT(*) AS event_count,
            ROUND(AVG(speed_kmh), 1) AS avg_speed,
            ROUND(AVG(is_congestion), 2) AS congestion_rate,
            ROUND(AVG(is_incident), 2) AS incident_rate
        FROM delta.gold.fact_traffic_events
        GROUP BY segment_key ORDER BY event_count DESC
    """
    timeline_sql = """
        SELECT CONCAT(CAST(t.year AS VARCHAR), '-', LPAD(CAST(t.month AS VARCHAR), 2, '0'), '-',
            LPAD(CAST(t.day AS VARCHAR), 2, '0'), ' ', LPAD(CAST(t.hour AS VARCHAR), 2, '0'), ':00') AS period,
            COUNT(*) AS event_count, ROUND(AVG(f.speed_kmh), 1) AS avg_speed,
            ROUND(AVG(f.is_congestion), 2) AS congestion_rate
        FROM delta.gold.fact_traffic_events f
        JOIN delta.gold.dim_time t ON f.time_key = t.time_key
        GROUP BY t.year, t.month, t.day, t.hour
        ORDER BY t.year, t.month, t.day, t.hour DESC LIMIT 24
    """

    kpi, events, geo, segments, timeline = await asyncio.gather(
        _query_scalar(kpi_sql),
        _query_json(events_sql),
        _query_json(geo_sql),
        _query_json(segments_sql),
        _query_json(timeline_sql),
    )

    if kpi is None:
        kpi = dict(_EMPTY_KPI)

    events = _enrich_events(events if events else [], sm, em)
    geo = _enrich_geo(geo if geo else [], sm)
    segments = _enrich_segments(segments if segments else [], sm)

    return {
        "kpi": kpi,
        "events": events,
        "geo": {"features": geo},
        "segments": {"segments": segments},
        "timeline": {"timeline": timeline if timeline else []},
        "live_metrics": kpi,
        "dim_cache": dim_cache,
    }


# ─────────────────────────────────────────────
# 5. Background worker Trino (toutes les 5s)
# ─────────────────────────────────────────────

async def trino_collector_loop():
    dim_cache = {}
    while True:
        t0 = time.time()
        try:
            snapshot = await _run_trino_refresh(dim_cache)
            dim_cache = snapshot.pop("dim_cache", dim_cache)

            STATE_CACHE["kpi"] = snapshot["kpi"]
            STATE_CACHE["events"] = snapshot["events"]
            STATE_CACHE["geo"] = snapshot["geo"]
            STATE_CACHE["segments"] = snapshot["segments"]
            STATE_CACHE["timeline"] = snapshot["timeline"]
            STATE_CACHE["live_metrics"] = snapshot["live_metrics"]
            STATE_CACHE["last_refresh"] = time.time()

            total_ms = int((time.time() - t0) * 1000)
            logger.info(f"[CACHE] Trino refresh OK — {snapshot['kpi'].get('total_events', 0)} events, {len(snapshot['events'])} recent, {total_ms}ms")

            await manager.broadcast({
                "type": "UPDATE_DATA",
                "kpi": snapshot["kpi"],
                "events": snapshot["events"],
                "geo": snapshot["geo"],
                "segments": snapshot["segments"],
                "timeline": snapshot["timeline"],
                "live_metrics": snapshot["live_metrics"],
            })
        except Exception as e:
            logger.error(f"[CACHE] Trino refresh error: {e}")
        await asyncio.sleep(5)


# ─────────────────────────────────────────────
# 6. Startup — premier refresh SYNCHRONE
# ─────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logger.info("[API] First Trino refresh — blocking startup until cache is populated...")
    try:
        snapshot = await _run_trino_refresh()
        STATE_CACHE["kpi"] = snapshot["kpi"]
        STATE_CACHE["events"] = snapshot["events"]
        STATE_CACHE["geo"] = snapshot["geo"]
        STATE_CACHE["segments"] = snapshot["segments"]
        STATE_CACHE["timeline"] = snapshot["timeline"]
        STATE_CACHE["live_metrics"] = snapshot["live_metrics"]
        STATE_CACHE["last_refresh"] = time.time()
        logger.info(f"[API] First refresh done — {snapshot['kpi'].get('total_events', 0)} events loaded")
    except Exception as e:
        logger.error(f"[API] First Trino refresh failed (cache will use default zeros): {e}")

    global _cache_ready
    _cache_ready = True

    asyncio.create_task(trino_collector_loop())
    logger.info("[API] Background trino_collector_loop started (every 5s)")

# ─────────────────────────────────────────────
# 7. WebSocket
# ─────────────────────────────────────────────

def _build_ws_payload() -> dict:
    return {
        "type": "INIT_DATA",
        "kpi": STATE_CACHE["kpi"],
        "events": STATE_CACHE["events"],
        "geo": STATE_CACHE["geo"],
        "segments": STATE_CACHE["segments"],
        "timeline": STATE_CACHE["timeline"],
        "live_metrics": STATE_CACHE["live_metrics"],
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        await ws.send_text(json.dumps(_build_ws_payload(), default=str))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)

# ─────────────────────────────────────────────
# 8. Routes static + health
# ─────────────────────────────────────────────

@app.get("/")
def dashboard():
    return FileResponse(STATIC_DIR / "index.html")

@app.get("/api/health")
def health():
    return {"status": "ok", "version": "2.1.0", "cache_mode": "background_refresh", "cache_ready": _cache_ready}

# ─────────────────────────────────────────────
# 9. Endpoints O(1) — lisent STATE_CACHE uniquement
# ─────────────────────────────────────────────

@app.get("/api/kpi/reset-cache")
def reset_cache():
    STATE_CACHE["kpi"] = dict(_EMPTY_KPI)
    STATE_CACHE["events"] = []
    STATE_CACHE["geo"] = {"features": []}
    STATE_CACHE["segments"] = {"segments": []}
    STATE_CACHE["timeline"] = {"timeline": []}
    STATE_CACHE["live_metrics"] = dict(_EMPTY_KPI)
    STATE_CACHE["last_refresh"] = 0
    return {"status": "ok", "message": "STATE_CACHE reinitialise"}

@app.get("/api/kpi/summary")
def kpi_summary():
    return STATE_CACHE["kpi"]

@app.get("/api/kpi/realtime")
def kpi_realtime(limit: int = Query(50, ge=1, le=200)):
    events = STATE_CACHE["events"]
    return {"events": events[:limit]}

@app.get("/api/kpi/geo")
def kpi_geo():
    return STATE_CACHE["geo"]

@app.get("/api/kpi/segments")
def kpi_segments():
    return STATE_CACHE["segments"]

@app.get("/api/kpi/timeline")
def kpi_timeline(
    period: str = Query("hour", pattern="^(hour|day|week)$"),
    limit: int = Query(24, ge=1, le=168),
):
    return STATE_CACHE["timeline"]

@app.get("/api/traffic/live-metrics")
def live_metrics():
    return STATE_CACHE["live_metrics"]

# ─────────────────────────────────────────────
# 10. Rapports
# ─────────────────────────────────────────────

@app.get("/api/report/csv")
def report_csv(
    type: str = Query("summary", description="summary / segments / anomalies / timeline"),
):
    data, filename = generate_csv_bytes(type)
    return StreamingResponse(
        iter([data]),
        media_type="text/csv; charset=utf-8-sig",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.get("/api/report/pdf")
def report_pdf(
    type: str = Query("complet", description="complet / summary"),
):
    data, filename = generate_pdf_bytes(type)
    return StreamingResponse(
        iter([data]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
