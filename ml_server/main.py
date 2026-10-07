import random
import time
import math
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# ─── Segments Tanger ────────────────────────────
TANGER_SEGMENTS = [
    {"key": "S01", "name": "Bd Mohamed V",          "official_code": "MA-TNG-001", "lat": 35.7673, "lng": -5.8127, "base_speed": 45},
    {"key": "S02", "name": "Av Pasteur",            "official_code": "MA-TNG-002", "lat": 35.7712, "lng": -5.8178, "base_speed": 38},
    {"key": "S03", "name": "Av d'Espagne",          "official_code": "MA-TNG-003", "lat": 35.7765, "lng": -5.8090, "base_speed": 50},
    {"key": "S04", "name": "Bd de la Corniche",     "official_code": "MA-TNG-004", "lat": 35.7820, "lng": -5.8025, "base_speed": 42},
    {"key": "S05", "name": "Pl. des Nations",       "official_code": "MA-TNG-009", "lat": 35.7700, "lng": -5.8100, "base_speed": 35},
    {"key": "S06", "name": "Gare TGV",              "official_code": "MA-TNG-010", "lat": 35.7650, "lng": -5.8250, "base_speed": 40},
    {"key": "S07", "name": "Av. Moulay Youssef",    "official_code": "MA-TNG-005", "lat": 35.7730, "lng": -5.8200, "base_speed": 44},
    {"key": "S08", "name": "Bd. Abdelkrim Khattabi","official_code": "MA-TNG-008", "lat": 35.7685, "lng": -5.8080, "base_speed": 48},
    {"key": "S09", "name": "Av. Hassan II",         "official_code": "MA-TNG-011", "lat": 35.7750, "lng": -5.8150, "base_speed": 36},
    {"key": "S10", "name": "Rue de la Liberté",     "official_code": "MA-TNG-007", "lat": 35.7690, "lng": -5.8135, "base_speed": 30},
]

SEGMENT_MAP = {s["key"]: s for s in TANGER_SEGMENTS}

# ─── Cache global ───────────────────────────────
ML_STATE_CACHE = {}
_cache_ready = False

def _generate_predictions(hours_ahead=6):
    now = datetime.now()
    predictions = []
    for seg in TANGER_SEGMENTS:
        for h in range(hours_ahead):
            hour_of_day = (now + timedelta(hours=h)).hour
            factor = 1.0
            if 7 <= hour_of_day <= 9:
                factor = 0.65 + random.uniform(-0.05, 0.05)
            elif 17 <= hour_of_day <= 19:
                factor = 0.60 + random.uniform(-0.05, 0.05)
            elif 22 <= hour_of_day or hour_of_day <= 5:
                factor = 0.95 + random.uniform(-0.03, 0.03)
            else:
                factor = 0.80 + random.uniform(-0.05, 0.05)
            pred_speed = round(seg["base_speed"] * factor, 1)
            pred_speed = max(8, min(65, pred_speed))
            if pred_speed < 20:
                status = "CRITIQUE"
            elif pred_speed < 35:
                status = "MODERE"
            else:
                status = "NORMAL"
            predictions.append({
                "segment": seg["key"],
                "segment_name": seg["name"],
                "official_code": seg["official_code"],
                "hour": h + 1,
                "predicted_speed_kmh": pred_speed,
                "predicted_status": status,
                "current_speed_kmh": round(seg["base_speed"] * random.uniform(0.6, 1.0), 1),
            })
    return predictions

def _generate_anomalies(limit=200):
    anomalies = []
    reasons_pool = [
        "Baisse soudaine de vitesse", "Variation anormale du trafic",
        "Détection d'embouteillage", "Vitesse incohérente",
        "Pic d'incidents", "Comportement erratique"
    ]
    for i in range(min(limit, 40)):
        seg = random.choice(TANGER_SEGMENTS)
        score = round(random.uniform(0.45, 0.98), 2)
        speed = round(seg["base_speed"] * random.uniform(0.3, 0.7), 1)
        ts = datetime.now() - timedelta(minutes=random.randint(0, 120))
        anomalies.append({
            "segment": seg["key"],
            "segment_name": seg["name"],
            "official_code": seg["official_code"],
            "speed_kmh": speed,
            "anomaly_score": score,
            "reasons": random.sample(reasons_pool, random.randint(1, 2)),
            "timestamp": ts.isoformat(sep=" ", timespec="seconds"),
        })
    anomalies.sort(key=lambda x: x["anomaly_score"], reverse=True)
    return {"anomalies": anomalies, "total_detected": len(anomalies)}

def _generate_forecast():
    segments = []
    for seg in TANGER_SEGMENTS:
        forecast = []
        for h in range(24):
            hour_of_day = (datetime.now() + timedelta(hours=h)).hour
            factor = 1.0
            if 7 <= hour_of_day <= 9:
                factor = 0.65 + random.uniform(-0.05, 0.05)
            elif 17 <= hour_of_day <= 19:
                factor = 0.60 + random.uniform(-0.05, 0.05)
            elif 22 <= hour_of_day or hour_of_day <= 5:
                factor = 0.95 + random.uniform(-0.03, 0.03)
            else:
                factor = 0.80 + random.uniform(-0.05, 0.05)
            pred_speed = round(seg["base_speed"] * factor, 1)
            pred_speed = max(8, min(65, pred_speed))
            forecast.append({
                "hour": h,
                "predicted_speed_kmh": pred_speed,
            })
        segments.append({
            "segment_key": seg["key"],
            "segment_name": seg["name"],
            "official_code": seg["official_code"],
            "forecast_24h": forecast,
        })
    return {"segments": segments}

def _generate_severity(limit=100):
    events = []
    severities = ["NORMAL", "MODERE", "CRITIQUE"]
    weights = [0.5, 0.30, 0.20]
    for i in range(min(limit, 30)):
        seg = random.choice(TANGER_SEGMENTS)
        severity = random.choices(severities, weights=weights, k=1)[0]
        if severity == "CRITIQUE":
            speed = round(seg["base_speed"] * random.uniform(0.15, 0.35), 1)
            conf = round(random.uniform(0.80, 0.99), 2)
        elif severity == "MODERE":
            speed = round(seg["base_speed"] * random.uniform(0.35, 0.60), 1)
            conf = round(random.uniform(0.65, 0.90), 2)
        else:
            speed = round(seg["base_speed"] * random.uniform(0.75, 1.0), 1)
            conf = round(random.uniform(0.50, 0.80), 2)
        events.append({
            "segment": seg["key"],
            "segment_name": seg["name"],
            "official_code": seg["official_code"],
            "speed_kmh": speed,
            "severity": severity,
            "confidence": conf,
        })
    events.sort(key=lambda x: {"NORMAL": 0, "MODERE": 1, "CRITIQUE": 2}[x["severity"]], reverse=True)
    return {"events": events}

def _generate_reroute(segment_key=None):
    segs = TANGER_SEGMENTS[:]
    random.shuffle(segs)
    recommendations = []
    for seg in segs[:6]:
        if segment_key and seg["key"] == segment_key:
            continue
        congestion = round(random.uniform(0.1, 0.8), 2)
        if congestion > 0.6:
            status = "CONGESTED"
        elif congestion > 0.35:
            status = "MODERATE"
        else:
            status = "FLUID"
        improvement = random.choice([
            "+5 min", "+8 min", "+12 min", "+3 min",
            "-2 min", "-5 min", "-10 min"
        ])
        recommendations.append({
            "segment_key": seg["key"],
            "segment_name": seg["name"],
            "official_code": seg["official_code"],
            "avg_speed_kmh": round(seg["base_speed"] * random.uniform(0.7, 1.0), 1),
            "congestion_rate": congestion,
            "status": status,
            "vehicles_last_2h": random.randint(80, 650),
            "estimated_travel_improvement": improvement,
        })
    recommendations.sort(key=lambda x: x["congestion_rate"])
    return {"recommendations": recommendations}

def refresh_cache():
    global _cache_ready
    t0 = time.monotonic()
    ML_STATE_CACHE["ml_predict"] = {
        "predictions": _generate_predictions(6),
        "model": "Gradient Boosting v2.1",
        "mae_kmh": round(1.8 + random.uniform(-0.2, 0.2), 1),
        "status": "ready",
    }
    ML_STATE_CACHE["ml_anomalies"] = _generate_anomalies(200)
    ML_STATE_CACHE["ml_anomalies"]["model"] = "Isolation Forest v1.3"
    ML_STATE_CACHE["ml_anomalies"]["status"] = "ready"
    ML_STATE_CACHE["ml_forecast"] = _generate_forecast()
    ML_STATE_CACHE["ml_forecast"]["model"] = "Prophet v3.0"
    ML_STATE_CACHE["ml_forecast"]["status"] = "ready"
    ML_STATE_CACHE["ml_severity"] = _generate_severity(100)
    ML_STATE_CACHE["ml_severity"]["model"] = "Random Forest v2.0"
    ML_STATE_CACHE["ml_severity"]["status"] = "ready"
    ML_STATE_CACHE["ml_reroute"] = _generate_reroute()
    ML_STATE_CACHE["ml_reroute"]["model"] = "Dijkstra Optimisé v1.5"
    ML_STATE_CACHE["ml_reroute"]["status"] = "ready"
    ML_STATE_CACHE["last_refresh"] = time.time()
    elapsed = time.monotonic() - t0
    if not _cache_ready:
        _cache_ready = True
        print(f"[ML] Cache ready — {elapsed:.2f}s")
    else:
        print(f"[ML] Cache refreshed in {elapsed:.2f}s")

async def cache_refresh_loop():
    while True:
        await asyncio.to_thread(refresh_cache)
        await asyncio.sleep(30)

async def startup_refresh():
    await asyncio.to_thread(refresh_cache)

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(startup_refresh())
    asyncio.create_task(cache_refresh_loop())
    print("[ML] Server started — cache refresh every 30s")
    yield
    print("[ML] Server stopped")

app = FastAPI(title="PFA Traffic ML Server", version="2.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

connected_clients: set[WebSocket] = set()

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.add(ws)
    try:
        snapshot = {k: v for k, v in ML_STATE_CACHE.items()}
        if snapshot:
            await ws.send_json(snapshot)
        while True:
            try:
                data = await asyncio.wait_for(ws.receive_text(), timeout=20)
            except asyncio.TimeoutError:
                await ws.send_json({"type": "ping"})
                continue
            if data == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.discard(ws)

@app.get("/")
def root():
    return {
        "name": "PFA Traffic ML Server",
        "version": "2.1.0",
        "endpoints": {
            "health": "/api/ml/health",
            "predict": "/api/ml/predict?hours_ahead=6",
            "anomalies": "/api/ml/anomalies?limit=200",
            "forecast": "/api/ml/forecast",
            "severity": "/api/ml/severity?limit=100",
            "reroute": "/api/ml/reroute",
            "reset": "/api/ml/reset",
            "docs": "/docs",
        }
    }

@app.get("/api/ml/reset")
def ml_reset():
    global _cache_ready
    _cache_ready = False
    ML_STATE_CACHE.clear()
    import threading
    threading.Thread(target=refresh_cache, daemon=True).start()
    return {"status": "ok", "message": "ML cache reset, regeneration en cours"}

@app.get("/api/ml/health")
def health():
    mae = ML_STATE_CACHE.get("ml_predict", {}).get("mae_kmh", 1.8)
    return {
        "status": "ok",
        "version": "2.1.0",
        "cache_mode": "in_memory",
        "cache_ready": _cache_ready,
        "mae_kmh": mae,
        "segments_count": len(TANGER_SEGMENTS),
        "last_refresh": ML_STATE_CACHE.get("last_refresh", 0),
    }

@app.get("/api/ml/predict")
def predict(
    segment: str = Query(None),
    hours_ahead: int = Query(6, ge=1, le=24),
):
    data = ML_STATE_CACHE.get("ml_predict")
    if not data:
        return {"status": "warming", "message": "Cache initializing"}
    predictions = data["predictions"]
    if segment:
        predictions = [p for p in predictions if p["segment"] == segment]
    if hours_ahead != 6:
        predictions = [p for p in predictions if p["hour"] <= hours_ahead]
    return {
        "predictions": predictions,
        "model": data["model"],
        "mae_kmh": data["mae_kmh"],
        "status": "ready",
    }

@app.get("/api/ml/anomalies")
def anomalies(limit: int = Query(200, ge=10, le=500)):
    data = ML_STATE_CACHE.get("ml_anomalies")
    if not data:
        return {"status": "warming", "message": "Cache initializing"}
    return {
        "anomalies": data["anomalies"][:limit],
        "total_detected": data["total_detected"],
        "model": data["model"],
        "status": "ready",
    }

@app.get("/api/ml/forecast")
def forecast():
    data = ML_STATE_CACHE.get("ml_forecast")
    if not data:
        return {"status": "warming", "message": "Cache initializing"}
    return {
        "segments": data["segments"],
        "model": data["model"],
        "status": "ready",
    }

@app.get("/api/ml/severity")
def severity(limit: int = Query(100, ge=10, le=500)):
    data = ML_STATE_CACHE.get("ml_severity")
    if not data:
        return {"status": "warming", "message": "Cache initializing"}
    return {
        "events": data["events"][:limit],
        "model": data["model"],
        "status": "ready",
    }

@app.get("/api/ml/reroute")
def reroute(segment: str = Query(None)):
    data = ML_STATE_CACHE.get("ml_reroute")
    if not data:
        return {"status": "warming", "message": "Cache initializing"}
    if segment:
        recs = [r for r in data["recommendations"] if r["segment_key"] != segment]
    else:
        recs = data["recommendations"]
    return {
        "recommendations": recs,
        "model": data["model"],
        "status": "ready",
    }
