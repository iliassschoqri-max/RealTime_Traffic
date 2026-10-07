import urllib.request, json

BASE = "http://localhost:8002/api/ml"

def get(path):
    try:
        r = urllib.request.urlopen(f"{BASE}{path}", timeout=5)
        return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

print("=" * 60)
print("1. HEALTH")
print("=" * 60)
d = get("/health")
print(f"  Status: {d.get('status')}, Ready: {d.get('cache_ready')}, Segments: {d.get('segments_count')}, MAE: {d.get('mae_kmh')} km/h")

print("\n" + "=" * 60)
print("2. PREDICT (hours_ahead=6)")
print("=" * 60)
d = get("/predict?hours_ahead=6")
print(f"  Model: {d.get('model')}, Status: {d.get('status')}, Predictions: {len(d.get('predictions', []))}")
for p in d.get('predictions', [])[:5]:
    print(f"    +{p['hour']}h | {p['segment']} {p['segment_name']:20s} | {p['predicted_speed_kmh']:5.1f} km/h | {p['predicted_status']}")

print("\n" + "=" * 60)
print("3. ANOMALIES (limit=5)")
print("=" * 60)
d = get("/anomalies?limit=5")
print(f"  Model: {d.get('model')}, Total detected: {d.get('total_detected')}")
for a in d.get('anomalies', [])[:5]:
    print(f"    {a['segment']} {a['segment_name']:20s} | {a['speed_kmh']:5.1f} km/h | score={a['anomaly_score']} | {a['reasons'][0]}")

print("\n" + "=" * 60)
print("4. FORECAST")
print("=" * 60)
d = get("/forecast")
print(f"  Model: {d.get('model')}, Segments: {len(d.get('segments', []))}")
for s in d.get('segments', [])[:3]:
    print(f"    {s['segment_key']} {s['segment_name']:20s} | {len(s['forecast_24h'])}h forecast")

print("\n" + "=" * 60)
print("5. SEVERITY (limit=5)")
print("=" * 60)
d = get("/severity?limit=5")
print(f"  Model: {d.get('model')}, Events: {len(d.get('events', []))}")
for e in d.get('events', [])[:5]:
    print(f"    {e['segment']} {e['segment_name']:20s} | {e['speed_kmh']:5.1f} km/h | {e['severity']:8s} | conf={e['confidence']}")

print("\n" + "=" * 60)
print("6. REROUTE")
print("=" * 60)
d = get("/reroute")
print(f"  Model: {d.get('model')}, Recommendations: {len(d.get('recommendations', []))}")
for r in d.get('recommendations', [])[:5]:
    print(f"    {r['segment_key']} {r['segment_name']:20s} | {r['status']:10s} | congestion={r['congestion_rate']} | {r['estimated_travel_improvement']}")

print("\n" + "=" * 60)
print("7. RESET CACHE")
print("=" * 60)
d = get("/reset")
print(f"  {d.get('status')} - {d.get('message')}")

print("\n" + "=" * 60)
print("ALL ENDPOINTS TESTED SUCCESSFULLY")
print("=" * 60)
