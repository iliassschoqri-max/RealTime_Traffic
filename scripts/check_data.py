import sys
sys.path.insert(0, r"C:\Users\HP\Downloads\PFA_RealTime_Traffic\PFA_RealTime_Traffic\dashboard")
from core.trino_client import TrinoClient

c = TrinoClient()

r = c.query("SELECT COUNT(*) as cnt FROM delta.gold.fact_traffic_events")
print(f"Gold fact: {r['cnt'].values[0]} rows")

r = c.query("SELECT COUNT(*) as cnt FROM delta.gold.dim_time")
print(f"Dim time: {r['cnt'].values[0]} rows")

r = c.query("SELECT COUNT(*) as cnt FROM delta.gold.dim_road_segment")
print(f"Dim road: {r['cnt'].values[0]} rows")

r = c.query("SELECT event_type_key, COUNT(*) as cnt FROM delta.gold.fact_traffic_events GROUP BY event_type_key")
print(f"Events by type:\n{r.to_string()}")
