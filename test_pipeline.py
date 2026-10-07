"""
test_pipeline.py — Test complet du pipeline PFA
Kafka → Bronze → Silver → Gold → Trino
"""
import subprocess, sys, time, json, os
from pathlib import Path

BASE = Path(__file__).parent
SPARK = BASE / "spark"
DATA = BASE / "data"

def run(cmd, cwd=None, timeout=30):
    print(f"\n$ {cmd}")
    p = subprocess.run(cmd, shell=True, cwd=cwd or BASE, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        print(p.stderr[:500])
    else:
        print(p.stdout[:500])
    return p

def wait_for_stream(script, duration=25):
    import signal
    p = subprocess.Popen(
        [sys.executable, str(SPARK / script)],
        cwd=SPARK, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        p.wait(timeout=duration)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    out = p.stdout.read()
    err = p.stderr.read()
    for line in (out + err).split("\n"):
        if any(x in line for x in ["[BRONZE]", "[SILVER]", "[GOLD]", "Error", "Exception"]):
            print(f"  {line[:200]}")
    return p.returncode

# ── 1. Clean ──
print("="*60)
print("1. CLEANUP")
for d in ["delta/bronze", "delta/silver", "delta/gold", "parquet/bronze", "pbi"]:
    p = DATA / d
    if p.exists():
        import shutil; shutil.rmtree(p, ignore_errors=True)
for d in ["bronze", "silver", "gold"]:
    p = DATA / "checkpoints" / d
    if p.exists():
        import shutil; shutil.rmtree(p, ignore_errors=True)
print("  Old data cleaned")

# ── 2. Reset Kafka topic ──
print("\n" + "="*60)
print("2. KAFKA TOPIC")
run('docker exec pfa_kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic traffic-events 2>/dev/null')
run('docker exec pfa_kafka kafka-topics --bootstrap-server localhost:9092 --create --topic traffic-events --partitions 3 --replication-factor 1 2>/dev/null')

# ── 3. Produce test data ──
print("\n" + "="*60)
print("3. PRODUCER (10 sec)")
prod = subprocess.Popen([sys.executable, str(BASE / "producer" / "traffic_producer.py")])
time.sleep(10)
prod.kill()
prod.wait()
print("  Producer done")

# ── 4. Bronze ──
print("\n" + "="*60)
print("4. BRONZE PIPELINE")
wait_for_stream("bronze_pipeline.py", duration=30)

# Check bronze
r = run(f'{sys.executable} -c "from pyspark.sql import SparkSession; s=SparkSession.builder.appName(\"t\").master(\"local[2]\").config(\"spark.sql.extensions\",\"io.delta.sql.DeltaSparkSessionExtension\").config(\"spark.sql.catalog.spark_catalog\",\"org.apache.spark.sql.delta.catalog.DeltaCatalog\").getOrCreate(); d=s.read.format(\"delta\").load(\"data/delta/bronze\"); print(f\"Bronze: {d.count()} rows\"); d.show(3,truncate=False)"', timeout=120)
if "Bronze:" in r.stdout:
    print(f"  OK - {r.stdout.strip()}")

# ── 5. Silver ──
print("\n" + "="*60)
print("5. SILVER PIPELINE")
wait_for_stream("silver_pipeline.py", duration=20)

# ── 6. Gold ──
print("\n" + "="*60)
print("6. GOLD PIPELINE")
wait_for_stream("gold_pipeline.py", duration=70)

# ── 7. Register in Trino ──
print("\n" + "="*60)
print("7. REGISTER TABLES IN TRINO")
run('docker exec -i pfa_trino trino < sql/register_gold_tables.sql')
time.sleep(3)

# ── 8. Verify Trino ──
print("\n" + "="*60)
print("8. TRINO QUERIES")
tables = ["delta.gold.fact_traffic_events", "delta.gold.dim_time",
          "delta.gold.dim_road_segment", "delta.gold.dim_event_type", "delta.gold.dim_vehicle"]
for t in tables:
    r = run(f'docker exec pfa_trino trino --execute "SELECT COUNT(*) FROM {t}"', timeout=15)
    cnt = [l.strip() for l in r.stdout.split("\n") if l.strip().isdigit()]
    print(f"  {t}: {cnt[0] if cnt else '?'} rows")

kpi = run(f'docker exec pfa_trino trino --execute "SELECT COUNT(*) as cnt, ROUND(AVG(speed_kmh),1) as avg_spd FROM delta.gold.fact_traffic_events"', timeout=15)
print(f"\nKPI:\n{kpi.stdout[:300]}")

# ── 9. API test ──
print("\n" + "="*60)
print("9. API TEST (if running)")
r = run("curl -s http://localhost:8000/api/health 2>nul || echo API not running", timeout=5)
print(f"  {r.stdout[:100]}")

print("\n" + "="*60)
print("TEST COMPLETE")
