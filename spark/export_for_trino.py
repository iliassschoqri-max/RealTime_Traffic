import sys, os
os.environ['SPARK_LOCAL_HOSTNAME'] = '127.0.0.1'
sys.path.insert(0, os.path.dirname(__file__))

from spark_config import create_spark_session

spark = create_spark_session("ExportTrino")
spark.sparkContext.setLogLevel("WARN")

GOLD = "data/delta/gold"
PBI = "data/pbi"
os.makedirs(PBI, exist_ok=True)

tables = ["dim_time", "dim_road_segment", "dim_event_type", "dim_vehicle", "fact_traffic_events"]

for t in tables:
    path = f"{GOLD}/{t}"
    if not os.path.exists(path):
        print(f"SKIP: {t} not found")
        continue
    df = spark.read.format("delta").load(path)
    cnt = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(f"{PBI}/{t}")
    print(f"OK: {t} -> {PBI}/{t} ({cnt} rows)")

spark.stop()
print("Export termine - fichiers prets dans data/pbi/")
