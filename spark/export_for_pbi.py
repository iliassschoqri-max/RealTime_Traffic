from pyspark.sql import SparkSession
import os

os.environ['HADOOP_HOME'] = r"C:\hadoop"

spark = SparkSession.builder \
    .appName("ExportPBI") \
    .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

GOLD = "data/delta/gold"
PBI = "data/pbi"
os.makedirs(PBI, exist_ok=True)

tables = ["dim_time", "dim_road_segment", "dim_event_type", "dim_vehicle", "fact_traffic_events"]

for t in tables:
    df = spark.read.format("delta").load(f"{GOLD}/{t}")
    df.coalesce(1).write.mode("overwrite").parquet(f"{PBI}/{t}")
    print(f"OK: {t} -> {PBI}/{t}")

spark.stop()
print("\nFichiers prets pour Power BI dans data/pbi/")
