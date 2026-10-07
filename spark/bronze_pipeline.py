import os
import shutil
import time

from pyspark.sql import functions as F

from spark_config import create_spark_session
from schema import BRONZE_SCHEMA

BRONZE_PATH     = "data/delta/bronze"
CHECKPOINT_PATH = "data/checkpoints/bronze"

KAFKA_SERVERS   = "localhost:9092"
KAFKA_TOPIC     = "traffic-events"

MAX_RETRIES     = 5
RETRY_DELAY     = 5


def create_bronze_stream(spark):
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "100")
        .option("kafka.group.id", "bronze-pipeline-consumer")
        .load()
    )

    bronze_df = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                BRONZE_SCHEMA,
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition"),
            F.col("offset"),
        )
        .select("data.*", "kafka_timestamp", "partition", "offset")
        .withColumn("bronze_ingestion_time", F.current_timestamp())
        .withColumn("pipeline_version",      F.lit("2.0"))
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn("road_segment_id", F.col("road_id"))
        .withColumn("speed_kmh", F.col("speed").cast("double"))
        .withColumn(
            "event_type",
            F.when(F.col("congestion_level") >= 4, "congestion")
             .when(F.col("speed").cast("int") <= 10, "incident")
             .otherwise("normal"),
        )
        .withColumn("latitude", F.lit(None).cast("double"))
        .withColumn("longitude", F.lit(None).cast("double"))
    )
    return bronze_df


def start_stream(spark, df):
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("failOnDataLoss", "false")
        .trigger(processingTime="0 seconds")
        .start(BRONZE_PATH)
    )


def run_bronze_pipeline() -> None:
    spark = create_spark_session("BronzePipeline")
    spark.sparkContext.setLogLevel("WARN")

    print(f"[BRONZE] Pipeline started - path: {BRONZE_PATH}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                print(f"[BRONZE] Retry {attempt}/{MAX_RETRIES} - restarting stream...")
                time.sleep(RETRY_DELAY * attempt)

            bronze_df = create_bronze_stream(spark)
            query = start_stream(spark, bronze_df)
            print(f"[BRONZE] Stream {attempt} running. Ctrl+C to stop.")
            query.awaitTermination()

        except Exception as e:
            if "STREAM_FAILED" in str(e) or "NullPointerException" in str(e):
                print(f"[BRONZE] Stream error on attempt {attempt}: {e}")
                query.stop()
                if attempt < MAX_RETRIES:
                    print(f"[BRONZE] Clearing checkpoint for clean restart...")
                    if os.path.exists(CHECKPOINT_PATH):
                        shutil.rmtree(CHECKPOINT_PATH, ignore_errors=True)
                    continue
                else:
                    print("[BRONZE] Max retries reached. Exiting.")
                    raise
            raise


if __name__ == "__main__":
    run_bronze_pipeline()
