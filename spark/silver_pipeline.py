import os
import shutil
import time

from pyspark.sql import functions as F

from spark_config import create_spark_session

BRONZE_PATH     = "data/delta/bronze"
SILVER_PATH     = "data/delta/silver"
CHECKPOINT_PATH = "data/checkpoints/silver"

SPEED_MIN = 0
SPEED_MAX = 200
VALID_EVENT_TYPES = ["normal", "incident", "congestion"]

MAX_RETRIES = 5
RETRY_DELAY = 5


def apply_quality_checks(df):
    return (
        df
        .filter(
            F.col("vehicle_id").isNotNull()
            & F.col("event_timestamp").isNotNull()
            & F.col("road_segment_id").isNotNull()
        )
        .filter(
            F.col("speed_kmh").isNotNull()
            & (F.col("speed_kmh") >= SPEED_MIN)
            & (F.col("speed_kmh") <= SPEED_MAX)
        )
        .filter(F.col("event_type").isin(*VALID_EVENT_TYPES))
    )


def deduplicate_stream(df):
    watermark = "10 seconds"
    return (
        df.withWatermark("event_timestamp", watermark)
          .dropDuplicates(["vehicle_id", "event_timestamp"])
    )


def enrich_data(df):
    return (
        df
        .withColumn(
            "speed_category",
            F.when(F.col("speed_kmh") <  20,  "very_slow")
             .when(F.col("speed_kmh") <  50,  "slow")
             .when(F.col("speed_kmh") <  90,  "normal")
             .when(F.col("speed_kmh") < 130,  "fast")
             .otherwise("very_fast"),
        )
        .withColumn("is_incident",   (F.col("event_type") == "incident").cast("integer"))
        .withColumn("is_congestion", (F.col("event_type") == "congestion").cast("integer"))
        .withColumn("event_hour",  F.hour("event_timestamp"))
        .withColumn("event_date",  F.to_date("event_timestamp"))
        .withColumn("day_of_week", F.dayofweek("event_timestamp"))
        .withColumn(
            "time_of_day",
            F.when(F.col("event_hour").between(6,  9),  "morning_rush")
             .when(F.col("event_hour").between(10, 15), "midday")
             .when(F.col("event_hour").between(16, 19), "evening_rush")
             .when(F.col("event_hour").between(20, 23), "evening")
             .otherwise("night"),
        )
        .withColumn("silver_processed_time", F.current_timestamp())
    )


def start_stream(spark):
    bronze_stream = (
        spark.readStream
        .format("delta")
        .option("path", BRONZE_PATH)
        .option("skipChangeCommits", "true")
        .load()
    )

    cleaned  = apply_quality_checks(bronze_stream)
    deduped  = deduplicate_stream(cleaned)
    enriched = enrich_data(deduped)

    return (
        enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("failOnDataLoss", "false")
        .trigger(processingTime="2 seconds")
        .start(SILVER_PATH)
    )


def run_silver_pipeline() -> None:
    spark = create_spark_session("SilverPipeline")
    spark.sparkContext.setLogLevel("WARN")

    print(f"[SILVER] Pipeline started - path: {SILVER_PATH}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                print(f"[SILVER] Retry {attempt}/{MAX_RETRIES} - restarting...")
                time.sleep(RETRY_DELAY * attempt)

            query = start_stream(spark)
            print(f"[SILVER] Stream {attempt} running. Ctrl+C to stop.")
            query.awaitTermination()

        except Exception as e:
            print(f"[SILVER] Stream error on attempt {attempt}: {e}")
            try:
                query.stop()
            except Exception:
                pass
            if attempt < MAX_RETRIES:
                print(f"[SILVER] Clearing checkpoint for clean restart...")
                if os.path.exists(CHECKPOINT_PATH):
                    shutil.rmtree(CHECKPOINT_PATH, ignore_errors=True)
                continue
            raise


if __name__ == "__main__":
    run_silver_pipeline()
