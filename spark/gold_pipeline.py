"""
##══════════════════════════════════════════════════════════════##
#   PFA -- Couche Gold                                           # 
#   Modelisation Star Schema : Fait + Dimensions                # 
##══════════════════════════════════════════════════════════════##
"""

import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_config import create_spark_session

# --- Chemins -------------------------------------------------
SILVER_PATH     = "data/delta/silver"
GOLD_BASE       = "data/delta/gold"
CHECKPOINT_PATH = "data/checkpoints/gold"

MAX_RETRIES = 5
RETRY_DELAY = 5


# ═══════════════════════════════════════════════════════════════
# Builders -- Tables de Dimensions
# ═══════════════════════════════════════════════════════════════

def build_dim_time(df):
    """dim_time -- Dimension temporelle."""
    time_key_expr = F.md5(F.concat_ws(
        "-",
        F.year("event_timestamp"),
        F.month("event_timestamp"),
        F.dayofmonth("event_timestamp"),
        F.hour("event_timestamp"),
        F.minute("event_timestamp"),
    ))
    return (
        df.select(
            time_key_expr.alias("time_key"),
            F.year("event_timestamp").alias("year"),
            F.month("event_timestamp").alias("month"),
            F.dayofmonth("event_timestamp").alias("day"),
            F.hour("event_timestamp").alias("hour"),
            F.minute("event_timestamp").alias("minute"),
            F.date_format("event_timestamp", "EEEE").alias("day_name"),
            F.col("day_of_week"),
            F.col("time_of_day"),
            F.col("event_date").alias("full_date"),
        )
        .dropDuplicates(["time_key"])
    )


def build_dim_road_segment(df):
    """dim_road_segment -- Dimension segments de route."""
    seg_window = Window.partitionBy("road_segment_id")
    return (
        df.select(
            F.col("road_segment_id").alias("segment_key"),
            F.col("road_id").alias("segment_name"),
            F.avg("latitude").over(seg_window).alias("avg_latitude"),
            F.avg("longitude").over(seg_window).alias("avg_longitude"),
        )
        .dropDuplicates(["segment_key"])
    )


def build_dim_event_type(df):
    """dim_event_type -- Dimension types et severite d'evenements."""
    return (
        df.select(
            F.col("event_type").alias("event_type_key"),
            F.col("event_type").alias("event_type_name"),
            F.when(F.col("event_type") == "normal",     1)
             .when(F.col("event_type") == "congestion", 2)
             .otherwise(3).alias("severity_level"),
            F.when(F.col("event_type") == "normal",     "Vert")
             .when(F.col("event_type") == "congestion", "Orange")
             .otherwise("Rouge").alias("severity_color"),
        )
        .dropDuplicates(["event_type_key"])
    )


def build_dim_vehicle(df):
    """dim_vehicle -- Dimension vehicules."""
    return (
        df.select(
            F.col("vehicle_id").alias("vehicle_key"),
            F.lit("unknown").alias("vehicle_type"),
            F.lit("MA").alias("registration_region"),
        )
        .dropDuplicates(["vehicle_key"])
    )


# ═══════════════════════════════════════════════════════════════
# Builder -- Table de Faits
# ═══════════════════════════════════════════════════════════════

def build_fact_table(df):
    """fact_traffic_events -- Table de faits principale."""
    time_key_expr = F.md5(F.concat_ws(
        "-",
        F.year("event_timestamp"),
        F.month("event_timestamp"),
        F.dayofmonth("event_timestamp"),
        F.hour("event_timestamp"),
        F.minute("event_timestamp"),
    ))
    return df.select(
        # Cles etrangeres
        F.col("vehicle_id"),
        F.col("road_segment_id").alias("segment_key"),
        F.col("event_type").alias("event_type_key"),
        time_key_expr.alias("time_key"),
        # Mesures
        F.col("speed_kmh"),
        F.col("is_incident"),
        F.col("is_congestion"),
        F.col("latitude"),
        F.col("longitude"),
        F.lit(0).alias("direction"),
        F.col("speed_category"),
        # Horodatages
        F.col("event_timestamp"),
        F.current_timestamp().alias("gold_loaded_at"),
    )


# ═══════════════════════════════════════════════════════════════
# Traitement par micro-batch
# ═══════════════════════════════════════════════════════════════

def _write_single_table(name: str, df) -> str:
    path = f"{GOLD_BASE}/{name}"
    df.write.format("delta").mode("append").option("path", path).save()
    return name


def process_gold_batch(batch_df, batch_id: int) -> None:
    """Ecrire toutes les tables Gold pour chaque micro-batch (écriture parallélisée)."""
    print(f"[GOLD] Batch {batch_id} -- Construction du Star Schema...")

    tables = {
        "dim_time":              build_dim_time(batch_df),
        "dim_road_segment":      build_dim_road_segment(batch_df),
        "dim_event_type":        build_dim_event_type(batch_df),
        "dim_vehicle":           build_dim_vehicle(batch_df),
        "fact_traffic_events":   build_fact_table(batch_df),
    }

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_write_single_table, name, df): name
            for name, df in tables.items()
        }
        for future in as_completed(futures):
            name = future.result()
            print(f"[GOLD]   v {name:<26} -> {GOLD_BASE}/{name}")

    # ═══════════════════════════════════════════════════════════════
# Point d'entree
# ═══════════════════════════════════════════════════════════════

def run_gold_pipeline() -> None:
    print(f"[GOLD] Pipeline started - base: {GOLD_BASE}")

    for attempt in range(1, MAX_RETRIES + 1):
        spark = None
        query = None
        try:
            if attempt > 1:
                print(f"[GOLD] Retry {attempt}/{MAX_RETRIES} - restarting...")
                time.sleep(RETRY_DELAY * attempt)

            spark = create_spark_session("GoldPipeline")
            spark.sparkContext.setLogLevel("WARN")

            silver_stream = (
                spark.readStream
                .format("delta")
                .option("path", SILVER_PATH)
                .option("skipChangeCommits", "true")
                .option("maxFilesPerTrigger", "1")
                .load()
            )

            query = (
                silver_stream.writeStream
                .foreachBatch(process_gold_batch)
                .option("checkpointLocation", CHECKPOINT_PATH)
                .trigger(processingTime="2 seconds")
                .start()
            )

            print(f"[GOLD] Stream {attempt} running. Ctrl+C to stop.")
            query.awaitTermination()

        except KeyboardInterrupt:
            print(f"[GOLD] Stopped by user.")
            break

        except Exception as e:
            print(f"[GOLD] Stream error on attempt {attempt}: {e}")
            if attempt >= MAX_RETRIES:
                raise
            print(f"[GOLD] Cleaning up for retry...")
            if os.path.exists(CHECKPOINT_PATH):
                shutil.rmtree(CHECKPOINT_PATH, ignore_errors=True)

        finally:
            try:
                if query is not None:
                    query.stop()
            except Exception:
                pass
            try:
                if spark is not None:
                    spark.stop()
            except Exception:
                pass


if __name__ == "__main__":
    run_gold_pipeline()
