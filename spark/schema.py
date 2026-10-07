"""
╔══════════════════════════════════════════════════════════════╗
║  PFA — Schémas PySpark                                       ║
║  Définition du schéma des événements bruts (couche Bronze)   ║
╚══════════════════════════════════════════════════════════════╝
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)

# Schéma Bronze — événements bruts Kafka (matching traffic_producer.py)
BRONZE_SCHEMA = StructType([
    StructField("vehicle_id",        StringType(),    False),
    StructField("road_id",           StringType(),    True),
    StructField("city_zone",         StringType(),    True),
    StructField("speed",             IntegerType(),   True),
    StructField("congestion_level",  IntegerType(),   True),
    StructField("weather",           StringType(),    True),
    StructField("event_time",        StringType(),    True),
])
