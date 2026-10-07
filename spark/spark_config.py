import os
import socket
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(app_name: str = "TrafficPipeline") -> SparkSession:

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = r"C:\hadoop"
    os.environ['SPARK_LOCAL_HOSTNAME'] = "127.0.0.1"
    os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"
    os.environ['_JAVA_OPTIONS'] = '-Djava.net.preferIPv4Stack=true'

    extra_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
        "org.postgresql:postgresql:42.7.2",
        "com.google.code.findbugs:jsr305:3.0.0",
    ]

    builder = configure_spark_with_delta_pip(
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.executor.host", "127.0.0.1")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "120s")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.streaming.backpressure.enabled", "true")
        .config("spark.sql.streaming.metricsEnabled", "false")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.storage.blockManagerSlaveTimeoutMs", "600000")
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem"),
        extra_packages=extra_packages,
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.conf.set("spark.databricks.delta.properties.defaults.appendOnly", "true")
    spark.conf.set("spark.databricks.delta.commitValidation.enabled", "true")
    return spark
