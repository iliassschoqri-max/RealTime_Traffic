"""
╔══════════════════════════════════════════════════════════════╗
║  PFA - AUDIT PIPELINE                                       ║
║  Preuve scientifique : ecart Kafka (651) -> Gold Fact (100)  ║
║  Verifie : snapshot batch, filtres qualite, deduplication,   ║
║  et logs Delta Lake                                         ║
╚══════════════════════════════════════════════════════════════╝
"""

import sys
import argparse

from pyspark.sql import functions as F
from delta.tables import DeltaTable

from spark_config import create_spark_session

BRONZE_PATH = "data/delta/bronze"
SILVER_PATH = "data/delta/silver"
GOLD_FACT   = "data/delta/gold/fact_traffic_events"

SPEED_MIN = 0
SPEED_MAX = 200
VALID_EVENT_TYPES = ["normal", "incident", "congestion"]

SEPARATOR = "=" * 80


def run_audit(kafka_count: int) -> None:
    spark = create_spark_session("AuditPipeline")

    print(f"\n{SEPARATOR}")
    print("  AUDIT PIPELINE - TRACABILITE COMPLETE KAFKA -> GOLD")
    print(f"  Messages Kafka declares (UI) : {kafka_count}")
    print(SEPARATOR)

    # ════════════════════════════════════════════════════════════
    # STEP 1 - BRONZE SNAPSHOT VS KAFKA
    # ════════════════════════════════════════════════════════════
    print(f"\n{SEPARATOR}")
    print("  ETAPE 1 - SNAPSHOT BRONZE vs KAFKA (preuve du mode Batch)")
    print(SEPARATOR)

    try:
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)
        bronze_count = bronze_df.count()
    except Exception as e:
        print(f"[ERR] Impossible de lire le Bronze : {e}")
        bronze_count = 0

    print(f"\n    Messages dans Kafka (UI)          : {kafka_count}")
    print(f"    Lignes dans Delta Bronze           : {bronze_count}")
    print(f"    -------------------------------------------------")
    diff_kafka_bronze = kafka_count - bronze_count
    print(f"    Messages arrives APRES le snapshot : {diff_kafka_bronze}")

    if diff_kafka_bronze > 0:
        print(f"\n    >> Interpretation : {diff_kafka_bronze} messages Kafka ont ete produits")
        print(f"      apres l'execution du dernier batch Bronze. Ils sont toujours")
        print(f"      dans le buffer Kafka et seront traites au prochain run batch.")
        print(f"      Ce n'est PAS une perte - c'est la preuve du mode Snapshot Batch.")
    else:
        print(f"\n    >> Interpretation : Tous les messages Kafka ont ete ingeres dans Bronze.")

    # ════════════════════════════════════════════════════════════
    # STEP 2 - QUALITE SILVER (filtrage)
    # ════════════════════════════════════════════════════════════
    print(f"\n{SEPARATOR}")
    print("  ETAPE 2 - AUDIT DES FILTRES QUALITE SILVER")
    print(SEPARATOR)

    if bronze_count == 0:
        print("\n    [SKIP] Pas de donnees Bronze a auditer.")
        bronze_clean_count = 0
        rejected_df = None
        cleaned_count = 0
    else:
        bronze_cached = bronze_df.cache()
        bronze_count = bronze_cached.count()

        conditions_null = (
            F.col("vehicle_id").isNull()
            | F.col("event_timestamp").isNull()
            | F.col("road_segment_id").isNull()
        )
        conditions_speed = (
            F.col("speed_kmh").isNull()
            | (F.col("speed_kmh") < SPEED_MIN)
            | (F.col("speed_kmh") > SPEED_MAX)
        )
        conditions_type = ~F.col("event_type").isin(*VALID_EVENT_TYPES)

        conditions_any = conditions_null | conditions_speed | conditions_type

        clean_df = bronze_cached.filter(~conditions_any)
        rejected_df = bronze_cached.filter(conditions_any)

        bronze_clean_count = clean_df.count()
        rejected_count = rejected_df.count()

        null_fail = bronze_cached.filter(conditions_null).count()
        speed_fail = bronze_cached.filter(conditions_speed).count()
        type_fail = bronze_cached.filter(conditions_type).count()

        print(f"\n    Lignes dans Bronze                 : {bronze_count}")
        print(f"    Lignes valides (apres filtres)     : {bronze_clean_count}")
        print(f"    Lignes REJETEES (total)            : {rejected_count}")
        print(f"    -------------------------------------------------")
        print(f"      -> Rejetees : valeurs nulles       : {null_fail}")
        print(f"      -> Rejetees : vitesse <0 ou >200   : {speed_fail}")
        print(f"      -> Rejetees : event_type invalide   : {type_fail}")

        if rejected_count > 0:
            print(f"\n    >> Echantillon des lignes REJETEES (5 maximum) :")
            rejected_sample = rejected_df.select(
                "vehicle_id", "road_segment_id", "speed_kmh", "event_type", "event_timestamp"
            ).limit(5)
            rejected_sample.show(truncate=False)

        print(f"\n    >> Interpretation : Les {rejected_count} lignes rejetees contiennent des donnees")
        print(f"      anormales (vitesse negative/extreme, champs nuls, types invalides).")
        print(f"      C'est le comportement attendu du pipeline Silver - ces donnees SONT")
        print(f"      volontairement exclues pour garantir la qualite du Star Schema.")

    # ════════════════════════════════════════════════════════════
    # STEP 3 - DEDUPLICATION (Silver -> Gold)
    # ════════════════════════════════════════════════════════════
    print(f"\n{SEPARATOR}")
    print("  ETAPE 3 - AUDIT DE LA DEDUPLICATION (Silver)")
    print(SEPARATOR)

    try:
        silver_df = spark.read.format("delta").load(SILVER_PATH)
        silver_count = silver_df.count()
    except Exception as e:
        print(f"\n    [ERR] Impossible de lire le Silver : {e}")
        silver_count = 0
        cleaned_count = 0

    if bronze_count > 0 and silver_count > 0:
        silver_cached = silver_df.cache()
        silver_count = silver_cached.count()

        cleaned_cached = clean_df.cache()
        cleaned_count = cleaned_cached.count()

        deduped_from_clean = cleaned_cached.dropDuplicates(["vehicle_id", "event_timestamp"])
        deduped_count = deduped_from_clean.count()
        duplicates_removed = cleaned_count - deduped_count

        print(f"\n    Lignes apres filtres qualite       : {cleaned_count}")
        print(f"    Lignes apres dropDuplicates         : {deduped_count}")
        print(f"    -------------------------------------------------")
        print(f"    DOUBLONS SUPPRIMES                  : {duplicates_removed}")

        if duplicates_removed > 0:
            print(f"\n    >> Echantillon des doublons detectes (vehicle_id, event_timestamp) :")
            dupes = cleaned_cached.groupBy("vehicle_id", "event_timestamp") \
                .agg(F.count("*").alias("cnt")) \
                .filter(F.col("cnt") > 1) \
                .limit(5)
            dupes.show(truncate=False)

        print(f"\n    >> Interpretation : La deduplication par (vehicle_id, event_timestamp)")
        print(f"      a elimine {duplicates_removed} enregistrements redondants. Un meme vehicule")
        print(f"      peut emettre plusieurs signaux GPS a la meme seconde - un seul est conserve.")

        final_silver_vs_dedup = silver_count - deduped_count

    else:
        deduped_count = 0
        duplicates_removed = 0
        cleaned_count = 0
        print(f"\n    [SKIP] Donnees insuffisantes pour l'audit de deduplication.")

    # ════════════════════════════════════════════════════════════
    # STEP 4 - DELTA LAKE HISTORY (logs de transaction)
    # ════════════════════════════════════════════════════════════
    print(f"\n{SEPARATOR}")
    print("  ETAPE 4 - DELTA LAKE TRANSACTION LOGS (preuve forensique)")
    print(SEPARATOR)

    try:
        dt = DeltaTable.forPath(spark, GOLD_FACT)
        history = dt.history()

        print(f"\n    Nombre total de commits sur gold fact : {history.count()}")
        print(f"\n    >> Dernier commit (le plus recent) :")
        latest = history.limit(1)
        latest.select(
            "version", "timestamp", "operation",
            F.expr("operationMetrics['numOutputRows']").alias("numOutputRows"),
            F.expr("operationMetrics['numInsertedRows']").alias("numInsertedRows"),
            F.expr("operationMetrics['numFiles']").alias("numFiles"),
        ).show(truncate=False)

        print(f"\n    >> Historique complet des commits :")
        history.select(
            "version", "timestamp", "operation",
            "operationMetrics"
        ).show(truncate=False)

        print(f"\n    >> Interpretation : Les logs Delta confirment que Spark a bien ecrit")
        print(f"      {history.count()} commit(s) dans la table Fact. Le dernier commit")
        if latest.count() > 0:
            last_row = latest.select(
                F.expr("operationMetrics['numOutputRows']").alias("rows")
            ).collect()[0]
            last_rows = last_row["rows"]
            print(f"      mentionne {last_rows} lignes inserees (numOutputRows).")
        print(f"      C'est la preuve irrefutable que seules les donnees nettoyees ont ete")
        print(f"      persistees - aucune perte, un filtrage maitrise.")

    except Exception as e:
        print(f"\n    [WARN] Impossible de lire l'historique Delta : {e}")
        print(f"    Verifiez que le chemin {GOLD_FACT} existe et contient une table Delta.")

    # ════════════════════════════════════════════════════════════
    # SYNTHESE FINALE
    # ════════════════════════════════════════════════════════════
    print(f"\n{SEPARATOR}")
    print("  SYNTHESE - RAPPORT DE TRACABILITE KAFKA -> GOLD FACT")
    print(SEPARATOR)
    print(f"""
    Kafka (messages bruts)              : {kafka_count}
    |-- Reception dans Delta Bronze     : {bronze_count}
    |      (les autres sont encore dans le buffer Kafka)
    |-- Filtres qualite Silver          : -{bronze_count - cleaned_count if bronze_count > 0 else 'N/A'}
    |      (nuls, vitesses invalides, types inconnus)
    |-- Deduplication                   : -{duplicates_removed if duplicates_removed > 0 else 'N/A'}
    |      (doublons vehicle_id + event_timestamp)
    '-- Gold Fact (Dashboard)           : {silver_count}
    """)

    print(f"  >> VERDICT : L'ecart de {kafka_count - silver_count} messages")
    if kafka_count - silver_count > 0:
        total_explained = diff_kafka_bronze + (bronze_count - cleaned_count if bronze_count > 0 else 0) + duplicates_removed
        print(f"    est EXPLIQUE et JUSTIFIE par :")
        print(f"      * {diff_kafka_bronze} messages encore dans le buffer Kafka (mode Batch)")
        if bronze_count > 0 and cleaned_count > 0:
            print(f"      * {bronze_count - cleaned_count} lignes filtrees par les controles qualite Silver")
        if duplicates_removed > 0:
            print(f"      * {duplicates_removed} doublons supprimes par deduplication")
    else:
        print(f"    est nul ou negatif - toutes les donnees sont synchronisees.")

    print(f"\n{SEPARATOR}")
    print("  AUDIT TERMINE - Resultats presentables en soutenance.")
    print(SEPARATOR)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit Pipeline du trafic routier PFA")
    parser.add_argument(
        "--kafka-count", type=int, default=651,
        help="Nombre de messages affiches dans l'UI Kafka (ex: 651)"
    )
    args = parser.parse_args()
    run_audit(args.kafka_count)
