"""
reset_project.py — Purge totale du pipeline PFA
Conserve la structure des dossiers, efface toutes les donnees.
"""

import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).parent

# ─────────────────────────────────────────────
# Utils
# ─────────────────────────────────────────────

def _empty_dir(path: Path):
    """Supprime fichiers + sous-dossiers, conserve le dossier vide."""
    if not path.exists():
        path.mkdir(parents=True)
        return
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()

def _http_get(url: str, timeout: int = 5):
    try:
        urllib.request.urlopen(url, timeout=timeout)
        return True
    except Exception:
        return False

def _run(cmd: list[str], ok_msg: str = "", err_msg: str = ""):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            if ok_msg:
                print(f"  [OK] {ok_msg}")
            return True
        else:
            if err_msg:
                print(f"  [--] {err_msg}")
            return False
    except Exception as e:
        if err_msg:
            print(f"  [--] {err_msg} ({e})")
        return False

# ─────────────────────────────────────────────
# 1. Dossiers de donnees (Delta / checkpoint / cache)
# ─────────────────────────────────────────────

DELTA_DIRS = [
    "data/delta/bronze",
    "data/delta/silver",
    "data/delta/gold/dim_event_type",
    "data/delta/gold/dim_road_segment",
    "data/delta/gold/dim_time",
    "data/delta/gold/dim_vehicle",
    "data/delta/gold/fact_traffic_events",
    "data/checkpoints/bronze",
    "data/checkpoints/silver",
    "data/checkpoints/gold",
]

def clean_data_dirs():
    print("=" * 60)
    print("[1/5] Purge des dossiers de donnees...")
    for rel in DELTA_DIRS:
        p = BASE_DIR / rel
        _empty_dir(p)
        print(f"  [OK] VIDE : {rel}")
    # data/cache peut exister
    cache_dir = BASE_DIR / "data" / "cache"
    if cache_dir.exists():
        _empty_dir(cache_dir)
        print(f"  [OK] VIDE : data/cache")
    print()

# ─────────────────────────────────────────────
# 2. Logs, rapports, caches Python
# ─────────────────────────────────────────────

def clean_artifacts():
    print("=" * 60)
    print("[2/5] Artefacts (logs, rapports, __pycache__)...")

    logs_dir = BASE_DIR / "logs"
    if logs_dir.exists():
        _empty_dir(logs_dir)
        print("  [OK] VIDE : logs/")
    else:
        print("  [--] Absent : logs/")

    reports_dir = BASE_DIR / "reports"
    if reports_dir.exists():
        _empty_dir(reports_dir)
        print("  [OK] VIDE : reports/")
    else:
        print("  [--] Absent : reports/")

    # __pycache__ recursifs
    count = 0
    for p in BASE_DIR.rglob("__pycache__"):
        if p.is_dir():
            shutil.rmtree(p)
            count += 1
    print(f"  [OK] {count} dossier(s) __pycache__ supprime(s)")
    print()

# ─────────────────────────────────────────────
# 3. Kafka — purge de tous les topics
# ─────────────────────────────────────────────

KAFKA_TOPICS = ["traffic-events"]

def clean_kafka():
    print("=" * 60)
    print("[3/5] Reinitialisation Kafka...")

    # Tuer les groupes de consommateurs
    for topic in KAFKA_TOPICS:
        _run(
            ["docker", "exec", "pfa_kafka", "kafka-consumer-groups",
             "--bootstrap-server", "localhost:9092",
             "--group", f"spark-{topic}", "--reset-offsets",
             "--all-topics", "--to-earliest", "--execute"],
            err_msg=f"Groups consumer pour {topic} (normal si inexistant)"
        )

    # Supprimer + recreer chaque topic
    for topic in KAFKA_TOPICS:
        _run(
            ["docker", "exec", "pfa_kafka", "kafka-topics",
             "--bootstrap-server", "localhost:9092",
             "--delete", "--topic", topic],
            err_msg=f"Topic {topic} deja absent"
        )
        _run(
            ["docker", "exec", "pfa_kafka", "kafka-topics",
             "--bootstrap-server", "localhost:9092",
             "--create", "--topic", topic,
             "--partitions", "3", "--replication-factor", "1"],
            ok_msg=f"Topic {topic} recree (3 partitions)",
            err_msg=f"ERREUR creation topic {topic}"
        )
    print()

# ─────────────────────────────────────────────
# 4. Trino / Hive Metastore
# ─────────────────────────────────────────────

TRINO_TABLES = [
    "delta.gold.dim_time",
    "delta.gold.dim_road_segment",
    "delta.gold.dim_event_type",
    "delta.gold.dim_vehicle",
    "delta.gold.fact_traffic_events",
]

# Tables a supprimer dans l'ordre (contrainte FK)
TRINO_DROP_SQLS = [
    "DROP TABLE IF EXISTS delta.gold.fact_traffic_events",
    "DROP TABLE IF EXISTS delta.gold.dim_time",
    "DROP TABLE IF EXISTS delta.gold.dim_road_segment",
    "DROP TABLE IF EXISTS delta.gold.dim_event_type",
    "DROP TABLE IF EXISTS delta.gold.dim_vehicle",
    # Nettoyer aussi bronze/silver si presents
    "DROP TABLE IF EXISTS delta.bronze.traffic_events",
    "DROP TABLE IF EXISTS delta.silver.traffic_events",
]

def clean_trino():
    print("=" * 60)
    print("[4/5] Nettoyage metastore Trino/Hive...")
    for sql in TRINO_DROP_SQLS:
        _run(
            ["docker", "exec", "pfa_trino", "trino", "--execute", sql],
            ok_msg=f"Requete Trino OK : {sql[:60]}...",
            err_msg=f"Trino : {sql[:60]}"
        )
    print()

# ─────────────────────────────────────────────
# 5. Caches API + ML (si serveurs actifs)
# ─────────────────────────────────────────────

def reset_caches():
    print("=" * 60)
    print("[5/5] Reset des caches API et ML...")

    api_ok = _http_get("http://localhost:8000/api/kpi/reset-cache")
    print(f"  {'[OK]' if api_ok else '[--]'} API cache reset "
          f"({'effectue' if api_ok else 'serveur non joignable'})")

    ml_ok = _http_get("http://localhost:8001/api/ml/reset")
    print(f"  {'[OK]' if ml_ok else '[--]'} ML cache reset "
          f"({'effectue' if ml_ok else 'serveur non joignable'})")
    print()

# ─────────────────────────────────────────────
# Verification finale
# ─────────────────────────────────────────────

VERIFY_DIRS = DELTA_DIRS + [
    "logs",
    "reports",
]

def verify_clean():
    print("=" * 60)
    print("[VERIFICATION] Integration des dossiers...")
    ok = True
    for rel in VERIFY_DIRS:
        p = BASE_DIR / rel
        if p.exists():
            children = list(p.iterdir())
            status = "[VIDE]" if not children else f"[{len(children)} FICHIERS]"
            print(f"  {status} {rel}")
            if children:
                ok = False
        else:
            print(f"  [MANQUANT] {rel}")
            ok = False
    # Verifier __pycache__ residues
    pycache_count = len(list(BASE_DIR.rglob("__pycache__")))
    if pycache_count > 0:
        print(f"  [ATTENTION] {pycache_count} __pycache__ residuels")
    print()
    if ok:
        print(">>> Environnement pret pour un nouveau flux de donnees.")
    else:
        print(">>> Certains dossiers contiennent encore des fichiers.")
    print("=" * 60)

# ═════════════════════════════════════════════
if __name__ == "__main__":
    print()
    print("  PFA — Purge Complete du Pipeline")
    print("  (Conserve la structure, efface toutes les donnees)")
    print()

    ans = input("Supprimer TOUTES les donnees ? (yes/no) : ").strip().lower()
    if ans != "yes":
        print("Annule.")
        sys.exit(0)

    t0 = time.time()
    clean_data_dirs()
    clean_artifacts()
    clean_kafka()
    clean_trino()
    reset_caches()
    verify_clean()
    print(f"Duree totale : {time.time() - t0:.1f}s")
