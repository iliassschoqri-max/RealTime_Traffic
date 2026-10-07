@echo off
REM Enregistre les tables Gold Delta dans Trino via Hive Metastore
docker exec -i pfa_trino trino < "%~dp0..\sql\register_gold_tables.sql"
echo Tables enregistrees. Verifier avec : docker exec pfa_trino trino --execute "SHOW TABLES FROM delta.gold;"
