import sys
sys.path.insert(0, r"C:\Users\HP\Downloads\PFA_RealTime_Traffic\PFA_RealTime_Traffic\dashboard")
from core.trino_client import trino_config, logger
import trino.dbapi

auth = trino.auth.BasicAuthentication(trino_config.user, "") if trino_config.user else None
conn = trino.dbapi.connect(
    host=trino_config.host, port=trino_config.port, user=trino_config.user,
    catalog=trino_config.catalog, schema=trino_config.schema,
    http_scheme=trino_config.http_scheme, auth=auth,
    source=trino_config.source, max_attempts=1, request_timeout=trino_config.request_timeout,
)
cur = conn.cursor()
tables = ['dim_time', 'dim_road_segment', 'dim_event_type', 'dim_vehicle', 'fact_traffic_events']
for t in tables:
    try:
        sql = f"CALL delta.system.unregister_table(schema_name => 'gold', table_name => '{t}')"
        cur.execute(sql)
        conn.commit()
        print(f"Unregistered: {t}")
    except Exception as e:
        print(f"Error unregistering {t}: {e}")
conn.close()
