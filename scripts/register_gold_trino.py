import trino.dbapi

conn = trino.dbapi.connect(
    host='localhost', port=8081, user='admin',
    catalog='delta', http_scheme='http', max_attempts=3, request_timeout=30
)
cur = conn.cursor()

try:
    cur.execute('CREATE SCHEMA IF NOT EXISTS delta.gold')
    print('[OK] Schema gold created')
except Exception as e:
    print(f'[WARN] Schema gold: {e}')

tables = [
    ('dim_time',           '/data/delta/gold/dim_time'),
    ('dim_road_segment',   '/data/delta/gold/dim_road_segment'),
    ('dim_event_type',     '/data/delta/gold/dim_event_type'),
    ('dim_vehicle',        '/data/delta/gold/dim_vehicle'),
    ('fact_traffic_events','/data/delta/gold/fact_traffic_events'),
]
for name, path in tables:
    try:
        sql = "CALL delta.system.register_table(schema_name => 'gold', table_name => '" + name + "', table_location => '" + path + "')"
        cur.execute(sql)
        conn.commit()
        print(f'[OK] Registered: {name}')
    except Exception as e:
        print(f'[ERR] {name}: {e}')

cur.execute('SELECT table_name, table_type FROM delta.information_schema.tables WHERE table_schema = \'gold\'')
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f'  -> {row[0]} ({row[1]})')
else:
    print('[WARN] No tables found in gold schema')

conn.close()
