import logging
import signal
import time

import pandas as pd
import trino
import trino.auth

logger = logging.getLogger("trino_client")


class TrinoConfig:
    host = "localhost"
    port = 8081
    user = "admin"
    catalog = "delta"
    schema = "gold"
    http_scheme = "http"
    source = "pfa_pipeline"
    request_timeout = 30


trino_config = TrinoConfig()


class TrinoClient:
    def __init__(self, config: TrinoConfig = None):
        self.config = config or trino_config

    def _connect(self):
        auth = (
            trino.auth.BasicAuthentication(self.config.user, "")
            if self.config.user
            else None
        )
        return trino.dbapi.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            catalog=self.config.catalog,
            schema=self.config.schema,
            http_scheme=self.config.http_scheme,
            auth=auth,
            source=self.config.source,
            max_attempts=1,
            request_timeout=self.config.request_timeout,
        )

    def query(self, sql: str) -> pd.DataFrame:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            return pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()

    def query_with_timeout(self, sql: str, timeout_sec: int = 30) -> pd.DataFrame | None:
        """Execute a query with a hard timeout. Returns None if it times out."""
        result = [None]
        error = [None]
        done = [False]

        def _run():
            try:
                result[0] = self.query(sql)
            except Exception as e:
                error[0] = e
            finally:
                done[0] = True

        import threading
        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout_sec)

        if not done[0]:
            logger.warning(f"[TIMEOUT] Trino query timed out after {timeout_sec}s: {sql[:80]}...")
            return None
        if error[0]:
            logger.warning(f"[TRINO ERROR] {error[0]}")
            return None
        return result[0]

    def execute(self, sql: str):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        finally:
            conn.close()

    def flush_metadata_cache(self, schema: str = "gold", table: str = "fact_traffic_events"):
        try:
            sql = f"CALL delta.system.flush_metadata_cache(schema_name => '{schema}', table_name => '{table}')"
            self.execute(sql)
            logger.debug(f"Flushed Trino metadata cache for {schema}.{table}")
        except Exception as e:
            logger.warning(f"flush_metadata_cache failed (non-fatal): {e}")
