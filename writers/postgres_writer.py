import hashlib
import polars as pl
from datetime import datetime, timezone
from writers.db_writer import DBWriter
from secret_handling.secret import Secret, SecretType
from sources.source import Source
import psycopg2
from psycopg2 import sql

DB_CONN_KEY = SecretType.DB_CONNECTION


class PostgresWriter(DBWriter):
    secrets: Secret | None = None

    def __init__(self, secrets: Secret):
        self.secrets = secrets
        self.conn_str = self.secrets.get_required_key(DB_CONN_KEY)

    @staticmethod
    def _map_dtype(dtype) -> str:
        # Map polars dtypes to simple Postgres types
        from polars import Utf8, Int64, Float64, Boolean, Datetime
        if str(dtype).startswith("Utf8"):
            return "TEXT"
        if str(dtype).startswith("Int"):
            return "BIGINT"
        if str(dtype).startswith("Float"):
            return "DOUBLE PRECISION"
        if str(dtype).startswith("Boolean"):
            return "BOOLEAN"
        if "Datetime" in str(dtype) or "Date" in str(dtype):
            return "TIMESTAMP"
        # Fallback
        return "TEXT"

    def write_scd2(self, df: pl.DataFrame, source: Source):
        source_name = source.name
        tag = source.tag.name

        # ensure source/user columns exists
        if source.user:
            user = source.user.value
            df = df.with_columns(
                pl.lit(user).cast(pl.Utf8).fill_null("NULL").alias("user"),
                pl.lit(source_name).cast(pl.Utf8).fill_null("NULL").alias("source"),
            )
        else:
            user = "NULL"
            df = df.with_columns(
                pl.lit(source_name).cast(pl.Utf8).fill_null("NULL").alias("source"),
            )

        # compute row_hash similarly to DuckDBWriter
        df = df.with_columns([
            pl.concat_str([df[col].cast(pl.Utf8).fill_null("NULL") for col in df.columns], separator="|")
            .map_elements(lambda x: hashlib.md5(x.encode()).hexdigest(), return_dtype=pl.Utf8, skip_nulls=False)
            .alias("row_hash")
        ])
        df = df.unique(subset=["row_hash"])
        df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("loaded_at"))

        pks_only = df.select(["pk", "loaded_at"]).unique(subset=["pk"])

        conn = psycopg2.connect(self.conn_str)
        conn.autocommit = False
        cur = conn.cursor()

        try:
            # Hub
            hub_table = sql.Identifier(f"hub_{tag}")
            cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (pk TEXT PRIMARY KEY, created_at TIMESTAMP)").format(hub_table))

            # insert new pks
            insert_hub_q = sql.SQL("INSERT INTO {} (pk, created_at) SELECT pk, loaded_at FROM (VALUES %s) AS v(pk, loaded_at) ON CONFLICT (pk) DO NOTHING").format(hub_table)
            pks = [(row[0], row[1].to_python()) for row in pks_only.rows()]
            if pks:
                psycopg2.extras.execute_values(cur, insert_hub_q.as_string(cur), pks, template=None, page_size=100)

            # Satellite
            schema_id = hashlib.md5(str([(name, str(dtype)) for name, dtype in df.schema.items()]).encode()).hexdigest()
            sat_name = f"sat_{tag}_{source_name}_{user}_{schema_id}"
            sat_table = sql.Identifier(sat_name)

            # Build DDL for satellite based on df schema
            cols = []
            for name, dtype in df.schema.items():
                pg_type = self._map_dtype(dtype)
                cols.append(sql.SQL("{} {}",).format(sql.Identifier(name), sql.SQL(pg_type)))
            # ensure row_hash and loaded_at present
            cols.append(sql.SQL("{} {}",).format(sql.Identifier("row_hash"), sql.SQL("TEXT")))
            cols.append(sql.SQL("{} {}",).format(sql.Identifier("loaded_at"), sql.SQL("TIMESTAMP")))

            create_cols_sql = sql.SQL(", ").join(cols)
            cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(sat_table, create_cols_sql))

            # Ensure unique index on row_hash
            cur.execute(sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {idx} ON {tbl} (row_hash)").format(
                idx=sql.Identifier(f"idx_{sat_name}_row_hash"),
                tbl=sat_table
            ))

            # Insert rows where row_hash not exists
            # We'll insert row-by-row for clarity (small datasets). Use parameterized insert.
            col_names = [col for col in df.columns] + ["row_hash", "loaded_at"]
            placeholders = sql.SQL(',').join(sql.Placeholder() * len(col_names))
            insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (row_hash) DO NOTHING").format(
                sat_table,
                sql.SQL(',').join(map(sql.Identifier, col_names)),
                placeholders
            )

            rows = []
            for r in df.rows():
                # df.rows() includes columns in order; ensure types are python native
                row = []
                for v in r:
                    if hasattr(v, 'to_python'):
                        row.append(v.to_python())
                    else:
                        row.append(v)
                rows.append(tuple(row))

            for r in rows:
                cur.execute(insert_sql, r)

            conn.commit()

        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        source.hub = f"hub_{tag}"
        if not hasattr(source, 'satellites'):
            source.satellites = set()
        source.satellites.add(sat_name)
        print(f"    📝 Wrote satellite {sat_name} to Postgres")
