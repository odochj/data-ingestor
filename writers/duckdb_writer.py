# writers/duckdb_writer.py
import duckdb
import hashlib
import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from writers.db_writer import DBWriter
from secret_handling.secret import Secret, SecretType

db_connection = SecretType.DB_CONNECTION

class DuckDBWriter(DBWriter):
    secrets: Secret | None = None

    def __init__(self, secrets: Secret):
        self.secrets = secrets
        self.path = self.secrets.get_required_key(SecretType.DB_CONNECTION)
        
    def hash_schema(df: pl.DataFrame) -> str:
        schema_str = str([(name, str(dtype)) for name, dtype in df.schema.items()])
        return hashlib.md5(schema_str.encode()).hexdigest()

    def write_scd2(self,df: pl.DataFrame, tag: str, source_name: str ):
        path = self.path
        con = duckdb.connect(str(path))
        print(f"🔗 Connecting to DuckDB at {path}")
        print(f"    🔎 [{source_name}] Read {df.height} rows from source.")
        df = df.with_columns([
            pl.concat_str(df.columns, separator="|").hash().cast(str).alias("row_hash")
        ])
        df = df.unique(subset=["row_hash"])
        df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("loaded_at"))

        # Hub table logic
        hub_table = f"hub_{tag}"
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {hub_table} (
                pk TEXT PRIMARY KEY,
                created_at TIMESTAMP
            )
        """)
        
        # Insert only new primary keys
        con.execute(f"""
            INSERT INTO {hub_table}
            SELECT pk, loaded_at FROM df
            WHERE pk NOT IN (SELECT pk FROM {hub_table})
        """)

        # Satellite table logic
        schema_id = DuckDBWriter.hash_schema(df)
        satellite_table = f"sat_{tag}_{source_name}_{schema_id}"

        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {satellite_table} AS
            SELECT * FROM df WHERE false
        """)

        before_count = con.execute(f"SELECT COUNT(*) FROM {satellite_table}").fetchone()[0]
        con.execute(f"""
            INSERT INTO {satellite_table}
            SELECT * FROM df
            WHERE row_hash NOT IN (
            SELECT row_hash FROM {satellite_table}
            )
        """)

        after_count = con.execute(f"SELECT COUNT(*) FROM {satellite_table}").fetchone()[0]
        written_count = after_count - before_count
        print(f"    📝 [{source_name}] Wrote {written_count} new rows to {satellite_table}.")

        con.close()
