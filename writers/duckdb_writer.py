import duckdb
import hashlib
import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from writers.db_writer import DBWriter
from secret_handling.secret import Secret, SecretType
from sources.source import Source

db_connection = SecretType.DB_CONNECTION

class DuckDBWriter(DBWriter):
    secrets: Secret | None = None

    def __init__(self, secrets: Secret):
        self.secrets = secrets
        self.path = self.secrets.get_required_key(SecretType.DB_CONNECTION)
        
    def hash_schema(df: pl.DataFrame) -> str:
        schema_str = str([(name, str(dtype)) for name, dtype in df.schema.items()])
        return hashlib.md5(schema_str.encode()).hexdigest()

    def write_scd2(self, df: pl.DataFrame, source: Source):
        source_name = source.name
        tag = source.tag.name
        path = self.path
        
        #TODO: not very nice. make better later (could this come earlier?)
        if source.user:
            user = source.user.value

            df = df.with_columns(
                pl.lit(user).cast(pl.Utf8).fill_null("NULL").alias("user"),
                pl.lit(source_name).cast(pl.Utf8).fill_null("NULL").alias("source"),
            )
        else:
            df = df.with_columns(
                pl.lit(source_name).cast(pl.Utf8).fill_null("NULL").alias("source"),
            )

        con = duckdb.connect(str(path))
        print(f"🔗 Connecting to DuckDB at {path}")
        print(f"    🔎 [{source_name}] Read {df.height} rows from source.")
      
        df = df.with_columns([
            pl.concat_str(
                [df[col].cast(pl.Utf8).fill_null("NULL") for col in df.columns],
                separator="|"
            ).map_elements(
                lambda x: hashlib.md5(x.encode()).hexdigest(),
                return_dtype=pl.Utf8,
                skip_nulls=False
            ).alias("row_hash")
        ])
        print("    🔸 Example row_hash:", df["row_hash"].to_list()[:1])
        df = df.unique(subset=["row_hash"])
        print(f"    ℹ️  {df.__len__()} de-duplicated rows after hashing")
        df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("loaded_at"))
        pks_only = df.select(["pk", "loaded_at"]).unique(subset=["pk"])

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
            SELECT pk, loaded_at FROM pks_only
            WHERE pk NOT IN (SELECT pk FROM {hub_table})
        """)

        # Satellite table logic
        schema_id = DuckDBWriter.hash_schema(df)
        satellite_table = f"sat_{tag}_{source_name}_{user}_{schema_id}"

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
        print(f"    📝 {written_count} new rows to {satellite_table} from {source_name}")

        con.close()
        source.hub = hub_table
        print(f"    💾 {hub_table} saved to {source_name}")
        source.satellites.add(satellite_table)
        print(f"    💾 {satellite_table} saved to {source_name}")