import polars as pl
from datetime import datetime
import hashlib
from pathlib import Path
from secret_handling.secret import Secret

class DBWriter:
    secrets: Secret

    #TODO: REFACTOR DuckDBWriter methods to be re-usable across different writer objects
    # def hash_schema(df: pl.DataFrame) -> str:
    #     schema_str = str([(f.name, f.dtype) for f in df.schema])
    #     return hashlib.md5(schema_str.encode()).hexdigest()

    # def write_scd2(df: pl.DataFrame, tag: str, source_name: str, db_path: Path = Path("data.duckdb")):
    #     con = duckdb.connect(str(db_path))
    #     df = df.with_columns([
    #         pl.lit(datetime.now(datetime.timezone.utc)).alias("loaded_at"),
    #         pl.concat_str(df.columns, separator="|").hash().cast(str).alias("row_hash")
    #     ])

    #     # Hub table logic
    #     hub_table = f"hub_{tag}"
    #     con.execute(f"""
    #         CREATE TABLE IF NOT EXISTS {hub_table} (
    #             pk TEXT PRIMARY KEY,
    #             created_at TIMESTAMP
    #         )
    #     """)
        
    #     # Insert only new primary keys
    #     con.execute(f"""
    #         INSERT INTO {hub_table}
    #         SELECT DISTINCT pk, loaded_at FROM df
    #         EXCEPT
    #         SELECT pk, created_at FROM {hub_table}
    #     """)

    #     # Satellite table logic
    #     schema_id = DuckDBWriter.hash_schema(df)
    #     satellite_table = f"sat_{tag}_{source_name}_{schema_id}"

    #     con.execute(f"""
    #         CREATE TABLE IF NOT EXISTS {satellite_table} AS
    #         SELECT * FROM df WHERE false
    #     """)

    #     con.execute(f"""
    #         INSERT INTO {satellite_table}
    #         SELECT * FROM df
    #         WHERE row_hash NOT IN (
    #         SELECT row_hash FROM {satellite_table}
    #         )
    #     """)

    #     con.close()
