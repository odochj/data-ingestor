from dataclasses import dataclass
from typing import Dict
import polars as pl
import hashlib


@dataclass
class Tag:
    name: str
    mandatory_columns: Dict[str, pl.DataType]  # {canonical_name: expected_dtype}
    
    def validate_dataframe(self, df: pl.DataFrame, column_mapping: Dict[str, str]) -> None:
        """
        Ensure all required columns are present and match expected dtypes.

        Parameters:
        - df: polars.DataFrame as-is from source
        - column_mapping: maps canonical names to actual column names in df

        Raises:
        - ValueError if column missing
        - TypeError if dtype mismatch
        """
        for canonical_name, expected_dtype in self.mandatory_columns.items():
            source_column = column_mapping.get(canonical_name)

            if not source_column:
                raise ValueError(f"    ❌ Missing mapping for required tag column: '{canonical_name}'")

            if source_column not in df.columns:
                raise ValueError(f"    ❌ Missing required source column: '{source_column}'")

            actual_dtype = df[source_column].dtype
            if actual_dtype != expected_dtype:
                raise TypeError(
                    f"    ⚠️ Column '{source_column}' expected {expected_dtype}, got {actual_dtype}"
                )
            if actual_dtype == expected_dtype:
                print(f"    ✅ {source_column} is a valid {canonical_name}")

            else:
                pass

    def generate_primary_key(self, df: pl.DataFrame, key_columns: list[str]) -> pl.DataFrame:
        print("🔑 Generating primary key...")

        df = df.with_columns([
            pl.concat_str(
                [df[col].cast(pl.Utf8).fill_null("NULL") for col in key_columns],
                separator="|"
            ).map_elements(lambda x: hashlib.md5(x.encode()).hexdigest(), return_dtype=pl.Utf8).alias("pk")
        ])
        print("    🔸 Example primary key:", df["pk"].to_list()[:1])
        return df
