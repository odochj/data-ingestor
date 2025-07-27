from typing import Optional
from secret_handling.secret import Secret
from secret_handling.manager import SecretManager
from dataclasses import dataclass
from pathlib import Path
from enum import Enum
from tags.tag import Tag
import polars as pl

class User(Enum):
    JAMES = "James"
    CAT = "Cat"

class SourceType(Enum):
    CSV = "csv"
    HTML = "html"
    API = "api"
    PDF = "pdf"


@dataclass
class Source:
    name: str
    user: User
    source_type: SourceType
    tag: Tag
    key_columns: list[str] = None
    column_mapping: dict[str, str]  = None# {canonical_name: source_column_name}
    secrets: Optional[Secret] = None

    def resolve_secrets(self) -> None:
        manager = SecretManager()
    
        if self.secrets:
            for key in self.secrets.keys:
                key = manager.resolve(key=key)
    
    def resolve_column_mapping(self) -> dict:
        canonical_columns = list(self.tag.mandatory_columns.keys())
        source_columns = self.key_columns
    
        if len(source_columns) != len(canonical_columns):
            raise ValueError("Mismatch in number of source vs canonical columns")

        return {
            canonical: source
            for canonical, source in zip(canonical_columns, source_columns)
        }

    def cast_key_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Cast key columns to types defined in tag.mandatory_columns,
        using the column_mapping to map canonical names to source columns.
        Handles datetime columns explicitly.
        """
        for canonical_name, expected_dtype in self.tag.mandatory_columns.items():
            source_column = self.column_mapping.get(canonical_name)
            if source_column and source_column in df.columns:
                if expected_dtype == pl.Datetime:
                    formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y", "%Y.%m.%d"]
                    parsed = False
                    for fmt in formats:
                        try:
                            df = df.with_columns(
                                pl.col(source_column).str.strptime(pl.Datetime, fmt, strict=False).alias(source_column)
                            )
                            parsed = True
                            break
                        except Exception:
                            continue
                    if not parsed:
                        try:
                            df = df.with_columns(
                                pl.col(source_column).str.to_datetime().alias(source_column)
                            )
                        except Exception as e:
                            import logging
                            logging.error(f"Failed to parse '{source_column}' as datetime: {e}")
                            raise ValueError(f"Could not convert column '{source_column}' to datetime with known formats.")
                else:
                    df = df.with_columns(df[source_column].cast(expected_dtype))
        return df

    def validate(self, df: pl.DataFrame) -> None:
        """
        Delegate validation to Tag, using column_mapping.
        """
        self.tag.validate_dataframe(df, self.column_mapping)