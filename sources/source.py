from typing import Optional
from secret_handling.secret import Secret
from secret_handling.manager import SecretManager
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum
from tags.tag import Tag
from sources.datetime_helper import try_parse_datetime_column
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
    key_columns: list[str]
    secrets: Secret
    #generated at runtime:
    satellites: set[str] = field(default_factory=set)
    column_mapping: dict[str, str]  = field(default_factory=dict) # {canonical_name: source_column_name}
    hub: Optional[str] = None

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
        """
        for canonical_name, expected_dtype in self.tag.mandatory_columns.items():
            source_column = self.column_mapping.get(canonical_name)
            if not source_column or source_column not in df.columns:
                continue

            try:
                if expected_dtype == pl.Datetime:
                    parsed_column = try_parse_datetime_column(df, source_column)
                    df = df.with_columns(parsed_column)
                else:
                    df = df.with_columns(
                        pl.col(source_column).cast(expected_dtype)
                    )
            except Exception as e:
                print(f"Failed to cast '{source_column}' to {expected_dtype}: {e}")
                raise

        return df

    def validate(self, df: pl.DataFrame) -> None:
        """
        Delegate validation to Tag, using column_mapping.
        """
        self.tag.validate_dataframe(df, self.column_mapping)