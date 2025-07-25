from tags.tag import Tag
import polars as pl

Transactions = Tag(
    name="transactions",
    mandatory_columns={
        "timestamp": pl.Datetime,
        "amount": pl.Float64,
        "description": pl.Utf8,
    }
)
