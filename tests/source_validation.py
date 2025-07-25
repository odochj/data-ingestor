import polars as pl
from tags.tag import Tag
from sources.source import Source

# Define Tag
transactions_tag = Tag(
    name="transactions",
    mandatory_columns={
        "timestamp": pl.Datetime,
        "amount": pl.Float64,
        "description": pl.Utf8,
    }
)

# Define Source
superbank = Source(
    name="superbank",
    tag=transactions_tag,
    column_mapping={
        "timestamp": "txn_date",
        "amount": "value",
        "description": "merchant_desc"
    }
)

# Raw source DataFrame
df = pl.DataFrame({
    "txn_date": [pl.datetime(2024, 1, 1)],
    "value": [25.50],
    "merchant_desc": ["Cafe Nero"]
})

# Just validate, no transformation
superbank.validate(df)
