import polars as pl
import logging

COMMON_DATETIME_FORMATS = [
"%Y-%m-%d",
"%d/%m/%Y",
"%m/%d/%Y",
"%Y/%m/%d",
"%d-%m-%Y",
"%Y.%m.%d",
"%Y-%m-%d %H:%M:%S",
"%d/%m/%Y %H:%M:%S",
"%m/%d/%Y %H:%M:%S",
]
def try_parse_datetime_column(df: pl.DataFrame, column: str) -> pl.Series:
    """
    Attempt to parse a column to pl.Datetime using known formats.
    Raises ValueError if none match.
    """
    for fmt in COMMON_DATETIME_FORMATS:
        try:
            parsed = df.select(
                pl.col(column).str.strptime(pl.Datetime, fmt, strict=True).alias(column)
            )[column]
            if parsed.null_count() < parsed.len():
                logging.info(f"Successfully parsed '{column}' with format '{fmt}'")
                return parsed
        except Exception:
            continue

    logging.error(f"Failed to parse datetime column '{column}' with all known formats")
    raise ValueError(f"Could not parse datetime column '{column}'")