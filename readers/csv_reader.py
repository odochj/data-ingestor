import polars as pl
from pathlib import Path
from collections import defaultdict

def csv_reader(path: str) -> list[pl.DataFrame]:
    csv_payload = []
    schema_groups = defaultdict(list)

    if path:
        path = Path(path)

        # Gather all CSV files from root and subdirectories
        all_csv_files = list(path.glob("*.csv"))
        for subfolder in path.rglob("*"):
            if subfolder.is_dir():
                all_csv_files.extend(subfolder.glob("*.csv"))

        # Group files by schema (column names)
        for csv_file in all_csv_files:
            df = pl.read_csv(csv_file, n_rows=0)  # Read only header
            schema = tuple(df.columns)
            schema_groups[schema].append(csv_file)

        # Read and concatenate files with the same schema
        for files in schema_groups.values():
            dfs = [pl.read_csv(f) for f in files]
            csv_payload.append(pl.concat(dfs))

    else:
        raise ValueError("Path to CSV files is required")
    if not csv_payload:
        raise ValueError(f"No CSV files found under: {path}")
    return csv_payload