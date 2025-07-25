from sources.source_registry import SOURCES
from sources.source import Source
from readers.reader_factory import ReaderFactory
from writers.duckdb_writer import DuckDBWriter as db
from secret_handling.manager import SecretManager

def run_pipeline(source: Source):
    print(f"\nProcessing source: {source.name}")
    
    # 1. Resolve secrets
    source.resolve_secrets()

    # 2. Read data
    dfs = ReaderFactory.get_reader(source)
    print(f"Sample data shape: {dfs[0].shape}")

    # 3. Map Columns 
    source.column_mapping = source.resolve_column_mapping()
    
    for df in dfs:
        # 4. Cast columns
        df = source.cast_key_columns(df)

        # 5. Validate
        source.validate(df)

        
        # 6. Generate primary key
        df = source.tag.generate_primary_key(
            df, 
            source.key_columns
            )
        # 7. Write to DuckDB
        db.write_scd2(
            df = df,
            source_name = source.name,
            tag = source.tag.name
        )

        print(f"Written to table: {source.tag.name}")

if __name__ == "__main__":
    for source in SOURCES:
        run_pipeline(source)
