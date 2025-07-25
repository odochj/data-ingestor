import os
import pytest
import duckdb
import pandas as pd
from pipeline import run_pipeline
from sources.source_registry import TEST_SOURCES
from secret_handling.secret import Secret, SecretType

@pytest.fixture
def clean_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    return db_path

@pytest.fixture
def clean_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    # Set environment variable for the test
    os.environ["DUCKDB_PATH"] = str(db_path)
    return db_path

@pytest.fixture
def test_source():
    return next(s for s in TEST_SOURCES)

def test_pipeline_full_write(test_source, clean_db):
    # Pass the secret to DuckDBWriter
    from writers.duckdb_writer import DuckDBWriter
    secrets = Secret(keys={"DUCKDB_PATH": SecretType.FILE_PATH})
    db = DuckDBWriter(secrets=secrets)

    # Patch the global db in pipeline if needed
    import pipeline
    pipeline.db = db

    run_pipeline(test_source)

    con = duckdb.connect(str(clean_db))

    # Check hub table
    hub_table = f"hub_{test_source.tag.name}"
    hub_df = con.execute(f"SELECT * FROM {hub_table}").df()
    assert len(hub_df) == len(hub_df["pk"].unique()), "Primary keys are not unique in the hub"

    # Find all satellites for this tag+source
    satellite_tables = con.execute("SHOW TABLES").df()
    satellites = [
        t for t in satellite_tables["name"]
        if t.startswith(f"sat_{test_source.tag.name}_{test_source.name}")
    ]

    # There should be only one satellite table per schema
    assert len(satellites) == 1, "More than one satellite created unexpectedly"

    sat_df = con.execute(f"SELECT * FROM {satellites[0]}").df()
    assert "row_hash" in sat_df.columns
    assert sat_df.shape[0] > 0, "Satellite table is empty"

    con.close()
