import os
import pytest
import polars as pl
from writers.postgres_writer import PostgresWriter

try:
    from secret_handling.secret import Secret, SecretType
except Exception:
    Secret = None


@pytest.mark.skipif(os.environ.get("POSTGRES_URL") is None, reason="POSTGRES_URL not set")
def test_postgres_writer_roundtrip():
    # Requires POSTGRES_URL env var
    conn_str = os.environ["POSTGRES_URL"]

    secrets = Secret({SecretType.DB_CONNECTION: conn_str})
    writer = PostgresWriter(secrets)

    df = pl.DataFrame({
        "pk": ["p1", "p2"],
        "value": [1, 2]
    })

    class _Source:
        def __init__(self):
            self.name = "testsrc"
            class Tag: name = "testtag"
            self.tag = Tag()
            self.user = None
            self.satellites = set()
            self.hub = None

    src = _Source()
    writer.write_scd2(df, src)

    # If we reach this point without exception, basic write succeeded
    assert src.hub is not None
    assert len(src.satellites) >= 1
