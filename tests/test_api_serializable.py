from fastapi.testclient import TestClient
from api.api import app

client = TestClient(app)


def test_list_sources_serializable():
    resp = client.get("/sources")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "sources" in data
    assert isinstance(data["sources"], list)
    assert all(isinstance(s, str) for s in data["sources"]) if data["sources"] else True


def test_source_metadata_serializable():
    # pick a source that exists in the registry
    resp = client.get("/sources/Monzo")
    assert resp.status_code == 200
    data = resp.json()
    assert "source" in data
    src = data["source"]
    assert isinstance(src, dict)

    # satellites should be a list with [list_of_satellites, column_mapping]
    satellites = src.get("satellites")
    assert isinstance(satellites, list)
    assert len(satellites) == 2
    sat_list, col_map = satellites
    assert isinstance(sat_list, list)
    assert isinstance(col_map, dict)
    assert all(isinstance(s, str) for s in sat_list) if sat_list else True

    # dimensions keys should be strings and values should be strings
    dimensions = src.get("dimensions")
    assert isinstance(dimensions, dict)
    assert all(isinstance(k, str) for k in dimensions.keys())
    assert all(isinstance(v, str) for v in dimensions.values()) if dimensions else True


def test_facts_serializable():
    resp = client.get("/facts")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    facts = data.get("facts")
    assert isinstance(facts, dict)
    for hub, cols in facts.items():
        assert isinstance(hub, str)
        assert isinstance(cols, list)
        assert all(isinstance(c, str) for c in cols)
