from fastapi import FastAPI, HTTPException
from typing import List, Optional
from sources.source_registry import SOURCES  # populated at runtime
from api.response_models import SourcesResponse

app = FastAPI(title="Data Ingestor API")


@app.get("/")
def root():
    return {"message": "Data Ingestor API running."}


@app.get("/facts")
def get_facts():
    """Return mapping of hub -> list of fact (dimension) names in a JSON-safe shape.

    Groups dimensions across sources per hub and returns lists (no sets/Enums).
    """
    facts = {}
    for s in SOURCES:
        if not getattr(s, "hub", None):
            # hub may be unset until a source has been written; skip these
            continue

        # collect canonical dimension names, converting Enum keys to their value
        dim_keys = [(k.value if hasattr(k, "value") else str(k)) for k in s.dimensions.keys()]
        facts.setdefault(s.hub, set()).update(dim_keys)

    # convert sets to sorted lists for deterministic JSON
    facts = {hub: sorted(list(cols)) for hub, cols in facts.items()}
    return {"facts": facts}


@app.get("/sources", response_model=SourcesResponse)
def list_sources():
    return {"sources": [s.name for s in SOURCES]}


@app.get("/sources/{source_name}")
def get_metadata(source_name: str):
    for s in SOURCES:
        if s.name == source_name:
            if hasattr(s, "resolve_column_mapping"):
                try:
                    s.column_mapping = s.resolve_column_mapping()
                except Exception:
                    s.column_mapping = getattr(s, "column_mapping", {})

            return {
                "source": {
                    "name": s.name,
                    "users": [getattr(s.user, "value", str(getattr(s, "user", "")))],
                    "tag": getattr(s.tag, "name", str(getattr(s, "tag", ""))),
                    "column_mapping": getattr(s, "column_mapping", {}),
                    "dimensions": {
                        (k.value if hasattr(k, "value") else str(k)): v
                        for k, v in getattr(s, "dimensions", {}).items()
                    },
                    "hub": getattr(s, "hub", None),
                    "satellites": [list(getattr(s, "satellites", [])), getattr(s, "column_mapping", {})],
                }
            }

    raise HTTPException(status_code=404, detail=f"Source '{source_name}' not found.")


@app.get("/tags")
def get_tags():
    return {"tags": list(set(getattr(s, "tag").name for s in SOURCES if getattr(s, "tag", None)))}


@app.get("/users")
def get_users():
    return {"users": list(set(getattr(s, "user").name for s in SOURCES if getattr(s, "user", None)))}


@app.post("/run")
def run_ingestion(sources: Optional[List[str]] = None):
    from pipeline import run_pipeline

    if sources:
        sources = [s.lower() for s in sources]
        matched = [s for s in SOURCES if s.name.lower() in sources]
    else:
        matched = SOURCES

    for s in matched:
        run_pipeline(s)

    return {"status": "success", "sources_run": [s.name for s in matched]}
