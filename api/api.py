from fastapi import FastAPI, HTTPException
from typing import List, Optional
from sources.source_registry import SOURCES  # populated at runtime

app = FastAPI(title="Data Ingestor API")

#TODO: find out if response_model is needed
@app.get("/")
def root():
    return {"message": "Data Ingestor API running."}

@app.get("/sources")
def list_sources():
    return {"sources": [s.name for s in SOURCES]}

@app.get("/sources/{source_name}")
def get_metadata(source_name: str):
    for s in SOURCES:
        if s.name == source_name:
            s.column_mapping = s.resolve_column_mapping()
            return { 
                "name": s.name,
                "users": [s.user.name for s in SOURCES if s.name == source_name],
                "tag": s.tag.name,
                "column_mapping": s.column_mapping,
                "dimensions":s.dimensions,
                "hub": s.hub,
                "satellites": s.satellites,
            }
    raise HTTPException(status_code=404, detail=f"Source '{source_name}' not found.")

#TODO: create list in tag_registry.py
@app.get("/tags")
def get_tags():
    return list(set(s.tag.name for s in SOURCES))

@app.get("/users")
def get_users():
    return list(set(s.user.name for s in SOURCES))

#TODO: Handle 'Unprocessable content' error. 
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
