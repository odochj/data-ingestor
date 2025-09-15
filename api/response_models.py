from pydantic import BaseModel
from typing import List

class SourcesResponse(BaseModel):
    sources: List[str]