
from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, List, Literal
import time

Platform = Literal["twitter","reddit","facebook"]

class Lead(BaseModel):
    platform: Platform
    source_url: HttpUrl
    canonical_id: str = Field(..., description="e.g., @handle without @")
    name: Optional[str] = None
    bio_or_desc: Optional[str] = None
    emails: List[str] = []
    phones: List[str] = []
    followers: Optional[int] = None
    extra: dict = {}
    scraped_at: int = Field(default_factory=lambda: int(time.time()))
