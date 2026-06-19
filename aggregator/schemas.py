from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Any, List, Optional

class EventPayload(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

class PublishRequest(BaseModel):
    events: List[EventPayload]

class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime_seconds: float
