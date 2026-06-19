import os
import json
import asyncio
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from contextlib import asynccontextmanager

from database import init_db, get_db
from models import EventModel, StatModel
from schemas import PublishRequest, StatsResponse
from worker import start_worker, redis_client, QUEUE_NAME

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    
    start_worker()
    
    yield
    
    await redis_client.aclose()

app = FastAPI(title="Pub-Sub Log Aggregator", lifespan=lifespan)

@app.post("/publish")
async def publish_events(request: PublishRequest, db: AsyncSession = Depends(get_db)):
    events = request.events
    if not events:
        return {"status": "ignored", "detail": "No events provided"}
    
    num_events = len(events)
    await db.execute(text(f"UPDATE stats SET received = received + {num_events} WHERE id = 1"))
    await db.commit()
    
    async with redis_client.pipeline() as pipe:
        for event in events:
            event_dict = event.model_dump()
            event_dict["timestamp"] = event_dict["timestamp"].isoformat()
            pipe.rpush(QUEUE_NAME, json.dumps(event_dict))
        await pipe.execute()
    
    return {"status": "success", "published": num_events}

@app.get("/events")
async def get_events(topic: str = Query(..., description="Topic to filter events by"), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EventModel).where(EventModel.topic == topic).order_by(EventModel.timestamp.desc()).limit(100))
    events = result.scalars().all()
    
    return {
        "topic": topic,
        "events": [
            {
                "event_id": e.event_id,
                "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                "source": e.source,
                "payload": e.payload
            } for e in events
        ]
    }

@app.get("/stats", response_model=StatsResponse)
async def get_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(StatModel).where(StatModel.id == 1))
    stat = result.scalar_one_or_none()
    
    if not stat:
        raise HTTPException(status_code=404, detail="Stats not initialized")
    
    topics_result = await db.execute(select(EventModel.topic).distinct())
    topics = [row[0] for row in topics_result.all()]
    
    uptime = (datetime.utcnow() - stat.start_time.replace(tzinfo=None)).total_seconds()
    
    return StatsResponse(
        received=stat.received,
        unique_processed=stat.unique_processed,
        duplicate_dropped=stat.duplicate_dropped,
        topics=topics,
        uptime_seconds=uptime
    )

@app.get("/health")
async def health_check():
    return {"status": "ok"}
