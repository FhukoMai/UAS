import os
import json
import asyncio
import logging
from datetime import datetime
import redis.asyncio as redis
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from database import AsyncSessionLocal
from models import EventModel, StatModel

logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379/0")
redis_client = redis.from_url(BROKER_URL)

QUEUE_NAME = "events_queue"

async def process_event(session: AsyncSession, event_data: dict):
    stmt = insert(EventModel).values(
        topic=event_data["topic"],
        event_id=event_data["event_id"],
        timestamp=datetime.fromisoformat(event_data["timestamp"].replace("Z", "+00:00")) if isinstance(event_data["timestamp"], str) else event_data["timestamp"],
        source=event_data["source"],
        payload=event_data["payload"]
    ).on_conflict_do_nothing(
        index_elements=['topic', 'event_id']
    )
    
    result = await session.execute(stmt)
    await session.commit()
    
    inserted = result.rowcount > 0
    
    if inserted:
        logger.info(f"Inserted new event: {event_data['topic']} - {event_data['event_id']}")
        await session.execute(text("UPDATE stats SET unique_processed = unique_processed + 1 WHERE id = 1"))
    else:
        logger.info(f"Duplicate event dropped: {event_data['topic']} - {event_data['event_id']}")
        await session.execute(text("UPDATE stats SET duplicate_dropped = duplicate_dropped + 1 WHERE id = 1"))
    
    await session.commit()

async def worker_loop():
    logger.info("Worker started, waiting for events...")
    while True:
        try:
            result = await redis_client.blpop(QUEUE_NAME, timeout=1)
            if result:
                _, data = result
                event_data = json.loads(data)
                
                async with AsyncSessionLocal() as session:
                    await process_event(session, event_data)
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            await asyncio.sleep(1)

def start_worker():
    asyncio.create_task(worker_loop())
