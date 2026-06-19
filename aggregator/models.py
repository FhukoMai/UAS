from sqlalchemy import Column, Integer, String, DateTime, JSON, UniqueConstraint, BigInteger
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class EventModel(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, nullable=False)
    event_id = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    source = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('topic', 'event_id', name='uq_topic_event_id'),
    )

class StatModel(Base):
    __tablename__ = "stats"
    id = Column(Integer, primary_key=True)
    received = Column(BigInteger, default=0)
    unique_processed = Column(BigInteger, default=0)
    duplicate_dropped = Column(BigInteger, default=0)
    start_time = Column(DateTime(timezone=True), default=datetime.utcnow)
