import os
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from models import Base, StatModel

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:pass@storage:5432/db")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with AsyncSessionLocal() as session:
        stat = await session.get(StatModel, 1)
        if not stat:
            new_stat = StatModel(id=1)
            session.add(new_stat)
            await session.commit()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
