from sqlalchemy.ext.asyncio import create_async_engine,  AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from app.config import settings


engine = create_async_engine(settings.database_url, echo=False)
SessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()


async def get_session() -> AsyncSession:
    """Асинхронный генератор сессии базы данных."""
    async with SessionLocal() as session:
        yield session
