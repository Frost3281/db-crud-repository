from typing import Iterator, AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import StaticPool, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.ext.asyncio.session import AsyncSession

engine = create_engine(
    'sqlite://',
    connect_args={'check_same_thread': False},
    poolclass=StaticPool,
    future=True,
    echo=True,
)


@pytest.fixture(name='db_session')
def db_session_fixture() -> Iterator[Session]:
    """Сессия для тестов."""
    with Session(engine) as session:
        session.execute(text("ATTACH DATABASE ':memory:' AS brl_apex;"))
        for table_metadata in SQLModel.metadata.tables.values():
            table_metadata.schema = 'brl_apex'
        SQLModel.metadata.create_all(engine)
        yield session
    engine.dispose()


def get_async_engine() -> AsyncEngine:
    """Возвращает асинхронный движок."""
    return create_async_engine(
        'sqlite+aiosqlite://',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool,
        future=True,
        echo=True,
    )


async_engine = get_async_engine()


@pytest_asyncio.fixture(name='async_session')
async def async_session_fixture() -> AsyncIterator[AsyncSession]:
    """Фикстура асинхронной сессии sqlite in-memory."""
    engine_async = get_async_engine()
    async with AsyncSession(engine_async) as session:
        await session.execute(text("ATTACH DATABASE ':memory:' AS brl_apex;"))
        for table_metadata in SQLModel.metadata.tables.values():
            table_metadata.schema = 'brl_apex'
        async with engine_async.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        yield session
    await engine_async.dispose()
