from typing import Iterator

import pytest
from sqlalchemy import StaticPool, text
from sqlmodel import Session, SQLModel, create_engine

engine = create_engine(
    'sqlite://',
    connect_args={'check_same_thread': False},
    poolclass=StaticPool,
    future=True,
)


@pytest.fixture(name='db_session', scope='session')
def db_session_fixture() -> Iterator[Session]:
    """Сессия для тестов."""
    with Session(engine) as session:
        session.execute(text("ATTACH DATABASE ':memory:' AS brl_apex;"))
        for table_metadata in SQLModel.metadata.tables.values():
            table_metadata.schema = 'brl_apex'
        SQLModel.metadata.create_all(engine)
        yield session
