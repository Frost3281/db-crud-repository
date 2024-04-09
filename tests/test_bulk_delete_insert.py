import pytest
from sqlmodel import select, Session
from sqlmodel.ext.asyncio.session import AsyncSession

from db_repository.async_base_repository import AsyncDBManager
from db_repository.base_repository import DBManager
from tests.models import EmployeeTest


@pytest.mark.asyncio()
async def test_async_bulk_delete_insert(async_session: AsyncSession):
    """Тестируем асинхронный delete-insert."""
    async_session.add_all(
        [
            EmployeeTest(name='Vitaliy', age=30),
            EmployeeTest(name='Igor', age=31),
            EmployeeTest(name='Alexey', age=32),
        ],
    )
    await AsyncDBManager(EmployeeTest, async_session).bulk_delete_insert(
        [
            EmployeeTest(name='Vitaliy', age=30),
            EmployeeTest(name='Andrey', age=31),
            EmployeeTest(name='Jack', age=32),
        ],
    )
    employees = (await async_session.exec(select(EmployeeTest))).all()
    assert len(employees) == 5
    assert employees[0].name == 'Vitaliy'
    assert employees[0].age == 30
    assert employees[1].name == 'Igor'
    assert employees[1].age == 31
    assert employees[2].name == 'Alexey'
    assert employees[2].age == 32
    assert employees[3].name == 'Andrey'
    assert employees[3].age == 31
    assert employees[4].name == 'Jack'
    assert employees[4].age == 32


def test_delete_insert(db_session: Session):
    """Тестируем синхронный delete-insert."""
    db_manager = DBManager(EmployeeTest, db_session)
    assert not db_manager._has_relationships
    DBManager(EmployeeTest, db_session).bulk_delete_insert(
        [
            EmployeeTest(name='Vitaliy', age=30),
            EmployeeTest(name='Andrey', age=31),
            EmployeeTest(name='Jack', age=32),
        ],
    )
    employees = db_session.exec(select(EmployeeTest)).all()
    assert len(employees) == 3
    assert employees[0].name == 'Vitaliy'
    assert employees[0].age == 30
    assert employees[1].name == 'Andrey'
    assert employees[1].age == 31
    assert employees[2].name == 'Jack'
    assert employees[2].age == 32
