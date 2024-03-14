from typing import Annotated

from sqlmodel import SQLModel, Field


class EmployeeTest(SQLModel, table=True):
    """Тестовая модель."""

    name: Annotated[str, Field(primary_key=True)]
    gender: Annotated[str, Field(primary_key=True, default='male')]
    age: int
