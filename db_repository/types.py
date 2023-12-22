from typing import TypeVar

from sqlmodel import SQLModel

T_SQLModel = TypeVar('T_SQLModel', bound=SQLModel)
