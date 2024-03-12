import asyncio
from dataclasses import dataclass
from typing import Any, Generic, Type, Union

from sqlalchemy import inspect, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.sql.elements import BinaryExpression
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from db_repository.types import T_SQLModel


@dataclass
class AsyncDBManager(Generic[T_SQLModel]):  # noqa: WPS214
    """Менеджер с асинхронной сессией."""

    model: Type[T_SQLModel]
    session: AsyncSession

    @property
    def primary_keys(self) -> list[str]:
        """Список названий столбцов - первичных ключей."""
        return [pk.name for pk in inspect(self.model).primary_key]

    async def bulk_delete_insert(self, entities: list[T_SQLModel]) -> None:
        """Удаляем данные из таблицы в БД по первичному ключу, затем выполняем вставку."""
        await self._delete_previous(entities)
        self.session.add_all(entities)
        try:
            await self.session.commit()
        except (StaleDataError, IntegrityError):
            await self.session.rollback()
            await self._delete_previous(entities)
            await self._load_one_by_one(entities)

    async def add_all(self, entities: list[T_SQLModel]) -> None:
        """Вставка в БД списка сущностей."""
        self.session.add_all(entities)
        await self.session.commit()

    async def get_by_primary_key(self, pk: Any) -> Union[T_SQLModel, None]:
        """Получаем одну строку из БД."""
        return await self.session.get(self.model, pk)

    async def get_all(
        self,
        *args: BinaryExpression,
        **kwargs: Any,
    ) -> list[T_SQLModel]:
        """Получаем одну строку."""
        statement = select(self.model).filter(*args).filter_by(**kwargs)
        return await (self.session.exec(statement)).all()  # type: ignore

    async def delete(self, entity: T_SQLModel) -> None:
        """Удаляем строку из БД."""
        await self.session.delete(entity)
        await self.session.commit()

    async def _delete_previous(self, entities: list[T_SQLModel]) -> None:
        """Удаляем записи, которые есть в текущей выборке."""
        items_pk_to_delete = [
            [getattr(db_item, pk) for pk in self.primary_keys] for db_item in entities
        ]
        items_to_delete = await self.session.exec(
            select(self.model).where(  # type: ignore
                tuple_(
                    *[col(getattr(self.model, pk)) for pk in self.primary_keys],
                ).in_(items_pk_to_delete),
            ),
        )
        await asyncio.gather(
            *[self.session.delete(db_item) for db_item in items_to_delete],
        )

    async def _load_one_by_one(self, entities: list[T_SQLModel]) -> None:
        """Загружаем сущности по одной."""

        async def _load_item(db_entity: T_SQLModel) -> None:
            self.session.add(db_entity)
            try:
                await self.session.commit()
            except (StaleDataError, IntegrityError):
                await self.session.rollback()

        await asyncio.gather(*[_load_item(db_item) for db_item in entities])
