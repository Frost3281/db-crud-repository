from dataclasses import dataclass
from typing import Any, Generic, Type, Union, Sequence

from sqlalchemy import inspect, tuple_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.sql.elements import BinaryExpression
from sqlmodel import Session, col, select

from db_repository.types import T_SQLModel


@dataclass
class DBManager(Generic[T_SQLModel]):
    """Базовый CRUD для SQLModel."""

    model: Type[T_SQLModel]
    session: Session

    @property
    def primary_keys(self) -> list[str]:
        """Список названий столбцов - первичных ключей."""
        return [pk.name for pk in inspect(self.model).primary_key]

    def bulk_delete_insert(self, entities: list[T_SQLModel]) -> None:
        """Удаляем данные из таблицы в БД по первичному ключу, затем выполняем вставку."""
        self._delete_previous(entities)
        self.session.add_all(entities)
        try:
            self.session.commit()
        except (StaleDataError, IntegrityError):
            self.session.rollback()
            self._delete_previous(entities)
            self._load_one_by_one(entities)

    def add_all(self, entities: list[T_SQLModel]) -> None:
        """Вставка в БД списка сущностей."""
        self.session.add_all(entities)
        self.session.commit()

    def get_by_primary_key(self, pk: Any) -> Union[T_SQLModel, None]:
        """Получаем одну строку из БД."""
        return self.session.get(self.model, pk)

    def get_all(
        self, *args: BinaryExpression, **kwargs: Any,
    ) -> Sequence[T_SQLModel]:
        """Получаем одну строку."""
        statement = select(self.model).filter(*args).filter_by(**kwargs)
        return self.session.exec(statement).all()

    def delete(self, entity: T_SQLModel) -> None:
        """Удаляем строку из БД."""
        self.session.delete(entity)
        self.session.commit()

    def _delete_previous(self, entities: list[T_SQLModel]) -> None:
        """Удаляем записи, которые есть в текущей выборке."""
        items_pk_to_delete = [
            [getattr(db_item, pk) for pk in self.primary_keys]
            for db_item in entities
        ]
        items_to_delete = self.session.exec(
            select(self.model).where(
                tuple_(
                    *[
                        col(getattr(self.model, pk))
                        for pk in self.primary_keys
                    ],
                ).in_(items_pk_to_delete),
            ),
        )
        for db_item in items_to_delete:
            self.session.delete(db_item)

    def _load_one_by_one(self, entities: list[T_SQLModel]) -> None:
        """Загружаем сущности по одной."""
        for db_item in entities:
            self.session.add(db_item)
            try:
                self.session.commit()
            except (StaleDataError, IntegrityError):
                self.session.rollback()
