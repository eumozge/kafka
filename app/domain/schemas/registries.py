from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum

from domain.common.events import BaseEvent
from patterns import Singleton


class SchemaSubjectName(StrEnum):
    PAYMENT_TRANSACTION = "payments.transaction"


@dataclass
class SchemaRegistry(metaclass=Singleton):
    __schemas: dict[type[BaseEvent], SchemaSubjectName] = field(default_factory=dict)

    @property
    def schemas(self) -> dict[type[BaseEvent], SchemaSubjectName]:
        return self.__schemas

    def register(self, schema_subject_name: SchemaSubjectName) -> Callable[[type[BaseEvent]], type[BaseEvent]]:
        def wrapper(cls: type[BaseEvent]) -> type[BaseEvent]:
            self.schemas[cls] = schema_subject_name
            return cls

        return wrapper

    def get_schema(self, event: BaseEvent) -> SchemaSubjectName:
        if type(event) not in self.schemas:
            raise ValueError(event)
        return self.schemas[type(event)]

    def get_class(self, schema_subject_name: SchemaSubjectName) -> BaseEvent:
        for cls, name in self.schemas.items():
            if name == schema_subject_name:
                return cls
        raise ValueError(schema_subject_name)


registry = SchemaRegistry()
