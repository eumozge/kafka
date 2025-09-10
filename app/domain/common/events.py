from abc import ABC
from collections.abc import Callable
from dataclasses import asdict, dataclass, field

import orjson
from patterns import Singleton
from utils import default_serializer


@dataclass(frozen=True)
class Event(ABC):
    def to_representative(self) -> dict:
        return orjson.loads(orjson.dumps(asdict(self), default=default_serializer))


@dataclass
class EventRegistry(metaclass=Singleton):
    __registry: dict[type[Event], str] = field(default_factory=dict)

    def __contains__(self, item: Event) -> bool:
        return type(item) in self.registry

    def __getitem__(self, item: Event) -> str:
        assert item in self, item
        return self.registry[type(item)]

    @property
    def registry(self) -> dict[type[Event], str]:
        return self.__registry

    def register(self, schema_subject_name: str) -> Callable[[type[Event]], type[Event]]:
        def wrapper(cls: type[Event]) -> type[Event]:
            self.registry[cls] = schema_subject_name
            return cls

        return wrapper


registry = EventRegistry()
