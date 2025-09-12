from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Self, TypeAlias

import orjson
from utils import default_serializer

EventRepresentation: TypeAlias = dict


@dataclass(frozen=True)
class BaseEvent(ABC):
    def to_representative(self) -> EventRepresentation:
        return orjson.loads(orjson.dumps(asdict(self), default=default_serializer))

    @classmethod
    @abstractmethod
    def from_representation(cls, payload: EventRepresentation) -> Self: ...
