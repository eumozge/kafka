import uuid
from abc import ABC
from dataclasses import asdict, dataclass, field
from typing import Self, TypeAlias

import orjson
from domain.common import events as domain_events
from domain.schemas import SchemaSubjectName
from infra.topics import Topic
from utils import default_serializer

EventMetedataPayload: TypeAlias = dict
IntegrationEventPayload: TypeAlias = dict


@dataclass(frozen=True, eq=False, kw_only=True)
class EventMetadata:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    schema: SchemaSubjectName
    version: str

    @classmethod
    def from_representative(cls, payload: EventMetedataPayload) -> Self:
        return cls(id=payload["id"], schema=payload["schema"], version=payload["version"])

    def to_representative(self) -> EventMetedataPayload:
        return asdict(self)


@dataclass(frozen=True, kw_only=True)
class IntegrationEvent(ABC):
    topic: Topic
    key: str
    metadata: EventMetadata
    domain_event: domain_events.BaseEvent

    def to_representative(self) -> IntegrationEventPayload:
        return {
            "metadata": self.metadata.to_representative(),
            "payload": self.domain_event.to_representative(),
        }

    def encode(self) -> bytes:
        return orjson.dumps(self.to_representative(), default=default_serializer)
