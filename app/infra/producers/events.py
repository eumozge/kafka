import uuid
from abc import ABC
from dataclasses import asdict, dataclass, field

import orjson
from domain.common import events as domain_events
from infra.topics import Topic
from utils import default_serializer


@dataclass(frozen=True, eq=False, kw_only=True)
class EventMetadata:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str

    def to_representative(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, kw_only=True)
class IntegrationEvent(ABC):
    topic: Topic
    key: str
    metadata: EventMetadata
    domain_event: domain_events.Event

    def to_representative(self) -> dict:
        return {
            "metadata": self.metadata.to_representative(),
            "payload": self.domain_event.to_representative(),
        }

    def encode(self) -> bytes:
        return orjson.dumps(self, default=default_serializer)
