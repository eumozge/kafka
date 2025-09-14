import logging
from abc import ABC, abstractmethod
from enum import StrEnum, auto

import orjson
from confluent_kafka import Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from domain.common import events as domain_events
from domain.schemas.registries import SchemaRegistry
from infra.events import EventMetadata, IntegrationEvent
from patterns import Singleton

__all__ = ("EventSerializerType", "get_event_serializer")

logger = logging.getLogger(__name__)


class EventSerializerType(StrEnum):
    JSON = auto()
    AVRO = auto()


class BaseEventSerializer(ABC, Singleton):
    @abstractmethod
    def to_bytes(self, intergration_event: IntegrationEvent) -> bytes: ...

    @abstractmethod
    def parse(self, message: Message) -> dict: ...

    def to_integration_event(self, message: Message) -> IntegrationEvent:
        """TODO Add error handling if there are wrong formats."""
        serialized_value = self.parse(message)
        metadata = EventMetadata.from_representative(payload=serialized_value["metadata"])
        event_class = self.schema_registry.get_class(metadata.schema)
        domain_event = event_class.from_representation(payload=serialized_value["payload"])
        return IntegrationEvent(
            topic=message.topic(),
            key=message.key().decode("utf-8"),
            metadata=metadata,
            domain_event=domain_event,
        )


class JSONEventSerializer(BaseEventSerializer):
    def __init__(self, schema_registry: SchemaRegistry):
        self.__schema_registry = schema_registry

    @property
    def schema_registry(self) -> SchemaRegistry:
        return self.__schema_registry

    def to_bytes(self, intergration_event: IntegrationEvent) -> bytes:
        return intergration_event.encode()

    def parse(self, message: Message) -> dict:
        return orjson.loads(message.value().decode("utf-8"))


class AvroEventSerializer(BaseEventSerializer):
    def __init__(self, schema_registry_client: SchemaRegistryClient, schema_registry: SchemaRegistry):
        self.__schema_registry_client = schema_registry_client
        self.__schema_registry = schema_registry
        self.__cache: dict[str, AvroSerializer] = {}

    @property
    def client(self) -> SchemaRegistryClient:
        return self.__schema_registry_client

    @property
    def schema_registry(self) -> SchemaRegistry:
        return self.__schema_registry

    @property
    def cache(self) -> dict[str, AvroSerializer]:
        return self.__cache

    def get_serializer(self, domain_event: domain_events.BaseEvent) -> AvroSerializer:
        schema_subject_name = self.schema_registry.get_schema(domain_event)

        if schema_subject_name not in self.cache:
            latest_schema = self.client.get_latest_version(schema_subject_name)
            serializer = AvroSerializer(
                schema_registry_client=self.client,
                schema_str=latest_schema.schema.schema_str,
            )
            self.cache[schema_subject_name] = serializer
        return self.cache[schema_subject_name]

    def to_bytes(self, intergration_event: IntegrationEvent) -> bytes:
        serializer = self.get_serializer(intergration_event.domain_event)
        return serializer(
            intergration_event.to_representative(),
            SerializationContext(intergration_event.topic, MessageField.VALUE),
        )

    def get_deserializer(self) -> AvroDeserializer:
        return AvroDeserializer(self.client)

    def parse(self, message: Message) -> dict:
        deserializer = self.get_deserializer()
        context = SerializationContext(message.topic(), MessageField.VALUE)
        return deserializer(message.value(), context)


def get_json_event_serializer(
    schema_registry: SchemaRegistry,
) -> JSONEventSerializer:
    logger.info("Get JSON serializer, Avro schema not used.")
    return JSONEventSerializer(schema_registry=schema_registry)


def get_avro_event_serializer(
    schema_registry_client: SchemaRegistryClient,
    schema_registry: SchemaRegistry,
) -> AvroEventSerializer:
    logger.info("Get Avro serializer, Avro schema used")
    return AvroEventSerializer(schema_registry_client=schema_registry_client, schema_registry=schema_registry)


def get_event_serializer(
    event_serializer_type: EventSerializerType,
    schema_registry_client: SchemaRegistryClient,
    schema_registry: SchemaRegistry,
) -> BaseEventSerializer:
    match event_serializer_type:
        case EventSerializerType.JSON:
            return get_json_event_serializer(schema_registry=schema_registry)
        case EventSerializerType.AVRO:
            return get_avro_event_serializer(
                schema_registry_client=schema_registry_client,
                schema_registry=schema_registry,
            )
        case _:
            raise ValueError(event_serializer_type)
