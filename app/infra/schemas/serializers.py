from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from domain.common import events as domain_events
from infra.producers.events import IntegrationEvent
from patterns import Singleton


class EventSchemaSerializer(metaclass=Singleton):
    def __init__(self, schema_registry: SchemaRegistryClient, event_registry: domain_events.EventRegistry):
        self.__schema_registry = schema_registry
        self.__event_registry = event_registry
        self.__cache: dict[str, AvroSerializer] = {}

    @property
    def schema_registry(self) -> SchemaRegistryClient:
        return self.__schema_registry

    @property
    def event_registry(self) -> domain_events.EventRegistry:
        return self.__event_registry

    @property
    def cache(self) -> dict[str, AvroSerializer]:
        return self.__cache

    def get_serializer(self, domain_event: domain_events.Event) -> AvroSerializer:
        subject_name = self.event_registry[domain_event]

        if subject_name not in self.cache:
            latest_schema = self.schema_registry.get_latest_version(subject_name)
            serializer = AvroSerializer(
                schema_registry_client=self.schema_registry,
                schema_str=latest_schema.schema.schema_str,
            )
            self.cache[subject_name] = serializer
        return self.cache[subject_name]

    def serialize(self, intergration_event: IntegrationEvent) -> bytes:
        serializer = self.get_serializer(intergration_event.domain_event)
        return serializer(
            intergration_event.to_representative(),
            SerializationContext(intergration_event.topic, MessageField.VALUE),
        )


def get_event_schema_serializer(
    schema_registry: SchemaRegistryClient,
    event_registry: domain_events.EventRegistry,
) -> EventSchemaSerializer:
    return EventSchemaSerializer(schema_registry, event_registry)
