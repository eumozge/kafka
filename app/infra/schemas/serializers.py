from confluent_kafka import Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from domain.common import events as domain_events
from domain.schemas.registries import SchemaRegistry
from infra.events import EventMetadata, IntegrationEvent
from patterns import Singleton


class EventSchemaSerializer(metaclass=Singleton):
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

    def serialize(self, intergration_event: IntegrationEvent) -> bytes:
        serializer = self.get_serializer(intergration_event.domain_event)
        return serializer(
            intergration_event.to_representative(),
            SerializationContext(intergration_event.topic, MessageField.VALUE),
        )

    def get_deserializer(self) -> AvroDeserializer:
        return AvroDeserializer(self.client)

    def deserialize(self, message: Message) -> IntegrationEvent:
        deserializer = self.get_deserializer()
        context = SerializationContext(message.topic(), MessageField.VALUE)
        value = deserializer(message.value(), context)
        metadata = EventMetadata.from_representative(payload=value["metadata"])
        event_class = self.schema_registry.get_class(metadata.schema)
        domain_event = event_class.from_representation(payload=value["payload"])
        return IntegrationEvent(
            topic=message.topic(),
            key=message.key().decode("utf-8"),
            metadata=metadata,
            domain_event=domain_event,
        )


def get_event_schema_serializer(
    schema_registry: SchemaRegistryClient,
    event_registry: SchemaRegistry,
) -> EventSchemaSerializer:
    return EventSchemaSerializer(schema_registry, event_registry)
