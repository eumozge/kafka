import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Self

from confluent_kafka import KafkaError, Message, Producer as KafkaProducer
from infra.producers.events import IntegrationEvent
from infra.producers.settings import ProducerSettings
from infra.schemas.serializers import EventSchemaSerializer

logger = logging.getLogger(__name__)


class ProducerWrapper:
    def __init__(self, producer: KafkaProducer, event_schema_serializer: EventSchemaSerializer) -> None:
        self.__producer: KafkaProducer = producer
        self.__event_schema_serializer: EventSchemaSerializer = event_schema_serializer

    @property
    def producer(self) -> KafkaProducer:
        return self.__producer

    @property
    def event_schema_serializer(self) -> EventSchemaSerializer:
        return self.__event_schema_serializer

    @classmethod
    def get(cls, settings: ProducerSettings, event_schema_serializer: EventSchemaSerializer) -> Self:
        producer = KafkaProducer(settings.to_representative())
        message = f"Producer started with id: {settings.client_id}"
        logger.info(message)
        return cls(producer, event_schema_serializer=event_schema_serializer)

    def produce(
        self,
        intergration_event: IntegrationEvent,
        callback: Callable[[KafkaError, Message | None], None] | None = None,
    ) -> None:
        serialized_value = self.event_schema_serializer.serialize(intergration_event)
        self.producer.produce(
            topic=intergration_event.topic,
            key=intergration_event.key,
            value=serialized_value,
            callback=callback,
        )
        self.poll()

    def poll(self) -> None:
        self.producer.poll(0)

    def stop(self) -> None:
        logger.info("Stopping producer...")
        self.producer.flush(timeout=10)
        self.poll()
        logger.info("Producer stopped.")


@contextmanager
def get_producer(
    producer_settings: ProducerSettings,
    event_schema_serializer: EventSchemaSerializer,
) -> Generator[ProducerWrapper, None, None]:
    producer = ProducerWrapper.get(producer_settings, event_schema_serializer)
    try:
        yield producer
    finally:
        producer.stop()
