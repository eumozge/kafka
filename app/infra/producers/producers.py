import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Self

from confluent_kafka import KafkaError, Message, Producer as KafkaProducer
from infra.producers.settings import ProducerSettings
from infra.topics import Topic

logger = logging.getLogger(__name__)


class ProducerWrapper:
    def __init__(self, producer: KafkaProducer) -> None:
        self.__producer: KafkaProducer = producer

    @property
    def producer(self) -> KafkaProducer:
        return self.__producer

    @classmethod
    def get(cls, settings: ProducerSettings) -> Self:
        producer = KafkaProducer(settings.to_representative())
        logger.info("Producer started with id: %s", settings.client_id)
        return cls(producer)

    def produce(
        self,
        topic: Topic,
        key: str,
        serialized_value: bytes,
        callback: Callable[[KafkaError, Message | None], None] | None = None,
    ) -> None:
        self.producer.produce(
            topic=topic,
            key=key,
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
def get_producer(producer_settings: ProducerSettings) -> Generator[ProducerWrapper, None, None]:
    producer = ProducerWrapper.get(producer_settings)
    try:
        yield producer
    finally:
        producer.stop()
