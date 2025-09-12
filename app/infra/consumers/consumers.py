import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Self

from confluent_kafka import Consumer as KafkaConsumer, Message
from infra.consumers.settings import ConsumerSettings
from infra.topics import Topic

logger = logging.getLogger(__name__)


class ConsumerWrapper:
    def __init__(self, consumer: KafkaConsumer) -> None:
        self.__consumer: KafkaConsumer = consumer

    @property
    def consumer(self) -> KafkaConsumer:
        return self.__consumer

    @classmethod
    def get(cls, settings: ConsumerSettings) -> Self:
        consumer = KafkaConsumer(settings.to_representative())
        logger.info("Consumer started with id: %s, group: %s", settings.client_id, settings.group_id)
        return cls(consumer)

    def subscribe(self, topics: list[Topic]) -> None:
        self.consumer.subscribe(topics)
        logger.info("Subscribed to topics: %s", topics)

    def poll(self, timeout: float = 1.0) -> Message | None:
        return self.consumer.poll(timeout)

    def commit(self, message: Message) -> None:
        self.consumer.commit(message=message)

    def stop(self) -> None:
        logger.info("Stopping consumer...")
        self.consumer.close()
        logger.info("Consumer stopped.")


@contextmanager
def get_consumer(consumer_settings: ConsumerSettings) -> Generator[ConsumerWrapper, None, None]:
    consumer = ConsumerWrapper.get(settings=consumer_settings)
    try:
        yield consumer
    finally:
        consumer.stop()
