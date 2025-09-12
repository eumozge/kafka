import logging

from confluent_kafka import KafkaError, Message

logger = logging.getLogger(__name__)


def delivery_logging(error: KafkaError | None, message: Message, message_number: int, throttling: int = 10) -> None:
    if message_number % throttling:
        return

    if error:
        logger.error("Message `%s` to `%s` delivery failed: %", f"{message_number:>06}", message.topic(), error.str())
    else:
        logger.info("Message `%s` to `%s` delivery succeed.", f"{message_number:>06}", message.topic())
