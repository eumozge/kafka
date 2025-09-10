import logging

from confluent_kafka import KafkaError, Message

logger = logging.getLogger(__name__)


def delivery_logging(error: KafkaError | None, message: Message, message_number: int, throttling: int = 10) -> None:
    if message_number % throttling:
        return

    if error:
        message = f"Message `{message_number:>06}` to `{message.topic()}` delivery failed: {error.str()}"
        logger.error(message)
    else:
        message = f"Message `{message_number:>06}` to `{message.topic()}` delivery succesed."
        logger.info(message)
