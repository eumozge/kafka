import logging
from time import sleep

import click
import orjson
from domain.schemas.registries import registry as event_registry
from infra.consumers.consumers import get_consumer
from infra.consumers.settings import ConsumerSettings
from infra.schemas.schemas import get_schemas_registry_client, load_schemas
from infra.schemas.serializers import EventSerializerType, get_event_serializer
from infra.schemas.settings import SchemaRegisterSettings
from infra.topics import Topic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def consumers() -> None: ...


@consumers.command()
@click.option(
    "--message-delay",
    "-d",
    type=click.IntRange(0, 5_000),
    default=100,
    help="Time for message processing in microseconds.",
)
@click.option(
    "--group",
    "-g",
    type=str,
    default="default",
    help="Consumer group id.",
)
@click.option(
    "--serializer",
    "-s",
    type=click.Choice([str(val) for val in EventSerializerType]),
    default=str(EventSerializerType.JSON),
    help="Type of evetn serializer",
)
@click.option(
    "--log-throttling",
    "-lt",
    type=click.INT,
    default=1,
    help="Throttling for message callback logs.",
)
def start(message_delay: int, group: str, serializer: EventSerializerType, log_throttling: int) -> None:
    logger.info("Starting counsumers...")
    schema_registry = get_schemas_registry_client(settings=SchemaRegisterSettings())
    load_schemas(schema_registry)

    event_schema_serializer = get_event_serializer(
        event_serializer_type=EventSerializerType(serializer),
        schema_registry_client=schema_registry,
        schema_registry=event_registry,
    )

    logger.info("Start counsumers in 3 secods.")
    sleep(3)

    with get_consumer(consumer_settings=ConsumerSettings(group_id=group)) as consumer:
        try:
            consumer.subscribe([Topic.PAYMENT_TRANSACTION])
            message_number = 0
            while True:
                message = consumer.poll()
                if message is None:
                    continue

                message_number += 1

                integration_event = event_schema_serializer.to_integration_event(message)
                if not message_number % log_throttling:
                    logger.info(
                        "Message `%s` from `%s` received succesed:\n %s",
                        f"{message_number:>06}",
                        message.topic(),
                        orjson.dumps(integration_event.to_representative(), option=orjson.OPT_INDENT_2).decode("utf-8"),
                    )
                consumer.commit(message)
                sleep(message_delay / 1000)
        except KeyboardInterrupt:
            logger.info("Producer interrupted.")
        except Exception:
            logger.exception("Unexpected error.")
