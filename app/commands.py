import logging
from time import sleep

import click
from domain import payments
from domain.common import registry as event_registry
from infra.producers.callbacks import delivery_logging
from infra.producers.events import EventMetadata, IntegrationEvent
from infra.producers.producers import get_producer
from infra.producers.settings import ProducerSettings
from infra.schemas.schemas import get_schemas_registry_client, load_schemas
from infra.schemas.serializers import get_event_schema_serializer
from infra.schemas.settings import SchemaRegisterSettings
from infra.topics import Topic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def cli() -> None: ...


@cli.command()
@click.option(
    "--message-delay",
    "-d",
    type=click.IntRange(0, 3_000),
    default=100,
    help="Time between messages in microseconds.",
)
@click.option(
    "--message-count",
    "-m",
    type=click.INT,
    default=100,
    help="Total producing messages.",
)
@click.option(
    "--log-throttling",
    "-lt",
    type=click.INT,
    default=10,
    help="Throttling for message callback logs.",
)
def start(message_delay: int, message_count: int, log_throttling: int) -> None:
    logger.info("Starting producers...")
    schema_registry = get_schemas_registry_client(settings=SchemaRegisterSettings())
    load_schemas(schema_registry)
    logger.info("Start producers in 3 secods.")
    sleep(3)

    event_schema_serializer = get_event_schema_serializer(
        schema_registry=schema_registry,
        event_registry=event_registry,
    )

    with get_producer(
        producer_settings=ProducerSettings(),
        event_schema_serializer=event_schema_serializer,
    ) as producer:
        try:
            for i in range(message_count):
                domain_event = payments.get_random_event()
                integraion_event = IntegrationEvent(
                    topic=Topic.PAYMENT,
                    key=domain_event.username,
                    metadata=EventMetadata(version="1.0"),
                    domain_event=domain_event,
                )
                producer.produce(
                    integraion_event,
                    callback=lambda error, message, message_numer=i: delivery_logging(
                        error,
                        message,
                        message_number=message_numer,
                        throttling=log_throttling,
                    ),
                )
                sleep(message_delay / 1000)
        except KeyboardInterrupt:
            logger.info("Producer interrupted.")
        except Exception:
            logger.exception("Unexpected error.")
