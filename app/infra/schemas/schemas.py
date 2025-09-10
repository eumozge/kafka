import logging
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from infra.schemas.settings import SchemaRegisterSettings

logger = logging.getLogger(__name__)


def get_schemas_registry_client(settings: SchemaRegisterSettings) -> SchemaRegistryClient:
    return SchemaRegistryClient(settings.to_representative())


def load_schemas(schema_register: SchemaRegistryClient) -> None:
    logger.info("Loading schemas...")
    directory = Path(Path(__file__).parent.resolve(), "avro")

    for file in directory.rglob("*.avsc"):
        subject_name = str(file).split("/avro/")[1].replace("/", ".").replace(".avsc", "")
        with file.open() as f:
            schema = Schema(f.read())
            schema_register.register_schema(schema=schema, subject_name=subject_name)

    schemas = schema_register.get_subjects()
    message = f"Available schemas: {schemas}"
    logger.info(message)
    logger.info("Schemas are loaded.")
