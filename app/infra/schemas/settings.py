import logging
from dataclasses import dataclass, field

from settings import broker, schema_register

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class SchemaRegisterSettings:
    bootstrap_servers: str = field(default=f"{broker.host}:{broker.port}")
    schema_registry_url: str = field(default=f"{schema_register.host}:{schema_register.port}")

    def to_representative(self) -> dict:
        return {"url": f"{schema_register.protocol}://{self.schema_registry_url}"}
