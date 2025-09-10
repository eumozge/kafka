import logging
import uuid
from dataclasses import dataclass, field

from settings import broker as broker_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class ProducerSettings:
    bootstrap_servers: str = f"{broker_settings.host}:{broker_settings.port}"
    client_id: str = field(default_factory=lambda: str(uuid.uuid4().hex[:8]))
    max_in_flight_requests_per_connection: int = 1
    enable_idempotence: bool = True
    acks: str = "all"
    linger_ms: int = 10
    retries: int = 10
    retry_backoff_ms: int = 300
    reconnect_backoff_ms: int = 100
    reconnect_backoff_max_ms: int = 10 * 1000

    def to_representative(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "max.in.flight.requests.per.connection": self.max_in_flight_requests_per_connection,
            "enable.idempotence": self.enable_idempotence,
            "acks": self.acks,
            "linger.ms": self.linger_ms,
            "retries": self.retries,
            "retry.backoff.ms": self.retry_backoff_ms,
            "reconnect.backoff.ms": self.reconnect_backoff_ms,
            "reconnect.backoff.max.ms": self.reconnect_backoff_max_ms,
        }
