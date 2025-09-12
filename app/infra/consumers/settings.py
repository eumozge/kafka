import logging
import uuid
from dataclasses import dataclass, field

from settings import broker as broker_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class ConsumerSettings:
    bootstrap_servers: str = f"{broker_settings.host}:{broker_settings.port}"
    client_id: str = field(default_factory=lambda: str(uuid.uuid4().hex[:8]))
    group_id: str = "default"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    reconnect_backoff_ms: int = 100
    reconnect_backoff_max_ms: int = 10 * 1000

    def to_representative(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
            "reconnect.backoff.ms": self.reconnect_backoff_ms,
            "reconnect.backoff.max.ms": self.reconnect_backoff_max_ms,
        }
