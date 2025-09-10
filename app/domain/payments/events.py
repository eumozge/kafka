import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from domain.common.events import Event, registry
from domain.payments.consts import Action


@dataclass(frozen=True, kw_only=True)
@registry.register(schema_subject_name="payments.processing")
class PaymentProcessing(Event):
    username: str
    action: Action
    amount: Decimal
    timestamp: str = field(default_factory=lambda: datetime.now(tz=UTC).isoformat())


def get_random_event() -> PaymentProcessing:
    return PaymentProcessing(
        username=f"user-{random.randint(1, 1000):<05}",
        amount=Decimal(random.randint(100, 1000)),
        action=random.choice(list(Action)),
    )
