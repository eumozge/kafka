import datetime as dt
import random
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Self

from domain.common.events import BaseEvent, EventRepresentation
from domain.payments.consts import Action
from domain.schemas.registries import SchemaSubjectName, registry


@dataclass(frozen=True, kw_only=True)
@registry.register(schema_subject_name=SchemaSubjectName.PAYMENT_TRANSACTION)
class PaymentTransaction(BaseEvent):
    username: str
    action: Action
    amount: Decimal
    timestamp: dt.datetime = field(default_factory=lambda: dt.datetime.now(tz=dt.UTC))

    @classmethod
    def from_representation(cls, payload: EventRepresentation) -> Self:
        return cls(
            username=payload["username"],
            action=Action(payload["action"]),
            amount=Decimal(payload["amount"]),
            timestamp=dt.datetime.fromisoformat(payload["timestamp"]),
        )

    def to_representative(self) -> EventRepresentation:
        return super().to_representative()


def get_random_event() -> PaymentTransaction:
    return PaymentTransaction(
        username=f"user-{random.randint(1, 1000):<05}",
        amount=Decimal(random.randint(100, 1000)),
        action=random.choice(list(Action)),
    )
