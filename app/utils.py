from decimal import Decimal
from typing import Any


def default_serializer(obj: Any) -> str:
    if isinstance(obj, Decimal):
        return str(obj)
    message = f"Object of type {obj.__class__.__name__} is not JSON serializable"
    raise TypeError(message)
