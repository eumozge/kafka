from typing import Any, Optional


class Singleton:
    __instance: Optional["Singleton"] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "Singleton":  # noqa: ARG003
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance
