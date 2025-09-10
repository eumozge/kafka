from typing import Any, Optional


class Singleton(type):
    __instance: Optional["Singleton"] = None

    def __call__(cls, *args: Any, **kwargs: Any) -> "Singleton":
        if cls.__instance is None:
            cls.__instance = super().__call__(*args, **kwargs)
        return cls.__instance
