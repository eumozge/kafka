from enum import StrEnum, auto


class Action(StrEnum):
    CHECK = auto()
    PAY = auto()
    FAIL = auto()
    CONFIRM = auto()
    REFUND = auto()
    CANCEL = auto()
