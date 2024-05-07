from enum import Enum


class JoinType(Enum):
    BROADCAST = "BROADCAST"
    SHUFFLE = "SHUFFLE"
