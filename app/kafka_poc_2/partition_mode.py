from enum import Enum


class PartitionMode(str, Enum):
    NO_KEY = "no-key"
    KEY = "key"
    EXPLICIT_PARTITION = "partition"