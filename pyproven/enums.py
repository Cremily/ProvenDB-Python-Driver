from enum import Enum


class BulkLoadEnums(Enum):
    COMMAND = "bulkLoad"
    START = "start"
    STOP = "stop"
    KILL = "kill"
    STATUS = "status"
