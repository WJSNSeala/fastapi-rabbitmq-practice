from enum import Enum

class MessageType(str, Enum):
    NOTIFICATION = "notification"
    EMAIL = "email"
    LOG = "log"
    DATA_PROCESSING = "data_process"
    REPORT = "report"

    def __str__(self) -> str:
        return self.value