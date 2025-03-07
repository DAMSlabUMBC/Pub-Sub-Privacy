from enum import Enum, IntEnum
from types import ModuleType
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from SyncModule import BenchmarkSynchronizer
    from LoggingModule import ResultLogger

# Framework Method Enums
class PurposeManagementMethod(Enum):
    PM_0 = "None"
    PM_1 = "Purpose-Encoding Topics"
    PM_2 = "Per-Message Declaration"
    PM_3 = "Registration by Message"
    PM_4 = "Registration by Topic"

class C1RightsMethod(Enum):
    C1_0 = "None"
    C1_1 = "Direct Publication"
    C1_2 = "Pre-Registration"

class C2RightsMethod(Enum):
    C2_0 = "None"
    C2_1 = "Direct Publication"
    C2_2 = "Broker-Facilitated"

class C3RightsMethod(Enum):
    C3_0 = "None"
    C3_1 = "Direct Publication"
    C3_2 = "Broker-Facilitated"

# Exit Code definitions
class ExitCode(IntEnum):
    SUCCESS = 0
    BAD_ARGUMENT = 1
    MALFORMED_CONFIG = 2
    BAD_CLIENT_API = 3
    FAILED_TO_INIT_SYNC = 4
    FAILED_TO_INIT_LOGGING = 5
    UNKNOWN_ERROR = 99

CLIENT_MODULE = None
SYNC_MODULE = None
LOGGING_MODULE = None

CLIENT_FUNCTIONS = [
    "create_v5_client",
    "connect_client",
    "subscribe_with_purpose_filter",
    "register_publish_purpose_for_topic",
    "publish_with_purpose"
]
