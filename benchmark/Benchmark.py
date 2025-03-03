from enum import Enum
import importlib
import argparse
from SyncModule import BenchmarkSynchronizer
from LoggingModule import ResultLogger

# Define enums for easier referencing later
class PurposeManagementMethod(Enum):
    PM_0 = "None"
    PM_1 = "Purpose-Enconding Topics"
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
    C2_2 = "Broker-Facilitatied"

class C3RightsMethod(Enum):
    C3_0 = "None"
    C3_1 = "Direct Publication"
    C3_2 = "Broker-Facilitatied"

CLIENT_MODULE: importlib.ModuleType
SYNC_MODULE: BenchmarkSynchronizer
LOGGING_MODULE: ResultLogger
CLIENT_FUNCTIONS: list[str] = ["create_v5_client", "connect_client", "subscribe_with_purpose_filter", 
                               "register_publish_purpose_for_topic", "publish_with_purpose"]

# Notes
# - Should add multiple test sizes by default, then allow tuning in config
# - Log level is required by module driver
# - Command line parameters override config

def main():

    split_config = False
    run_tests = True
    calculate_results = False

    # Read Command Line

    # Perform relevation operation
    if split_config:
        return split_config()
    elif run_tests:
        return run_tests()
    elif calculate_results:
        return calculate_results()
    
    return -1


def split_config():
    return 0 

def run_tests():

    module_name = "ClientInterface"
    my_id = "benchmark-1"
    existing_benchmarks = ["benchmark-1", "benchmark-2"]
    broker_address = "192.168.1.1"
    broker_port = 1883
    method = PurposeManagementMethod.PM_1
    log_file = "BenchmarkLog.txt"

    # Read Config


    # Load Client Interface
    try:
        CLIENT_MODULE = _load_client_module(module_name)
    except Exception as e:
        print(f"Unable to load client module {module_name} - {e}")
        return -1

    # Create and Start the Syncronization Module
    SYNC_MODULE = BenchmarkSynchronizer(my_id, existing_benchmarks)

    try:
        SYNC_MODULE.start(broker_address, broker_port, method)
    except Exception as e:
        print(f"Unable to initialize the syncronization module - {e}")
        return -2
    
    # Configure Logging Module
    LOGGING_MODULE = ResultLogger()
    try:
        LOGGING_MODULE.start(log_file)
    except Exception as e:
        print(f"Unable to initialize the logging module - {e}")
        return -3
    
    # Create Clients
    
    
    # Instantiate Subscribers
    
    
    # Notify Ready and Wait
    SYNC_MODULE.notify_ready(method)
    SYNC_MODULE.wait_for_ready()
    
    # All Ready - Start tests
    
    
    # Notify Done and Wait
    SYNC_MODULE.notify_done(method)
    SYNC_MODULE.wait_for_done()
    
    # All Done - Exit
    return 0
    

def calculate_results():
    return 0 

"""Loads the interface for the MQTT Client

Parameters
----------
module_name : str
    The name of the module to load

Returns
----------
importlib.ModuleType
    The loaded module

Raises
----------
AttributeError
    When the module does not contain required functions
"""
def _load_client_module(module_name: str):
    
    # Load module and verify all functions are present
    module = importlib.import_module(module_name)

    # Check for necessary functions
    for function in CLIENT_FUNCTIONS:
        if not hasattr(module, function):
            raise AttributeError(f"Could not find required function {function}")

    return module


if __name__ == "__main__":
    main()