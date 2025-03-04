import sys
from os import path
import time
import importlib
from types import ModuleType
from GlobalDefs import *
from SyncModule import BenchmarkSynchronizer
from LoggingModule import ResultLogger

def configure_and_run_tests(config: str, broker_address: str, broker_port: int, log_file: str | None):

    module_name = "ClientInterface"
    my_id = "benchmark-1"
    existing_benchmarks = ["benchmark-1", "benchmark-2"]
    method = PurposeManagementMethod.PM_1

    # TODO: Read Config


    # Load Client Interface
    try:
        CLIENT_MODULE = _load_client_module(module_name)
    except Exception as e:
        print(f"Unable to load client module {module_name} - {e}")
        sys.exit(ExitCode.BAD_CLIENT_API)

    # Create and Start the Syncronization Module
    SYNC_MODULE = BenchmarkSynchronizer(my_id, existing_benchmarks)

    try:
        SYNC_MODULE.start(broker_address, broker_port, method)
    except Exception as e:
        print(f"Unable to initialize the syncronization module - {e}")
        sys.exit(ExitCode.FAILED_TO_INIT_SYNC)
    
    # Configure Logging Module
    LOGGING_MODULE = ResultLogger()
    
    if log_file is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file = f"logs/{my_id}_{timestring}.log"

    try:
        LOGGING_MODULE.start(log_file)
    except Exception as e:
        print(f"Unable to initialize the logging module - {e}")
        sys.exit(ExitCode.FAILED_TO_INIT_LOGGING)
    
    # TODO: Create Clients
    
    
    # TODO: Instantiate Subscribers
    
    
    # Notify Ready and Wait
    SYNC_MODULE.notify_ready(method)
    SYNC_MODULE.wait_for_ready()
    
    # TODO: All Ready - Start tests
    
    
    # Notify Done and Wait
    SYNC_MODULE.notify_done(method)
    SYNC_MODULE.wait_for_done()
    
    # All Done - Exit
    return


"""Loads the interface for the MQTT Client

Parameters
----------
module_name : str
    The name of the module to load

Returns
----------
types.ModuleType
    The loaded module

Raises
----------
AttributeError
    When the module does not contain required functions
"""
def _load_client_module(module_name: str) -> ModuleType:
    
    # Load module and verify all functions are present
    module = importlib.import_module(module_name)

    # Check for necessary functions
    for function in CLIENT_FUNCTIONS:
        if not hasattr(module, function):
            raise AttributeError(f"Could not find required function {function}")

    return module