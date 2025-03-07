# TestManagementModule.py
import sys
import os
import time
import importlib
from types import ModuleType
from GlobalDefs import *
from SyncModule import BenchmarkSynchronizer
from LoggingModule import ResultLogger

def configure_and_run_tests(config: str, broker_address: str, broker_port: int, log_file: str | None):
    # Extract my_id from config file name (e.g., "benchmark-1.cfg" -> "benchmark-1")
    my_id = os.path.basename(config).replace(".cfg", "")
    existing_benchmarks = ["benchmark-1", "benchmark-2"]  # TODO: Read from config or CLI in the future
    method = PurposeManagementMethod.PM_1

    # TODO: Read Config (for future dynamic benchmark list)

    # Load Client Interface
    module_name = "ClientInterface"
    try:
        CLIENT_MODULE = _load_client_module(module_name)
    except Exception as e:
        print(f"Unable to load client module {module_name} - {e}")
        sys.exit(ExitCode.BAD_CLIENT_API)

    # Create and Start the Synchronization Module
    SYNC_MODULE = BenchmarkSynchronizer(my_id, existing_benchmarks)
    try:
        SYNC_MODULE.start(broker_address, broker_port, method)
    except Exception as e:
        print(f"Unable to initialize the synchronization module - {e}")
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
    
    # Notify Ready and Wait
    SYNC_MODULE.notify_ready(method)
    SYNC_MODULE.wait_for_ready()
    
    # All Ready - Simulate some test work
    print(f"{my_id} starting benchmark work...")
    time.sleep(5)  # Replace with actual test logic
    LOGGING_MODULE.log(f"{my_id} completed benchmark work")
    
    # Notify Done and Wait
    SYNC_MODULE.notify_done(method)
    SYNC_MODULE.wait_for_done()
    
    # All Done - Log completion and exit
    LOGGING_MODULE.log(f"{my_id} finished with all nodes synchronized")
    SYNC_MODULE.stop()
    print(f"{my_id} completed successfully")
    return

def _load_client_module(module_name: str) -> ModuleType:
    module = importlib.import_module(module_name)
    for function in CLIENT_FUNCTIONS:
        if not hasattr(module, function):
            raise AttributeError(f"Could not find required function {function}")
    return module
