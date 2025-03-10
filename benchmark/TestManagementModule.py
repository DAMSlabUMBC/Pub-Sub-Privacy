# TestManagementModule.py
import sys
import os
import time
import importlib
from types import ModuleType
import GlobalDefs
from SyncModule import BenchmarkSynchronizer
from LoggingModule import ResultLogger

def configure_and_run_tests(config: str, broker_address: str, broker_port: int, log_file: str | None):
    # Extract my_id from config file name (e.g., "benchmark-1.cfg" -> "benchmark-1")
    my_id = os.path.basename(config).replace(".cfg", "")
    existing_benchmarks = ["benchmark-1", "benchmark-2"]  # TODO: Read from config or CLI in the future
    method = GlobalDefs.PurposeManagementMethod.PM_1

    # TODO: Read Config (for future dynamic benchmark list)

    # Load Client Interface
    module_name = "ClientInterface"
    try:
        GlobalDefs.CLIENT_MODULE = _load_client_module(module_name)
    except Exception as e:
        print(f"Unable to load client module {module_name} - {e}")
        sys.exit(GlobalDefs.ExitCode.BAD_CLIENT_API)

    # Create and Start the Synchronization Module
    GlobalDefs.SYNC_MODULE = BenchmarkSynchronizer(my_id, existing_benchmarks)
    try:
        GlobalDefs.SYNC_MODULE.start(broker_address, broker_port, method)
    except Exception as e:
        print(f"Unable to initialize the synchronization module - {e}")
        sys.exit(GlobalDefs.ExitCode.FAILED_TO_INIT_SYNC)
    
    # Configure Logging Module
    GlobalDefs.LOGGING_MODULE = ResultLogger()
    if log_file is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file = f"logs/{my_id}_{timestring}.log"
    try:
        GlobalDefs.LOGGING_MODULE.start(log_file)
    except Exception as e:
        print(f"Unable to initialize the logging module - {e}")
        sys.exit(GlobalDefs.ExitCode.FAILED_TO_INIT_LOGGING)
    
    # Notify Ready and Wait
    print(f"{my_id} waiting...")
    GlobalDefs.SYNC_MODULE.notify_and_wait_for_ready(method)
    
    # All Ready - Simulate some test work
    print(f"{my_id} starting benchmark work...")
    time.sleep(5)  # Replace with actual test logic
    
    # Notify Done and Wait
    GlobalDefs.SYNC_MODULE.notify_and_wait_for_done(method)
    
    # All Done - Log completion and exit
    GlobalDefs.SYNC_MODULE.stop()
    print(f"{my_id} completed successfully")
    return

def _load_client_module(module_name: str) -> ModuleType:
    module = importlib.import_module(module_name)
    for function in GlobalDefs.CLIENT_FUNCTIONS:
        if not hasattr(module, function):
            raise AttributeError(f"Could not find required function {function}")
    return module
